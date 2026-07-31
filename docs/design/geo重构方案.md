# Doris Geo 重构技术方案


| 项    | 内容                                                                                                                                                                                                                                                                                       |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 文档状态 | 待评审                                                                                                                                                                                                                                                                                      |
| 涉及模块 | BE `exprs/function/geo`、FE Nereids 函数签名                                                                                                                                                                                                                                                  |
| 方案范围 | ① 执行层性能优化（Batch / 对象复用 / prepare 缓存）；② 空间关系函数二维/三维计算一致性                                                                                                                                                                                                                                  |


---

## 1. 背景与问题

Doris 当前的 Geo 能力采用「函数层 GIS + VARCHAR 载体」模型：几何对象在 SQL 类型系统中以 `VARCHAR`/`STRING` 表示，内部承载自定义二进制编码；所有几何解析与空间计算在 BE 基于 Google S2 执行；FE 不做几何计算，`st_*` 函数被常量折叠显式跳过（`FoldConstantRuleOnBE.shouldSkipFold`）。

在这个架构下，有两个独立的问题要解决。

### 1.1 问题一：执行层性能——批量执行中重复 decode

空间关系函数 `st_contains` / `st_intersects` / `st_disjoint` / `st_touches`（BE 侧统一为 `StRelationFunction` 模板）在 batch 内逐行执行：

```cpp
// be/src/exprs/function/geo/functions_geo.cpp — StRelationFunction::execute（现状）
for (int row = 0; row < size; ++row) {
    auto lhs_value = left_col.value_at(row);
    auto rhs_value = right_col.value_at(row);
    std::unique_ptr<GeoShape> shape1(GeoShape::from_encoded(...));  // 每行 decode
    std::unique_ptr<GeoShape> shape2(GeoShape::from_encoded(...));  // 每行 decode，即使该列是常量
    auto relation_value = Func::evaluate(shape1.get(), shape2.get());
}
```

典型查询 `WHERE ST_Contains(region, ST_Point(116.4, 39.9))` 中，右侧点在整个查询内不变，但每行都会完整执行一次 `from_encoded`（解码 + 堆分配 + S2 对象构造）。百万行即百万次无效解码；左侧为常量大多边形时（如省级边界），浪费更为显著。由于 FE 对 `st_*` 不做常量折叠（FE 无法表示 Geo 二进制值，见 `constant-fold-geo-analysis.md`），这个开销没法在计划期消掉，只能靠执行层解决。

同文件的 `StDistance` 已经用了 `unpack_if_const` + `const_vector` / `vector_const` / `vector_vector` 三分支，说明这套优化在当前框架里完全可行。

### 1.2 问题二：语义一致性——二维/三维计算路径结果不一致

空间关系函数基于 S2 的三维球面模型计算，但从 PostGIS / MySQL 迁过来的用户通常预期的是平面二维语义。两种模型对同一输入可能给出不同结果——比如一条长线段和多边形的相交判断，球面大圆路径和平面直线路径可能穿过不同的区域。当前引擎既不能选计算模型，也没法在一条 SQL 里表达"按平面语义判断"。

前期分析（见飞书文档）对比过几种候选方案，决定给四个空间关系函数加一个可选第三参数 `use_sphere BOOLEAN`。本文档给出具体落地方式。

## 2. 目标

| 编号  | 目标                        | 怎么验证                                                                                             |
| --- | ------------------------- | ------------------------------------------------------------------------------------------------ |
| G1  | 消除常量侧几何对象的重复 decode       | 常量侧 decode 次数从 O(N) 降到 O(1)（Phase A5 后进一步降到每 fragment 1 次）；`ST_Contains(col, 常量)` 类查询 CPU 显著下降 |
| G2  | 去掉轻量函数中的无效开销              | `ST_GeometryType` 不再做完整 S2 decode                                                              |
| G3  | 提供二维/三维计算模型的 SQL 级选择能力    | 四个关系函数支持 `use_sphere` 第三参，同一 SQL 内可以混用两种模式                                                      |
| G4  | 兼容存量行为                    | 存量两参写法的结果与现版本完全一致（包括 NULL / 非法输入路径）                                                             |




## 3. 方案总览

分两条独立工作流，改动都在 `be/src/exprs/function/geo/` 和 FE 四个函数签名类里，可以并行推进、独立交付、独立回滚：


| 工作流              | 解决的问题          | 核心手段                                                     | 是否改变对外行为      |
| ---------------- | -------------- | -------------------------------------------------------- | ------------- |
| **WS-A 执行层性能**   | §1.1 重复 decode | const/vector 三分支、编码头轻量读取、循环外槽位复用、`open/close` prepare 缓存 | 否（纯性能优化，结果不变） |
| **WS-B 计算模型选择**  | §1.2 二维/三维不一致  | 四函数新增可选 `use_sphere` 参数，BE 增加二维计算路径                      | 新增能力；存量写法行为不变 |


两条工作流在 `StRelationFunction` 上有交集，合入顺序在 §8 说明。

## 4. 详细设计：WS-A 执行层性能

### 4.1 Phase A1：关系函数 const 三分支（最高 ROI）

改造 `StRelationFunction`，对齐 `StDistance` 现有模式：

1. `unpack_if_const` 解包左右列。
2. 抽出共用 helper：`decode_shape(StringRef) -> std::unique_ptr<GeoShape>`。
3. 按常量分布走三个分支：


| 分支              | 场景                                               | 行为                                             |
| --------------- | ------------------------------------------------ | ---------------------------------------------- |
| `vector_const`  | 右侧常量（最常见，如 `ST_Contains(region, ST_Point(...))`） | 右侧 decode 一次，循环内只 decode 左侧                    |
| `const_vector`  | 左侧常量（如固定大区边界过滤点列）                                | 对称处理；大 Polygon decode 收益最显著                    |
| `vector_vector` | 两侧均为列                                            | decode 次数无法减少；循环外复用两个 `unique_ptr` 槽位，消除临时对象构造 |


1. 错误路径对齐 `StDistance`：const 侧 decode 失败时**整列置 NULL**，不逐行重试。

收益（右侧常量、N 行为例）：decode 次数从 `2N` 降到 `N + 1`；右侧从 N 次降到 1 次。

本 Phase 不动 `StContainsState`（留到 Phase A5），结果语义不变。

### 4.2 Phase A2：轻量路径 — `ST_GeometryType`

`ST_GeometryType(geom)` 只返回类型名字符串（`"ST_POINT"` / `"ST_POLYGON"` 等），不需要坐标、不做任何空间计算。但现状实现（`functions_geo.cpp` 中 `StGeometryType::execute`）每行都走完整 `GeoShape::from_encoded`：读类型 → 堆上构造对应对象 → **decode 整段 S2 payload**（大多边形代价高），最后仅调用虚函数 `GeometryType()` 返回一个固定字符串，payload 的解码结果被完全丢弃。

类型信息其实就在编码头部。`GeoShape::encode_to`（`geo_types.cpp`）的写入布局为：

```text
[0x00][GeoShapeType][S2 payload...]
 保留    类型枚举     坐标等（本函数不需要）
```

改造方式：在 `geo_types` 加一个静态 helper（例如 `GeoShape::type_name_from_encoded(const void*, size_t)`），只读头部返回类型名，不构造 `GeoShape`、不解 S2；`StGeometryType::execute` 改用这个 helper。校验规则与 `from_encoded` 的头部检查逐条对齐，保证 NULL 语义不变：


| 输入                            | `from_encoded`（现状）            | helper（改后）            |
| ----------------------------- | ----------------------------- | --------------------- |
| `size < 2` 或首字节非 `0x00`       | 返回 nullptr → 该行 NULL          | 同样返回失败 → 该行 NULL      |
| 第 2 字节不在已知 `GeoShapeType` 枚举内 | default 分支返回 nullptr → NULL   | 同样 NULL               |
| 头部合法但 payload 损坏              | payload decode 失败 → NULL      | **返回类型名（唯一行为差异，见下）** |
| 合法输入                          | 完整 decode 后调 `GeometryType()` | 直接映射同一字符串，逐字节一致       |



### 4.3 Phase A3：分配减负

现状在每个 `vector_vector` 迭代内都会构造两个局部 `unique_ptr`，循环结束析构：

```cpp
// 现状
for (int row = 0; row < size; ++row) {
    std::unique_ptr<GeoShape> shape1(GeoShape::from_encoded(lhs));
    std::unique_ptr<GeoShape> shape2(GeoShape::from_encoded(rhs));
    auto relation_value = Func::evaluate(shape1.get(), shape2.get());
}   // shape1、shape2 出作用域 → 析构 → delete 堆对象
```

改为循环外预置左右 `unique_ptr<GeoShape>` 槽位，每次只 `reset` 替换所指对象：

```cpp
// 改后：槽位提到循环外
std::unique_ptr<GeoShape> shape1;
std::unique_ptr<GeoShape> shape2;
for (int row = 0; row < size; ++row) {
    shape1.reset(GeoShape::from_encoded(lhs));  // 旧对象自动 delete
    shape2.reset(GeoShape::from_encoded(rhs));
    auto relation_value = Func::evaluate(shape1.get(), shape2.get());
}
```

不引入对象池与通用 blob cache，没有必要。

### 4.4 Phase A5：升级为 open/close prepare 缓存（跨 block 复用）

Phase A1 的 const 分支只在单次 `execute` 内生效：同一常量在每个 block 到来时仍要重新 decode 一次。Phase A5 把常量侧 decode 提前到函数 `open` 阶段、缓存进 `FunctionContext`，整个 fragment 生命周期只 decode 一次（与 StarRocks `st_contains_prepare` 同构）。

#### 4.4.1 框架就绪性（已确认）


| 所需能力                | Doris 现状                                                                                                                | 位置                                            |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| 函数级 `open/close` 钩子 | `IFunctionBase::open/close` 虚函数已存在，默认空实现                                                                                | `be/src/exprs/function/function.h`            |
| 常量列检测与获取            | `FunctionContext::is_col_constant(i)` / `get_constant_col(i)`；常量列由 `VExpr::open` 阶段统一填充（`set_constant_cols`）            | `be/src/exprs/function_context.h`、`vexpr.cpp` |
| 跨 block 状态挂载        | `set_function_state` / `get_function_state`，支持 `FRAGMENT_LOCAL` / `THREAD_LOCAL` 双 scope                                | `be/src/exprs/function_context.cpp`           |
| 同类先例                | `convert_tz`（FRAGMENT_LOCAL 缓存时区解析）、`like` / `regexp`（缓存编译后 pattern）、`dict_get` 等均为「open 读常量列 → 建 state → execute 复用」模式 | `function_convert_tz.cpp` 等                   |
| 状态结构体               | `StContainsState` / `StConstructState` 已在头文件定义（当前零引用的空壳）                                                                | `functions_geo.h`                             |


结论：框架没有缺口，不需要动 FE / thrift / 函数注册工厂。之前担心的"Doris 有没有等价 IFunction API"已经确认没问题。

#### 4.4.2 现状缺口与改动点

真正的缺口只在 Geo 模块自己：`GeoFunction::execute_impl` 拿到 `FunctionContext*` 就扔了，`Impl::execute` 签名里没有 context：

```cpp
// functions_geo.h — 现状：context 被丢弃
Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                    uint32_t result, size_t input_rows_count) const override {
    return Impl::execute(block, arguments, result);
}
```

改动分三步，对齐 `convert_tz` 的做法：

1. `GeoFunction` 实现 `open/close`：`FRAGMENT_LOCAL` scope 下，对关系函数检查 `is_col_constant(0/1)`；有常量侧则 `get_constant_col` → `GeoShape::from_encoded` → 存入 `StContainsState`（`shared_ptr` 交给 `set_function_state`，不用手动 delete）。常量侧 decode 失败则置 `state->is_null`，execute 直接整列 NULL（和 Phase A1 语义一致）。
2. 打通 context 传递：`Impl::execute` 增加 `FunctionContext*` 参数。也可以只给关系函数做特化模板，其他 Impl 不动，这样改动更小。
3. `StRelationFunction::execute` 读 state：有缓存侧直接用 `state->shapes[i]`，变侧沿用 Phase A1 的逐行 decode / 槽位复用；state 为空时退回 Phase A1 路径（两个模式共存，方便渐进回滚）。



#### 4.4.3 与 Phase A1 的关系与收益


|               | Phase A1（execute 内）                 | Phase A5（open 阶段）    |
| ------------- | ----------------------------------- | -------------------- |
| 常量侧 decode 次数 | 每个 block 1 次（扫 100 个 block 即 100 次） | 每个 fragment **1 次**  |
| 依赖            | 无                                   | 依赖 Phase A1 的三分支代码结构 |
| 回滚            | —                                   | state 为空自动退回 A1 路径   |


Phase A1 做完后，A5 的增量改动主要是 `open/close` 和 state 读写，预计一个小型 BE PR（`functions_geo.h/.cpp` + UT）。

注意几点：

- **scope**：跟 `convert_tz` 一样用 `FRAGMENT_LOCAL`（常量列在 fragment open 时就已就绪），不折腾 `THREAD_LOCAL`。
- **模板影响面**：`GeoFunction` 是所有 geo 函数共用的模板，`open` 里必须按 Impl 类型分流（比如通过 trait 判断要不要开缓存），别给不需要缓存的函数挂无用 state。
- **和 WS-B 的关系**：第三参 `use_sphere` 是常量时不影响缓存（缓存的是几何对象，和计算模型互不干扰）。



## 5. 详细设计：WS-B 计算模型选择（`use_sphere`）



### 5.1 为什么不选其他方案


| 方案                             | 说明                                          | 优点                                     | 缺点                                             | 结论                      |
| ------------------------------ | ------------------------------------------- | -------------------------------------- | ---------------------------------------------- | ----------------------- |
| B-1 函数第三参 `use_sphere`（**选定**） | `st_intersects(g1, g2, use_sphere)`，四函数同步支持 | 表达式级粒度，同一 SQL 可混用两种模式；语义显式、可审计；不依赖会话状态 | 需改 FE 签名 + BE arity，改动面较大                      | **采用**                  |
| B-2 会话变量统一切换                   | 不改 SQL，`TQueryOptions` 下发布尔开关               | SQL 零改动                                | 无法在同一查询内混用；会话状态隐式影响结果，排障困难；工程上仍需打通 BE 取会话选项的管道 | 本期不做；如后续实施，约定第三参优先于会话变量 |
| B-3 新函数名（如 `st_intersects_2d`） | 语义并行的函数族                                    | 实现最简单                                  | 函数数量翻倍，签名爆炸；与 OGC 命名习惯不符                       | 否决                      |


默认值：两参写法默认 `use_sphere = true`（沿用现有 S2 球面行为），存量查询结果不变。`DEFAULT_USE_SPHERE` 在 BE 用常量集中定义。

> 待评审：默认值是不是长期锚定 `true`。如果以后想对齐 PostGIS 的平面语义（默认 `false`），要走行为变更流程，不在这次方案里。



### 5.2 SQL 语义定义

```sql
-- 三维球面（显式指定；与现版本默认行为一致）
SELECT ST_Intersects(g1, g2, true);

-- 二维平面
SELECT ST_Intersects(g1, g2, false);

-- 两参：默认行为，与升级前完全一致
SELECT ST_Intersects(g1, g2);
```

- 适用函数：`st_intersects` / `st_disjoint` / `st_contains` / `st_touches`（当前 Nereids 已注册的全部空间关系函数）。
- 第三参为普通布尔表达式，允许列值（逐行生效）、常量与 NULL；NULL 遵循函数整体的 `AlwaysNullable` / `PropagateNullLiteral` 语义。
- 语义约束：不管 `use_sphere` 取什么值，`st_disjoint(g1, g2, s) == NOT st_intersects(g1, g2, s)` 必须成立（允许数值误差）。



### 5.3 FE 设计

关键点：必须和 `BinaryExpression` 解绑。现在四个 `St`* 类实现的是 `BinaryExpression`（固定两个子节点），和 2/3 参并存是冲突的。改成只继承 `ScalarFunction` + `ExplicitlyCastableSignature` + `AlwaysNullable` + `PropagateNullLiteral`，`withChildren` 允许 2 或 3 个子节点：

```java
// StIntersects.java — 结构示意
public class StIntersects extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, BooleanType.INSTANCE),
            FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE, BooleanType.INSTANCE));

    public StIntersects(Expression arg0, Expression arg1) {
        super("st_intersects", arg0, arg1);
    }

    public StIntersects(Expression arg0, Expression arg1, Expression arg2) {
        super("st_intersects", arg0, arg1, arg2);
    }

    @Override
    public StIntersects withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        return new StIntersects(getFunctionParams(children));
    }
    // getSignatures / accept 不变，仍走 visitStIntersects
}
```

FE 改动清单：


| 文件                                                                  | 改动                                               |
| ------------------------------------------------------------------- | ------------------------------------------------ |
| `.../scalar/StIntersects.java`                                      | 去 `BinaryExpression`；新增三参签名与构造；放宽 `withChildren` |
| `.../scalar/StDisjoint.java` / `StContains.java` / `StTouches.java` | 同上                                               |
| 依赖 `BinaryExpression` 的 Nereids 规则（如有）                              | 排查并解除依赖                                          |




### 5.4 BE 设计



#### 5.4.1 函数注册 arity（固定三参，FE 补默认值）

FE 在表达式构建阶段把两参写法统一补成三参：`ST_Contains(g1, g2)` → `ST_Contains(g1, g2, true)`，第三参填常量 `DEFAULT_USE_SPHERE`。BE 侧 `NUM_ARGS = 3` 固定，`is_variadic() = false` 不变，不需要双注册或者 variadic 特化：

```cpp
template <typename Func>
struct StRelationFunction {
    static constexpr auto NAME = Func::NAME;
    static const size_t NUM_ARGS = 3;   // FE 已补全第三参
    using Type = DataTypeUInt8;
    ...
};
```

这样 BE 改动最小——不用改 `GeoFunction` 模板，不用动 `SimpleFunctionFactory` 注册，`execute` 里按正常 3 参读第三列就行。

#### 5.4.2 `GeoShape` API

```cpp
// geo_types.h — 签名变更示意
class GeoShape {
public:
    virtual bool intersects(const GeoShape* rhs, bool use_sphere) const;
    virtual bool disjoint(const GeoShape* rhs, bool use_sphere) const;
    virtual bool contains(const GeoShape* rhs, bool use_sphere) const;
    virtual bool touches(const GeoShape* rhs, bool use_sphere) const;

    // 无参重载：转发 DEFAULT_USE_SPHERE（= true，保持兼容）
    virtual bool intersects(const GeoShape* rhs) const {
        return intersects(rhs, DEFAULT_USE_SPHERE);
    }
    // disjoint / contains / touches 同理
};
```

- `GeoPoint` / `GeoLine` / `GeoPolygon` / `GeoCircle` / `GeoMultiPolygon` 按需 override，内部用 `_2d` / `_3d` 私有方法分发。
- `GeoMultiPolygon` 必须把 `use_sphere` 透传到内部 polygon 逻辑。
- 始终成立：`disjoint(rhs, s) == !intersects(rhs, s)`，两种模式都一样。



#### 5.4.3 二维计算辅助函数（`geo_types.cpp`）


| 函数                                                                                                 | 算法要点                                               |
| -------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| `is_polygon_intersects_line_2d`                                                                    | 线段与边界相交；或线段中点在多边形内                                 |
| `is_polygon_intersects_polygon_2d`                                                                 | 边界相交；或顶点在对方内部                                      |
| `is_polygon_contains_point_2d`                                                                     | ray casting；排除边界点（与现有 `contains` 边界语义一致，容差 `1e-6`） |
| `is_polygon_contains_line_2d`                                                                      | 全部顶点在内且线段不穿越边界                                     |
| `is_polygon_contains_polygon_2d`                                                                   | inner 全部顶点在 outer 内                                |
| `is_polygon_touches_line_2d` / `is_polygon_touches_polygon_2d`                                     | 边界接触且无内部重叠                                         |
| `is_circle_intersects_polygon_2d` / `is_circle_touches_polygon_2d` / `is_circle_contains_point_2d` | 平面距离模型                                             |




#### 5.4.4 三维（球面）计算辅助函数（`geo_types.cpp`）

和二维路径不同，球面几何运算主要靠 S2 库。但 S2 的部分 API 在边界场景（线段与多边形环共线、纯边界接触等）有数值精度或方向性 bug，所以也需要几个补充函数来修。这些函数不是简单的 S2 wrapper——它们修的是 S2 处理不了的 edge case。

##### 5.4.4.1 多边形与线段的球面交互


| 函数                                  | 调用方                                            | S2 依赖                                | 为什么需要手工补丁                                                                                                   |
| ----------------------------------- | ---------------------------------------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| `is_polygon_intersects_line_sphere` | `GeoPolygon::intersects`、`GeoLine::intersects` | `S2Polygon::IntersectWithPolyline()` | S2 可能在数值精度问题下漏报仅边界相交的情况；补丁对每条多边形边与每条线段用 `is_segments_intersect` 做二次兜底                                       |
| `is_polygon_touches_line_sphere`    | `GeoPolygon::touches`                          | `S2Polygon::IntersectWithPolyline()` | 需区分"内部重叠"与"纯边界接触"——收集交点线的所有顶点，逐一确认是否都在多边形顶点集合中（即交点完全落在边界上）                                                  |
| `is_polygon_contains_line_sphere`   | `GeoPolygon::contains`                         | `S2Polygon::Contains(S2Polyline)`    | S2 的 `Contains` 在 collinear 线段方向判定上有已知缺陷；补丁检查线段顶点是否落在多边形边上但不形成线-线接触（`is_line_touches_line`），以此区分"内部"和"穿越边界" |




##### 5.4.4.2 多边形 vs 多边形的边界接触判定


| 函数                                     | 调用方                   | S2 依赖                             | 为什么需要手工补丁                                                                   |
| -------------------------------------- | --------------------- | --------------------------------- | --------------------------------------------------------------------------- |
| `is_polygon_intersection_empty_sphere` | `GeoPolygon::touches` | `S2Polygon::InitToIntersection()` | 两个多边形 `touches` = 相交但无内部重叠；直接构造 S2 交集多边形并检查其面积是否 < `TOLERANCE`，面积接近零即仅有边界接触 |


注意：`GeoPolygon::intersects` 在处理 POLYGON × POLYGON 且 `use_sphere=true` 时，直接用 `S2Polygon::Intersects()` 加 `polygon_touch_polygon` 兜底（同上边界漏报问题），没有对应的独立静态辅助函数——该逻辑内联在 `intersects` 方法体中。

##### 5.4.4.3 GeoCircle 的球面方法

Circle 在 `use_sphere=true` 时通过成员方法 `intersects_sphere` / `touches_sphere` 分发，而非 `_2d` 版的 plane 静态函数。这两个方法虽然是成员函数，但其算法同样值得说明：


| 方法                             | 算法要点                                                                                                                                                |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GeoCircle::intersects_sphere` | Point: 大圆距离 ≤ radius + TOLERANCE；Line: `compute_distance_to_line` ≤ radius；Polygon: 圆心在内部或任一边的大圆距离 ≤ radius；Circle×Circle: 中心大圆距离 ≤ r1+r2+TOLERANCE |
| `GeoCircle::touches_sphere`    | Point: `                                                                                                                                            |




##### 5.4.4.4 三维路径的几何基元

以下函数**不区分 2D/3D**，两种模式共用：


| 函数                          | 用途          | 说明                                                                               |
| --------------------------- | ----------- | -------------------------------------------------------------------------------- |
| `is_segments_intersect`     | 线段相交判断      | 提取 lat/lng 度数后在平面做欧几里得相交测试；球面路径用它做边界补丁（因为大圆与多边形边的相交退化到点级后 lat/lng 小范围内的欧几里得近似足够） |
| `is_point_in_polygon`       | ray casting | 经典光线投射法判断点是否在多边形内；lat/lng 度数平面上的算法，两种模式共用                                        |
| `compute_distance_to_point` | 大圆距离（米）     | 球面 Haversine 距离，Circle 球面模式和 ST_Distance 的基础                                     |
| `compute_distance_to_line`  | 点到线段最短距离（米） | 球面大圆距离，用于 touches/intersects 阈值判定                                                |




##### 5.4.4.5 与二维函数的映射关系


| 关系操作                         | 二维辅助函数                              | 三维辅助函数                                                           | 备注                                                |
| ---------------------------- | ----------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------- |
| Polygon × Line intersects    | `is_polygon_intersects_line_2d`     | `is_polygon_intersects_line_sphere`                              | 2D 用平面线段穿过判断；3D 用 S2 `IntersectWithPolyline` + 补丁 |
| Polygon × Line touches       | `is_polygon_touches_line_2d`        | `is_polygon_touches_line_sphere`                                 |                                                   |
| Polygon × Line contains      | `is_polygon_contains_line_2d`       | `is_polygon_contains_line_sphere`                                |                                                   |
| Polygon × Polygon intersects | `is_polygon_intersects_polygon_2d`  | `S2Polygon::Intersects()` + `polygon_touch_polygon`（内联）          | 3D 侧无独立静态函数                                       |
| Polygon × Polygon touches    | `is_polygon_touches_polygon_2d`     | `is_polygon_intersection_empty_sphere` + `polygon_touch_polygon` | 3D 需要面积检查                                         |
| Polygon × Point contains     | `is_point_in_or_on_polygon_2d` etc. | `S2Polygon::Contains(S2Point)`                                   | S2 原生 API 足够，无独立函数                                |
| Circle × Polygon intersects  | `is_circle_intersects_polygon_2d`   | `GeoCircle::intersects_sphere`                                   | 成员方法 vs 静态函数的差异                                   |
| Circle × Point contains      | `is_circle_contains_point_2d`       | `S2Cap::Contains()`                                              | S2 原生 API 足够                                      |




#### 5.4.5 执行体

第三参逐行读取（列值场景），`ColumnConst` 时提到循环外；不能用 `get_bool(0)` 代表整列：

```cpp
// 伪代码
for (int row = 0; row < size; ++row) {
    bool use_sphere = default_use_sphere;
    if (has_third_arg) {
        use_sphere = third_arg_view.get_bool(row);
    }
    auto relation_value = Func::evaluate(shape1.get(), shape2.get(), use_sphere);
    res->get_data()[row] = relation_value;
}
```

和 WS-A 的关系：`use_sphere` 是常量列时同样走 const 分支优化，几何常量侧的一次性 decode 和计算模型选择互不影响。

#### 5.4.6 BE 改动清单


| 文件                           | 改动                                        | 备注                  |
| ---------------------------- | ----------------------------------------- | ------------------- |
| `geo_types.h`                | `use_sphere` 重载与内部分发声明                    |                     |
| `geo_types.cpp`              | 二维辅助函数 + 三维辅助函数 + 各类型实现                   | 体量最大                |
| `functions_geo.h`            | arity 策略（双注册或 variadic）                   | 评审确定后实施；易遗漏         |
| `functions_geo.cpp`          | 执行体、按行读第三参                                | 与 WS-A 改动同文件，注意合入顺序 |
| `PaloInternalService.thrift` | 本期**不改**；仅未来实施 B-2 时新增 `TQueryOptions` 字段 |                     |




## 6. 兼容性分析


| 维度                    | 影响                                                                                                                                            | 结论              |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | --------------- |
| 存量 SQL（两参写法）          | 走原三维球面路径，结果不变                                                                                                                                 | 兼容              |
| 存量数据（VARCHAR 编码 blob） | 编码格式不变                                                                                                                                        | 兼容              |
| NULL / 非法输入           | WS-A 错误路径对齐 `StDistance`（const 侧非法 → 整列 NULL，和逐行 NULL 语义一致，Phase A1/A5 行为相同）；`ST_GeometryType` 轻量路径有一处只在数据损坏场景出现的差异（§4.2）；WS-B 遵循既有 nullable 语义 | 兼容（一处待确认项见 §10） |
| 输出精度                  | `ST_AsText` 13 位小数、`contains` 边界容差 `1e-6` 均不变                                                                                                 | 兼容              |
| 升级/降级                 | 纯函数层改动，无持久化格式变化；降级后三参 SQL 报「函数不存在」，属预期                                                                                                        | 可接受             |
| 生态对比                  | 第三参为 Doris 扩展语法；PostGIS 无此参数（它的 `geometry`/`geography` 类型天然区分两种模型）                                                                             | 需在用户文档中说明       |



