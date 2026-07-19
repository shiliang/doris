# Doris Geo 执行层 Batch / 对象复用重构方案

> 执行版。以 [`geo-batch-reuse-plan.md`](./geo-batch-reuse-plan.md) 为主展开：Phase 1–4、改哪些文件、怎么测、PR 怎么切。  
> **不属于常量折叠**：优化发生在 BE 执行期，不是 FE 编译期把表达式算死。

## 1. 背景与边界

Doris Geo 采用「函数层 GIS + VARCHAR 载体」：几何对象在 SQL 侧是 `VARCHAR`/`STRING`，BE 侧解码为 `GeoShape` 再算。当前多数标量 Geo 函数在 batch 内**逐行** `from_encoded`，常量侧也会重复 decode，CPU 浪费明显。

本方案只做执行层减负，对齐已有 `StDistance` 的 const/vector 三分支思路。

| 做 | 不做 |
|----|------|
| 降低 BE 标量 Geo 函数每行 CPU（decode / 分配 / 重复构造） | 空间索引 |
| 对齐已有 `StDistance` 的 const/vector 三分支 | 原生 `GEOMETRY` 类型 |
| 轻量读类型头，避免无意义的完整 S2 decode | FE 对 `st_*` 的常量折叠 |
| | 改 VARCHAR 存储格式 |

与 FE 常量折叠的区别：

| | FE 常量折叠 | 本方案（BE 执行期） |
|--|-------------|-------------------|
| 时机 | 计划生成前 | BE execute |
| 结果 | 可能变成计划里的常量 / `TRUE` | 仍是 `st_*`，每行仍 `evaluate` |
| Geo 现状 | `st_*` 被 `shouldSkipFold` 跳过 | 本方案要做的 |

## 2. 现状瓶颈

`StRelationFunction`（`st_contains` / `st_intersects` / `st_disjoint` / `st_touches`）当前路径：

```cpp
// be/src/exprs/function/geo/functions_geo.cpp — StRelationFunction::execute
for (int row = 0; row < size; ++row) {
    auto lhs_value = left_col.value_at(row);
    auto rhs_value = right_col.value_at(row);
    std::unique_ptr<GeoShape> shape1(GeoShape::from_encoded(...));  // 每行
    std::unique_ptr<GeoShape> shape2(GeoShape::from_encoded(...));  // 每行，即使右侧相同
    auto relation_value = Func::evaluate(shape1.get(), shape2.get());
}
```

同文件中 `StDistance` 已有可对齐模板：`unpack_if_const` + `const_vector` / `vector_const` / `vector_vector`。

目标形态：先 `unpack_if_const`，常量侧 decode 一次，循环只 decode 变侧；双侧都变时不减少 decode 次数，只做循环外槽位复用。

## 3. 主改文件

| 路径 | 说明 |
|------|------|
| `be/src/exprs/function/geo/functions_geo.cpp` | 关系函数 / GeometryType / 一元函数执行路径 |
| `be/src/exprs/function/geo/functions_geo.h` | 已有未使用的 `StContainsState`（Phase 1 **不启用**） |
| `be/src/exprs/function/geo/geo_types.h` / `geo_types.cpp` | 编码头读类型等 helper |
| `be/test/exprs/function/geo/` | BE UT（如 `function_geo_test.cpp`） |
| `regression-test/suites/query_p0/sql_functions/spatial_functions/` | 回归（如 `test_gis_function.groovy`） |

文档回写（交付后）：`docs/design/优化项.md`、`docs/design/geo-spatial-design.md` §7.3。

## 4. Phase 1：关系函数 Const 三分支（最高 ROI，先做）

### 要做什么

改造 `StRelationFunction`，对齐 `StDistance`：

1. `unpack_if_const` 左右列。
2. 抽出共用 `decode_shape(StringRef) -> unique_ptr<GeoShape>`。
3. 三个分支：
   - **right_const / vector_const**：右侧只 `from_encoded` 一次；循环内只 decode 左侧。
   - **left_const / const_vector**：对称。
   - **vector_vector**：两侧都变；语义不变，可顺带循环外复用两个 `unique_ptr` 槽位。
4. 一侧为 const 且 decode 失败：整列置 NULL（与 `StDistance` 一致）。

不启用现有空壳 `StContainsState`；不做 FE 常量折叠。

### 示例

**例 1：右侧常量（最常见）→ `vector_const`**

```sql
SELECT id
FROM geo_table
WHERE ST_Contains(region, ST_Point(116.4, 39.9));
--                  ↑ 列（每行不同）  ↑ 字面量（整列相同）
```

3 行时：现在左 3 + 右 3 = 6 次 decode；Phase 1 后右 1 + 左 3 = 4 次。百万行时右边从 100 万次 → **1 次**。

**例 2：左侧常量 → `const_vector`**

```sql
SELECT id
FROM pois
WHERE ST_Contains(
    ST_GeomFromText('POLYGON ((116 39, 117 39, 117 40, 116 40, 116 39))'),
    location
);
```

大 Polygon decode 很贵：左边只解一次，循环只解点。

**例 3：两侧都是列 → `vector_vector`**

```sql
SELECT id FROM geo_table
WHERE ST_Contains(region, location);
```

不能少 decode 次数；只做循环外 `unique_ptr` 复用。

**例 4：const 侧非法 → 整列 NULL**

右侧常量 blob decode 失败时，只解析一次，整列结果为 NULL，不再逐行重试（对齐 `st_distance`）。

### 验证

- UT：`function_geo_test.cpp` 覆盖 const-vector / vector-const / vector-vector。
- 回归：`test_gis_function.groovy`。
- 可选：对比 `WHERE ST_Contains(region, ST_Point(116.4, 39.9))` 的 CPU。

## 5. Phase 2：轻量路径 — `ST_GeometryType` / 复核 `ST_X`·`ST_Y`

### 2a `ST_GeometryType`：只读编码头

编码布局：

```text
[0x00][GeoShapeType][S2 payload...]
 保留    类型枚举     坐标等（本函数不需要）
```

类型名与枚举一一对应，只需前 2 字节即可映射 `"ST_POINT"` / `"ST_POLYGON"` 等。

在 `geo_types` 增加 helper（如 `GeoShape::type_name_from_encoded`），`StGeometryType::execute` 改用它；非法头仍返回 NULL。多边形越大，省得越多；结果字符串与现在一致。

### 2b `ST_X` / `ST_Y`：复核为主

现状已用栈上 `GeoPoint` + `decode_from`，不再 `from_encoded` 分配未知类型。默认策略：对外保持与回归一致的约 13 位小数行为；`StrFormat`+`stod` 热点若 profiling 证实再单开小改。**2b 不是主菜；主菜是 2a。**

## 6. Phase 3：循环内分配减负（有限度）

在 Phase 1 的 `vector_vector` / 变侧循环上：

- 循环外预置左右各一个 `unique_ptr<GeoShape>`，每行赋值复用智能指针对象。
- **首版不做**全局/线程本地 `GeoShape` 对象池。
- **首版不做**通用 dict/重复 blob cache（const 侧已由 Phase 1 覆盖）。

```cpp
// Phase 3：槽位在循环外，每行只替换所指对象
unique_ptr<GeoShape> a, b;
for (...) {
    a = from_encoded(...);
    b = from_encoded(...);
    evaluate(a.get(), b.get());
}
```

## 7. Phase 4：同类函数扫尾（follow-up）

- `st_distance`：已是模板，**不改语义**。
- 一元函数如 `st_length` / `st_area_*` / `st_astext`：可做「循环外复用 `unique_ptr`」，默认放在关系函数之后，避免 PR1 过大。

优化形态同 Phase 3，**不是** Phase 1 的 const 三分支（一元函数没有「另一侧常量」）。

```sql
SELECT ST_AsText(region), ST_Length(boundary), ST_Area_Square_Meters(region)
FROM geo_table;
```

## 8. 测试与质量门槛

| 项 | 命令 / 要求 |
|----|-------------|
| BE UT | `./run-be-ut.sh`，覆盖 geo 相关用例 |
| 回归 | `./run-regression-test.sh -d query_p0/sql_functions/spatial_functions` |
| 风格 | `build-support/clang-format.sh` / `check-format.sh`；有编译库时对改动文件 clang-tidy |
| 语义 | **结果必须与改前一致**（含 NULL / 非法 blob）；不为了快改边界语义 |

## 9. 交付物与 PR 切分

### 交付物

1. 代码：`StRelationFunction` 三分支 + `ST_GeometryType` 轻量读类型（及后续一元扫尾）。
2. UT + 回归通过。
3. 回写 `优化项.md` / `geo-spatial-design.md` §7.3：标明已做 / 对象池未做。

### 建议合入

| PR | 内容 |
|----|------|
| **PR1** | Phase 1（关系函数三分支）+ UT/回归 |
| **PR2** | Phase 2（`ST_GeometryType`）+ Phase 3/4 一元扫尾 |

对象池与 dict 短路 **不进入** 第一批 PR。

### 任务清单

| ID | 内容 | 状态 |
|----|------|------|
| phase1-relation-const | `StRelationFunction` 三分支 + UT/回归 | pending |
| phase2-geometry-type | encoded 头读 type_name；`StGeometryType` 去 full decode | pending |
| phase2-xy-review | 复核 `ST_X`/`ST_Y`，保持 13 位对外语义 | pending |
| phase3-loop-reuse | vector_vector 循环外复用 `unique_ptr`；不做通用对象池 | pending |
| phase4-unary-followup | （可选）`st_astext`/`st_length`/`st_area` 循环外复用 | pending |
| docs-update | 更新优化项 / geo-spatial-design 执行性能段 | pending |

## 10. 附录：Doris vs StarRocks（简表）

StarRocks 已用 prepare / execute / close 做 const 侧缓存，机制更完整；Doris Phase 1 用 execute 内 `unpack_if_const` 作为轻量替代。

| | Doris Phase 1 | StarRocks 现有 |
|--|---------------|----------------|
| 检测时机 | execute 内 `unpack_if_const` | prepare 阶段 `is_constant_column` |
| 缓存载体 | 函数内局部变量 | `FunctionContext` FRAGMENT_LOCAL state |
| 生命周期 | 单次 execute | prepare → execute → close，可跨 batch |
| 范围 | 先落在关系函数 | 多类 geo 函数可复用同一模式 |

对 Doris Phase 1 的启示：

1. StarRocks prepare 方案更通用，后续可演进。
2. 依赖框架 API（`is_constant_column` / `set_function_state` 等）；需确认 Doris 是否已有等价能力。
3. `unpack_if_const` 可快速落地，可维护性稍弱。
4. 两套不冲突：Phase 1 先落地，后续可演进到 prepare 模式。

## 参考

- [`geo-batch-reuse-plan.md`](./geo-batch-reuse-plan.md) — 详细实施计划与示例
- [`geo-spatial-design.md`](./geo-spatial-design.md) — 整体架构与限制
- [`constant-fold-geo-analysis.md`](./constant-fold-geo-analysis.md) — 为何 FE 不做 `st_*` 折叠
- BE：`be/src/exprs/function/geo/`
- 回归：`regression-test/suites/query_p0/sql_functions/spatial_functions/`
