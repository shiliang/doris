# Doris Geo 技术方案：编码内嵌 BBox（第二层·数据表示）

| 项 | 内容 |
|----|------|
| 文档状态 | 待评审 |
| 层级定位 | 《[架构建议](./架构建议.md)》第二层：数据表示（不引入新类型系统，只改编码格式） |
| 前置依赖 | 第一层执行层优化（[`geo重构方案.md`](./geo重构方案.md) WS-A）已合入为佳，非硬依赖 |
| 后继消费方 | 第三层谓词改写（[`geo-l3-predicate-pruning-design.md`](./geo-l3-predicate-pruning-design.md)）依赖本方案暴露的 bbox |
| 涉及模块 | BE `exprs/function/geo/geo_types.*`、`functions_geo.cpp`；不涉及 FE / 存储格式 |

---

## 1. 背景与问题

Doris Geo 的几何对象以自定义二进制编码存放在 VARCHAR 中，布局为：

```text
[0x00][GeoShapeType][S2 payload...]
 保留位   类型枚举      全部坐标数据
```

写入方为 `GeoShape::encode_to`（`geo_types.cpp`），读取方为 `GeoShape::from_encoded`。该编码是**不透明 blob**：除类型枚举外，任何元信息（包括最小外接矩形 MBR/bbox）都必须完整 decode 整段 S2 payload 才能获得。

由此产生两个问题：

1. **关系函数无法快速拒绝**。`ST_Contains` / `ST_Intersects` 等函数即便两个几何相距千里，也要把双方完整 decode 成 S2 对象后做精确计算。真实负载中「明显不相交」占比通常很高（点在全国、目标多边形只是一个城市），这些行的完整 decode 是纯浪费。第一层优化消除了**常量侧**的重复 decode，但**列侧每行仍必须 decode**——这是第一层收益的天花板。
2. **下游无信息可用**。第三层希望做存储级裁剪（谓词改写 + ZoneMap），前提是 bbox 能被廉价读出。DuckDB 的经验教训明确：WKB blob 存储连读 MBR 都要重解析，任何索引都无从谈起。

本方案参考 DuckDB 的 cached envelope 设计：**把 bbox 写进编码头部，读 bbox 不再需要解码 payload**。

## 2. 目标与非目标

### 2.1 目标

| 编号 | 目标 | 度量 |
|------|------|------|
| G1 | bbox 可从编码头部 O(1) 读出 | 新增 `bbox_from_encoded`，不构造 `GeoShape`、不解 S2 |
| G2 | 关系函数具备 bbox 快速拒绝路径 | 不相交行不再走完整 decode + 精确计算；正确性不变（只允许 false positive，不允许 false negative） |
| G3 | 新旧编码共存，存量数据零迁移 | 旧版本 blob 永久可读；混合版本列可正确计算 |
| G4 | 语义完全兼容 | 所有 `st_*` 函数结果与改前一致（含 NULL / 非法输入） |

### 2.2 非目标

- **不改 SQL 类型系统**：对外仍是 VARCHAR，原生 GEOMETRY 属第四层。
- **不做存储级索引/裁剪**：ZoneMap / 谓词改写属第三层，本方案只负责把信息暴露出来。
- **不做存量数据重写**：旧数据按旧版本读，不做后台格式升级任务。
- **不在本期处理 `ST_AsBinary` / WKB 对外格式**：对外导出格式与内部编码无关，不受影响。

## 3. 备选方案对比与决策

| 方案 | 说明 | 优点 | 缺点 | 结论 |
|------|------|------|------|------|
| E-1 编码头内嵌 bbox + 版本位（**选定**） | 新版本编码在 type 后追加 bbox，首字节作版本号 | 单笔投入全函数受益；读 bbox O(1)；为第三层打基础 | 每个非点几何 +32B；需处理版本共存 | **采用** |
| E-2 bbox 独立派生列（用户建列） | 建表时让用户建 `xmin/ymin/xmax/ymax` 生成列 | 不改编码，零兼容风险 | 依赖用户建模，引擎无法默认受益；函数内部快速拒绝仍不可用 | 不作为本层方案；作为第三层的用户侧兜底路线保留 |
| E-3 运行期 bbox cache（decode 后缓存） | 首次 decode 后把 bbox 存在内存 cache | 不改编码 | 无法跨查询复用；cache 键为 blob 本身，成本高；解决不了「首次 decode」问题 | 否决 |

**版本位决策依据（已核实源码）**：现编码首字节固定 `0x00` 且 `from_encoded` 对首字节非 `0x00` 的输入直接返回 nullptr——这意味着**首字节天然就是版本号**，新版本取 `0x01` 即可与存量数据无歧义区分，无需额外字节。

## 4. 详细设计

### 4.1 编码格式 V1

```text
V0（现状）: [0x00][type][S2 payload...]
V1（新增）: [0x01][type][bbox: xmin,ymin,xmax,ymax 4×double = 32B][S2 payload...]
```

规则：

| 类型 | V1 是否带 bbox | 理由 |
|------|---------------|------|
| `GeoPoint` | **不带**（沿用 V0 或 V1 空 bbox 段，见待评审项） | 点的 bbox 就是自身，+32B 纯浪费；点是基数最大的类型 |
| `GeoLine` / `GeoPolygon` / `GeoMultiPolygon` / `GeoCircle` | 带 | 快速拒绝收益主体 |

bbox 坐标为**经纬度平面值**（非 S2 单位向量），与 WKT 输入同域，便于第三层直接映射为普通数值谓词。

**跨反子午线（antimeridian）处理**：经度跨越 ±180° 的几何，其平面 bbox 会错误地覆盖整个经度带。V1 采用**保守退化**：检测到跨反子午线时写入全域 bbox `[-180,-90,180,90]`。快速拒绝允许 false positive（走精算兜底），绝不允许 false negative，保守退化满足该不变式。极点覆盖（polygon 含南/北极）同样退化。

### 4.2 写路径

`GeoShape::encode_to` 改为写 V1：

```cpp
void GeoShape::encode_to(std::string* buf) {
    buf->push_back(ENCODE_VERSION_1);        // 0x01
    buf->push_back((char)type());
    encode_bbox(buf);                        // 各子类实现；GeoPoint 特化跳过
    encode(buf);
}
```

bbox 在 `from_wkt` / `from_wkb` / 构造函数路径生成几何时顺带计算（S2 侧已有 `GetRectBound` 类能力），避免 encode 时二次遍历坐标。

**写开关**：新增 BE config `geo_encode_version`（默认见 §6 兼容性）。灰度期间可回退为写 V0。

### 4.3 读路径

`from_encoded` 按首字节分派：

```cpp
std::unique_ptr<GeoShape> GeoShape::from_encoded(const void* ptr, size_t size) {
    // V0: [0x00][type][payload]
    // V1: [0x01][type][bbox?][payload]，按类型决定是否有 bbox 段
    // 其余首字节 → nullptr（与现状一致）
}
```

新增两个头部 helper（与第一层 Phase A2 的 `type_name_from_encoded` 共用头部解析入口，该入口即未来所有编码版本化的唯一改动点）：

| Helper | 行为 |
|--------|------|
| `type_name_from_encoded(ptr, size)` | V0/V1 均只读 type 字节（Phase A2 已规划，本方案扩展其版本感知） |
| `bbox_from_encoded(ptr, size, GeoBBox* out)` | V1 直接读 32B；V0 返回 false（调用方退回完整 decode 路径） |

### 4.4 关系函数快速拒绝

`StRelationFunction`（第一层改造后的三分支结构上）插入 bbox 预筛：

```cpp
// 伪代码：vector_const 分支，右侧常量已在循环外 decode 并取得 bbox
for (int row = 0; row < size; ++row) {
    GeoBBox lhs_bbox;
    if (bbox_from_encoded(lhs_value.data, lhs_value.size, &lhs_bbox)) {
        if (!lhs_bbox.intersects(rhs_bbox)) {
            res[row] = Func::REJECT_RESULT;   // contains/intersects → false；disjoint → true
            continue;                          // 跳过完整 decode 与精算
        }
    }
    // bbox 相交或 V0 数据：完整 decode + 精确计算（现路径）
}
```

语义映射：

| 函数 | bbox 不相交时 | 说明 |
|------|--------------|------|
| `st_intersects` | `false` | bbox 不交 ⇒ 几何必不交 |
| `st_disjoint` | `true` | 同上取反 |
| `st_contains` | `false` | 不交必不包含 |
| `st_touches` | `false` | 接触要求边界相交，bbox 不交 ⇒ 必不接触 |

`st_distance` 不适用（需要精确值），不改。

### 4.5 改动文件汇总

| 文件 | 改动 |
|------|------|
| `geo_types.h` | 版本常量、`GeoBBox` 结构、`encode_bbox` 虚方法、`bbox_from_encoded` 静态 helper |
| `geo_types.cpp` | 版本分派的 `from_encoded` / `encode_to`；各子类 bbox 计算（含反子午线/极点退化） |
| `functions_geo.cpp` | 关系函数插入 bbox 预筛分支 |
| `be/src/common/config.h` | `geo_encode_version` 配置项 |
| `be/test/exprs/function/geo/` | UT（见 §7） |

不改：FE、thrift、存储引擎、回归结果基线。

## 5. 与第一层的组合关系

| 优化 | 消除的开销 | 剩余热点 |
|------|-----------|----------|
| 第一层 A1/A5（const 缓存） | 常量侧重复 decode | 列侧每行 decode + 精算 |
| **本方案（bbox 快速拒绝）** | 列侧「明显不相交」行的 decode + 精算 | 真正 bbox 相交行的精算 |

两者正交、收益叠加：`WHERE ST_Contains(region_常量, point_col)` 场景下，常量侧 1 次 decode（第一层）+ 列侧大部分行 O(1) 拒绝（本方案），只有 bbox 命中的行才进入 S2 精算。

## 6. 兼容性分析（本方案的核心风险面）

| 维度 | 影响 | 结论/缓解 |
|------|------|-----------|
| 存量 V0 数据 | `from_encoded` 保留 V0 分支，永久可读；`bbox_from_encoded` 返回 false 走原路径 | 兼容，零迁移 |
| 混合版本列 | 同一列内 V0/V1 行共存（升级后新写入为 V1），逐行按首字节分派 | 兼容 |
| **滚动升级（新写旧读）** | 升级期间新 BE 写出 V1 blob，尚未升级的旧 BE `from_encoded` 见首字节 `0x01` 返回 nullptr → 该行 NULL，**静默错误** | **两阶段发布**：版本 N 交付读能力（`geo_encode_version` 默认 0，只读不写 V1）；版本 N+1（或确认全集群升级后）默认改 1。评审必须确认此节奏 |
| 集群降级 | 降级到不认识 V1 的版本后，已写入的 V1 数据读出 NULL | 与滚动升级同源，靠两阶段发布 + 文档声明降级边界 |
| 存储空间 | 非点几何每值 +32B；点不变 | 多边形 payload 通常远大于 32B，占比小；点（基数主体）豁免 |
| 计算结果 | bbox 仅做保守预筛，false positive 走精算 | 结果与改前逐行一致 |
| `ST_AsText` / `ST_AsBinary` 等输出 | 输出由 decode 后对象生成，与编码版本无关 | 不变 |

## 7. 测试方案

| 类别 | 内容 |
|------|------|
| UT `geo_types_test.cpp` | V0/V1 编解码互通；`bbox_from_encoded` 各类型正确性；反子午线/极点退化为全域 bbox；非法首字节/截断输入返回失败 |
| UT `function_geo_test.cpp` | 四关系函数：bbox 拒绝行与完整精算结果一致（随机几何对拍）；V0/V1 混合列；`geo_encode_version=0/1` 两态 |
| 回归 | 存量 `test_gis_function.groovy` 零 diff（两种 `geo_encode_version` 下各跑一遍）；新增跨反子午线用例 |
| 性能 | 「常量城市多边形 × 全国点列」场景：改前 vs 改后 CPU；预期大部分行走 O(1) 拒绝 |
| 升级演练 | V0 集群写数据 → 升级 → 开写 V1 → 混合读；再验证「未开写开关时降级」无损 |

## 8. 实施计划

| PR | 内容 | 依赖 |
|----|------|------|
| PR-E1 | 编码 V1 读写 + `bbox_from_encoded` + UT（写开关默认关） | 建议在第一层 PR-A1 之后（共改 `StRelationFunction`） |
| PR-E2 | 关系函数 bbox 快速拒绝 + 回归/性能报告 | PR-E1 |
| PR-E3 | 默认开启写 V1（独立小 PR，便于单独回退） | PR-E2 + 发布节奏确认 |

## 9. 风险与应对

| 风险 | 等级 | 应对 |
|------|------|------|
| 滚动升级/降级期间 V1 blob 被旧代码读为 NULL | 高 | 两阶段发布（§6）；写开关独立 PR，可秒级回退 |
| 反子午线/极点几何 bbox 计算错误导致 false negative（漏结果） | 高 | 保守退化策略 + 定向 UT；对拍测试随机验证「bbox 拒绝 ⇒ 精算也拒绝」 |
| bbox 计算引入 encode 路径性能回退（导入侧） | 中 | bbox 随几何构造顺带计算，不二次遍历；导入基准对比 |
| 与第一层 / WS-B 改动在 `StRelationFunction` 冲突 | 中 | 排期在 PR-A1、PR-B2 之后 |
| Point 豁免逻辑造成版本分派复杂化 | 低 | 头部解析单一入口（§4.3），按 (version, type) 查表决定段布局 |

## 10. 待评审确认项

1. **Point 编码策略**：豁免 bbox（推荐，省 32B）但引入「V1 下按类型变长头」；或统一带 bbox 换取布局简单。
2. **发布节奏**：两阶段（先读后写）跨几个版本；`geo_encode_version` 用 BE config 还是 FE session variable 下发。
3. **bbox 精度**：4×double（32B）或 4×float（16B，容差需并入保守判断）。
4. **`GeoCircle` 的 bbox**：球面 cap 的经纬度 bbox 计算较绕，是否首期豁免（同 Point 处理）。

## 参考

- [`架构建议.md`](./架构建议.md) — 分层路线，本方案为第二层
- [`geo重构方案.md`](./geo重构方案.md) — 第一层执行层方案（本方案的代码基座）
- [`geo-l3-predicate-pruning-design.md`](./geo-l3-predicate-pruning-design.md) — 第三层，消费本方案的 bbox
- DuckDB Spatial：cached envelope（原生 GEOMETRY 内嵌 bbox 的先例与收益数据）
- 代码：`be/src/exprs/function/geo/geo_types.cpp`（`encode_to` / `from_encoded`）
