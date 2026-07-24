# Doris Geo 技术方案：原生 GEOMETRY 类型与 ColumnGeo（第四层·长期）

| 项 | 内容 |
|----|------|
| 文档状态 | 待评审（方向性评审，非实施评审） |
| 层级定位 | 《[架构建议](./架构建议.md)》第四层：大版本工程 |
| 前置依赖 | 无硬依赖，但建议第一~三层先行（各层收益独立，且第三层的裁剪思路在本层直接升级为 segment 级元数据） |
| 涉及模块 | FE 类型系统 / Nereids / Catalog、BE 类型与列系统、存储格式、thrift/proto、导入导出、MySQL 协议层 |

---

## 1. 背景与问题

前三层优化始终受制于同一个根源：**Doris 没有几何类型**。几何对象伪装成 VARCHAR，带来三个无法在函数层修补的根问题：

| 根问题 | 表现 | 前三层为何解决不了 |
|--------|------|-------------------|
| FE 无法表示几何字面量 | `st_*` 被 `FoldConstantRuleOnBE.shouldSkipFold` 显式跳过，常量子树（如 `ST_GeomFromText('POLYGON(...)')`）每次查询都在 BE 重复求值 | 常量折叠需要 FE 能持有并序列化结果值，`GeometryLiteral` 不存在 |
| 序列化不透明 | blob 对存储/优化器是黑盒；第二层内嵌 bbox、第三层派生列都是「在黑盒上开小窗」的补丁 | 编码格式再怎么扩展，类型系统仍不认识它 |
| 无法向量化 | 每行 decode 成堆上 `GeoShape` 对象再计算，坐标数据无列式布局，SIMD 无从谈起 | 函数层只能减少 decode 次数，无法消除「行式对象」这一形态本身 |

ClickHouse 的做法给出低成本路径：**geo 类型 = 原生嵌套列的别名**。`Point = Tuple(Float64, Float64)`、`Polygon = Array(Array(Point))`——不发明新存储格式，坐标天然列式、天然可向量化。DuckDB 则证明了原生类型 + 内嵌 bbox 之后，R-tree 才开始划算。

## 2. 目标与非目标

### 2.1 目标

| 编号 | 目标 |
|------|------|
| G1 | SQL 层新增 `GEOMETRY` 类型：建表、查询、函数签名、MySQL 协议返回完整支持 |
| G2 | FE 可表示几何字面量（`GeometryLiteral`），解锁 `st_*` 常量折叠 |
| G3 | 存储采用嵌套列式布局（对齐 GeoArrow），坐标可被向量化访问 |
| G4 | segment 级 bbox 元数据（类 ZoneMap），存储裁剪不再依赖用户建派生列 |
| G5 | 与 VARCHAR 方案长期共存，存量表/查询零破坏 |

### 2.2 非目标

- **R-tree 不在本方案交付**：仅预留位置。DuckDB 经验 + Doris LSM 写入模型下，segment 级 bbox 元数据（G4）优先；全局 R-tree 待 G1–G4 落地后按需评估。
- **不做坐标参考系（SRID）管理**：沿用现状（WGS84 经纬度语义），SRID 列入 far-future。
- **不迁移存量数据**：VARCHAR 几何列不做自动转换，提供 `CAST` 与迁移文档。
- **不做 GEOGRAPHY/GEOMETRY 双类型**：单一 `GEOMETRY`，球面/平面语义沿用第一层 WS-B 的 `use_sphere` 参数机制。

## 3. 备选方案对比与决策

### 3.1 存储表示

| 方案 | 说明 | 优点 | 缺点 | 结论 |
|------|------|------|------|------|
| N-1 嵌套列别名（**选定**） | `GEOMETRY` 物理上 = `Struct(type TINYINT, bbox 4×DOUBLE, coords Array(Array(Tuple(DOUBLE, DOUBLE))))` 式布局，复用现有 `ColumnArray`/`ColumnStruct` | 不发明存储格式；嵌套列的读写/压缩/编码全部现成；对齐 GeoArrow，Parquet 互通近乎免费 | 类型分派（Point/Polygon/...）需 type 标记列；空间上比紧凑二进制略胖 | **采用**（ClickHouse 同路线） |
| N-2 定制二进制列（`ColumnGeo` 持有 blob） | 新列类型内部仍存序列化 blob | 迁移成本最低 | 只是把 VARCHAR 换了名字，三个根问题一个都不解决 | 否决 |
| N-3 GeoParquet/WKB 直存 | 存储层直接用 WKB | 生态标准 | WKB 仍是不透明 blob（DuckDB 明确指出读 MBR 都要重解析） | 否决 |

`ColumnGeo` 在 N-1 下退化为一个**薄包装**：组合内部嵌套列并提供几何语义访问器（`get_bbox(row)`、`coords_data()` 裸指针等），而非新的物理列格式。

### 3.2 FE 字面量表示

| 方案 | 说明 | 结论 |
|------|------|------|
| L-1 `GeometryLiteral` 持有规范化 WKT + 惰性解析的坐标结构（**选定**） | FE 折叠结果以 WKT 文本为规范形；下发 BE 时序列化为列式布局 | 人类可读、diff 友好、与 explain/元数据展示天然兼容 |
| L-2 持有 BE 二进制编码 | FE 只当字节数组透传 | FE 无法参与任何计算/化简/比较，折叠意义减半 | 否决 |

## 4. 详细设计（分里程碑）

### 4.1 M1：类型骨架与常量折叠解锁（价值最快兑现）

范围：FE 为主，BE 传输层配合。

1. **类型系统**：FE `GeometryType`（Nereids `DataType` + catalog `Type`）；thrift `TTypeDesc` 新增 `GEOMETRY`；BE `DataTypeGeometry`（此阶段内部仍以现编码承载，M2 才换列布局）。
2. **字面量**：`GeometryLiteral`（WKT 规范形）；`ST_GeomFromText(常量)` 在 FE 直接解析为字面量。
3. **常量折叠**：`shouldSkipFold` 对返回 `GEOMETRY` 的表达式放行；BE 折叠结果经 proto 回传 WKT/WKB。
4. **函数签名双轨**：`st_*` 函数增加 `GEOMETRY` 参数/返回的签名，VARCHAR 签名保留；隐式 cast 规则 `VARCHAR(WKT) → GEOMETRY` 控制在显式函数内（避免全局隐式转换风暴）。

M1 交付后：常量几何子树全部在计划期求值一次，第一层 A5 缓存的「常量侧」问题从根上消失（A5 对非字面量常量仍有价值）。

### 4.2 M2：存储列格式

1. **列布局**（对齐 GeoArrow separated 布局）：

```text
GEOMETRY 列 =
  type:  TINYINT 列（GeoShapeType）
  bbox:  4 × DOUBLE 列（xmin/ymin/xmax/ymax）
  x, y:  DOUBLE 列（全部顶点坐标，扁平）
  ring_offsets / geom_offsets: 偏移列（Array 嵌套语义，复用现有 offsets 机制）
```

2. **segment 级 bbox 元数据（G4）**：列 writer 聚合本 segment 所有行 bbox 的并集，写入 segment footer（与 ZoneMap 同位置、同消费方式）；查询期空间谓词先与 segment bbox 求交裁剪。第三层的谓词改写在此升级为引擎原生能力，不再需要用户建派生列。
3. **建表/Schema Change**：`GEOMETRY` 可作普通 value 列；不允许作 key / 分区 / 分桶列（首期）。

### 4.3 M3：向量化执行

1. `st_*` 函数对 `ColumnGeo` 的执行路径：直接消费扁平坐标列，热点函数（`st_x/st_y/st_distance/点在框内`）SIMD 化；复杂精算（polygon 相交）仍走 S2，但从列构造 S2 对象比 decode blob 更廉价（零拷贝坐标视图）。
2. 保留 VARCHAR 路径：两套执行按输入列类型分派，共享 `GeoShape` 精算内核。

### 4.4 M4（预留，不承诺）：空间索引

segment bbox 元数据（M2）+ 排序键上的 Hilbert/S2 排序（导入侧 `ST_Hilbert` 类函数聚簇空间局部性）已覆盖多数场景；全局 R-tree 仅当上述手段被证明不足时立项。

## 5. 兼容性分析

| 维度 | 影响 | 结论/缓解 |
|------|------|-----------|
| 存量 VARCHAR 几何表 | 完全不动，双轨长期共存 | 兼容 |
| 函数生态 | 同名函数双签名；`GEOMETRY` 与 VARCHAR 混用时按显式 cast 规则解析 | 需仔细定义重载决议顺序（待评审） |
| MySQL 协议 | `GEOMETRY` 返回类型映射（MySQL 有原生 GEOMETRY 类型码，或降级 TEXT/WKT） | 待评审：对 BI 工具兼容性影响大 |
| 导入导出 | CSV 以 WKT 交换；Parquet 对接 GeoParquet（M2 布局对齐后成本低）；Stream Load 需类型转换支持 | 分里程碑交付 |
| 升级/降级 | M1 起 thrift/proto 有新类型枚举，降级后含 `GEOMETRY` 列的表不可读 | 大版本特性门槛，随版本发布说明 |
| 备份恢复 / CCR | 新列格式需同步支持 | M2 的验收项 |

## 6. 测试方案

| 类别 | 内容 |
|------|------|
| FE UT | 类型解析、字面量、折叠规则、签名决议、cast 规则 |
| BE UT | 列布局读写、segment bbox 元数据、双路径（VARCHAR/GEOMETRY）结果对拍 |
| 回归 | 存量 spatial 回归零 diff；新增 `GEOMETRY` 建表/导入/查询/函数全链路套件；折叠后 explain 断言 |
| 兼容 | 升级演练（旧表 + 新表混合）、MySQL 客户端/JDBC/BI 工具连通性矩阵 |
| 性能 | 与 VARCHAR 路径的端到端对比（点查、范围过滤、聚合），segment bbox 裁剪率报告 |

## 7. 实施计划（里程碑级）

| 里程碑 | 内容 | 规模预估 |
|--------|------|----------|
| M1 | 类型骨架 + `GeometryLiteral` + 常量折叠解锁 + 函数双签名 | 1 个版本周期，FE 为主 |
| M2 | 存储列格式 + segment bbox 元数据 + 建表/导入 | 1–2 个版本周期，BE 存储为主 |
| M3 | 向量化执行路径 + Parquet/GeoParquet 互通 | 1 个版本周期 |
| M4 | （按需）空间聚簇 / R-tree 评估 | 不承诺 |

每个里程碑独立可交付、独立有收益（M1 解锁折叠即有感知），避免大版本工程「全有或全无」。

## 8. 风险与应对

| 风险 | 等级 | 应对 |
|------|------|------|
| 工程面横跨 FE/BE/存储/协议/生态，周期长、易烂尾 | 高 | 里程碑切分保证每步独立有收益；M1 单独立项先行 |
| 函数双签名的重载决议引发隐性行为变化（VARCHAR 输入被意外 cast） | 高 | 隐式 cast 白名单收紧 + 全量回归对拍；决议规则作为 M1 评审重点 |
| MySQL 协议 / BI 生态兼容问题 | 中 | 返回类型可配置（原生 GEOMETRY 码 vs TEXT/WKT 降级）；连通性矩阵测试 |
| 列布局定型后难改（持久化格式） | 中 | 对齐 GeoArrow 标准而非自创；格式带版本号 |
| 与前三层改动的代码冲突 | 低 | 前三层收敛在函数层；本层 M1 仅 FE，天然错开 |

## 9. 待评审确认项

1. **方向确认**：是否接受「嵌套列别名（N-1）+ 里程碑推进」的总路线（本次评审的核心问题）。
2. **M1 是否单独立项先行**：常量折叠解锁收益独立且改动集中在 FE。
3. **MySQL 协议返回策略**：原生 GEOMETRY 类型码 vs WKT 文本降级。
4. **`GEOMETRY` 与 VARCHAR 混用的 cast/决议规则**（详细规则在 M1 设计中给出，此处确认原则：显式优先、隐式最小化）。
5. **SRID 是否彻底排除在本层之外**（建议是，单独 far-future）。

## 参考

- [`架构建议.md`](./架构建议.md) — 分层路线，本方案为第四层
- [`constant-fold-geo-analysis.md`](./constant-fold-geo-analysis.md) — 根问题 1 的现状分析（`shouldSkipFold`）
- [`geo-l2-bbox-encoding-design.md`](./geo-l2-bbox-encoding-design.md) / [`geo-l3-predicate-pruning-design.md`](./geo-l3-predicate-pruning-design.md) — 第二/三层（本层将其能力升级为引擎原生）
- ClickHouse：geo 类型 = 原生嵌套列别名（`Point = Tuple(Float64, Float64)`）
- DuckDB Spatial：原生 GEOMETRY + cached envelope + R-tree（STR bulk load + `ST_Hilbert`）
- GeoArrow / GeoParquet：列式几何布局标准
