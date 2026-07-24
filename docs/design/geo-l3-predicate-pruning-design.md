# Doris Geo 技术方案：谓词改写与存储裁剪（第三层·过滤时机）

| 项 | 内容 |
|----|------|
| 文档状态 | 待评审 |
| 层级定位 | 《[架构建议](./架构建议.md)》第三层：过滤时机（借鉴 ClickHouse，不建 R-tree） |
| 前置依赖 | P1 依赖用户建 bbox/坐标派生列，或第二层编码内嵌 bbox（[`geo-l2-bbox-encoding-design.md`](./geo-l2-bbox-encoding-design.md)）；P2 无硬前置 |
| 涉及模块 | FE Nereids 改写规则、生成列；BE geo 函数（新增 cell 函数）；存储层零改动（复用 ZoneMap/BloomFilter） |

---

## 1. 背景与问题

第一、二层优化后，Geo 查询仍是**全表逐行计算**：`WHERE ST_Contains(polygon, point_col)` 必须扫描每一行、每行至少做一次 bbox 判断。OLAP 引擎对普通数值谓词有成熟的存储级裁剪（ZoneMap 跳 page/segment、BloomFilter、前缀索引），但 Geo 谓词是黑盒函数调用，存储层完全无法参与。

ClickHouse 的实践证明了另一条路：**不建空间索引，而是把空间谓词翻译成存储层看得懂的普通谓词**。其 `pointInPolygon` 在主键分析时用多边形 bbox 与 granule 范围求交做粒度裁剪；Polygon Dictionary 把「亿级点找所属区域」变成字典 lookup。这三件套让 ClickHouse 在没有 R-tree 的情况下性能不差。

本方案把该思路落到 Doris，包含三个独立子项：

| 子项 | 问题 | 手段 |
|------|------|------|
| **P1 bbox 谓词补写** | 常量多边形过滤点列，全表精算 | FE 自动补写坐标范围谓词，ZoneMap 裁剪 |
| **P2 S2 CellId 生成列** | bbox 对狭长/凹多边形选择率差 | 常量多边形算 S2 covering，改写为 cell 区间过滤 |
| **P3 多边形字典** | 反向地理编码（点找区域）无索引可用 | 网格索引的 `geo_dict` 能力 |

## 2. 目标与非目标

### 2.1 目标

| 编号 | 目标 | 度量 |
|------|------|------|
| G1 | 典型「常量多边形 × 点列」查询获得存储级裁剪 | 扫描行数显著下降（ZoneMap 命中跳过 page/segment） |
| G2 | 改写对用户透明且语义严格不变 | 补写谓词只做 AND 补强，原 Geo 谓词保留兜底；结果逐行一致 |
| G3 | 提供比 bbox 更精细的裁剪选项（P2） | 狭长/凹多边形场景下 false positive 率低于纯 bbox |

### 2.2 非目标

- **不建 R-tree / 全局空间索引**：LSM 存储下维护成本高，且第四层原生类型就绪前不划算（DuckDB 经验）。
- **不改存储引擎**：只复用现有 ZoneMap / BloomFilter / 谓词下推通道，不新增索引结构。
- **不自动创建生成列**：P1/P2 的派生列由用户 DDL 声明（或后续工具建议），引擎只负责「有列就用」。
- **P3 不做成通用索引**：定位为反向地理编码专用能力，业务驱动才立项。

## 3. P1：bbox / ZoneMap 谓词补写

### 3.1 备选方案对比

| 方案 | 说明 | 优点 | 缺点 | 结论 |
|------|------|------|------|------|
| P1-a 基于用户坐标列改写（**选定为首期**） | 表已有 `lon`/`lat`（或生成列 `st_x(pt)`），FE 补写 `lon BETWEEN ... AND lat BETWEEN ...` | 纯 FE 改写规则，存储零改动；ZoneMap 天然生效 | 依赖建模约定（需要能把几何列与坐标列关联起来） | **首期采用**，关联方式见 §3.3 |
| P1-b 基于第二层 blob bbox + 存储感知 | 存储层直接理解 VARCHAR blob 内的 bbox 段建 ZoneMap | 无需用户建列 | 存储层需理解 geo 编码，破坏「blob 对存储不透明」的边界；改动大 | 否决（越层耦合） |
| P1-c 不做，只靠第二层函数内快速拒绝 | — | 零成本 | 仍要扫全表读出 blob 列，IO 不省 | 作为无派生列时的默认路径 |

关键区别：第二层的 bbox 快速拒绝**省 CPU 不省 IO**（行仍要读出来）；P1 的谓词补写**省 IO**（page/segment 级跳过）。两者叠加才完整。

### 3.2 改写规则定义

模式（Nereids 表达式改写规则，计划期常量可求值时触发）：

```sql
-- 原查询
SELECT ... FROM pts
WHERE ST_Contains(ST_GeomFromText('POLYGON((...))'), pt);   -- pt 与 lon/lat 关联

-- 改写后（AND 补强，原谓词保留）
SELECT ... FROM pts
WHERE lon >= {xmin} AND lon <= {xmax}
  AND lat >= {ymin} AND lat <= {ymax}
  AND ST_Contains(ST_GeomFromText('POLYGON((...))'), pt);
```

- 常量多边形的 bbox 在 FE 计划期计算。FE 无法 decode BE 编码，但**可以解析 WKT 文本**：改写只对 `ST_GeomFromText('...常量...')` 类可静态求 bbox 的模式生效；不可静态分析的常量（如来自子查询）不改写。
- 适用函数：`st_contains` / `st_intersects`（对点列语义均为「点在 bbox 内」的必要条件）。`st_disjoint` 是反选，不适用。
- 跨反子午线多边形：FE 侧检测后放弃改写（保守），与第二层退化策略一致。
- **正确性论证**：补写谓词是原谓词的必要条件（点不在 bbox 内 ⇒ 必不在多边形内），AND 连接下只会减少进入精算的行，false positive 由保留的原谓词兜底，结果集严格不变。

### 3.3 几何列与坐标列的关联方式（待评审重点）

改写的前提是 FE 知道「`pt` 列的经纬度就是 `lon`/`lat` 列」。候选：

| 方式 | 说明 | 评估 |
|------|------|------|
| a. 生成列反向识别（**推荐**） | 用户建 `lon DOUBLE AS (st_x(pt))`、`lat DOUBLE AS (st_y(pt))`；FE 从生成列表达式反查关联 | 声明式、无新语法；需确认生成列允许 `st_x/st_y`（FE 已有 `GeneratedColumnDesc` 框架） |
| b. 直接匹配查询模式 | 只改写 `ST_Contains(常量, ST_Point(lon, lat))` 形态（几何由坐标列现场构造） | 零建模要求，最简单；覆盖面窄但常见 |
| c. 表属性显式声明 | `PROPERTIES("geo.bbox.columns" = "pt:lon,lat")` | 灵活但新增配置面 |

建议 b + a 组合：b 先落地（纯模式匹配，无任何前置），a 作为通用化第二步。

### 3.4 改动清单

| 位置 | 改动 |
|------|------|
| FE Nereids | 新增改写规则（放在谓词下推之前，使补写谓词能继续下推至存储层）；WKT 轻量 bbox 解析器（仅头点扫描，不引入完整 GIS 库） |
| BE | 无（ZoneMap 对补写的 double 谓词天然生效） |
| 回归 | 改写前后结果一致性 + explain 验证谓词已补写、扫描行数下降 |

## 4. P2：S2 CellId 生成列 + 区间改写

### 4.1 动机

bbox 对狭长、倾斜、凹形多边形选择率差（bbox 面积远大于多边形本身）。S2 covering 用一组分层 cell 逼近多边形，逼近质量可控，且 cell id 是 **BIGINT**——ZoneMap / BloomFilter / 前缀索引全部天然可用。这是与 Doris 现有 S2 依赖最契合的「索引替代」。

### 4.2 设计

1. **新增标量函数 `st_s2cell(geo_point, level)`**（BE，基于已依赖的 S2 库 `S2CellId`；`S2RegionCoverer` 目前未在代码中使用，需新引入调用）：返回点在指定 level 的 cell id。用户建生成列：

```sql
CREATE TABLE pts (
    pt VARCHAR,
    cell BIGINT AS (st_s2cell(pt, 13))    -- level 由数据密度决定
    ...
);
```

2. **FE 改写规则**：常量多边形在计划期计算 covering（一组 cell 区间；同 level 下每个 cell 对应一个 id 区间），改写为：

```sql
-- 原
WHERE ST_Contains(常量polygon, pt)
-- 补写（AND 补强）
WHERE (cell BETWEEN c1_lo AND c1_hi OR cell BETWEEN c2_lo AND c2_hi OR ...)
  AND ST_Contains(常量polygon, pt)
```

   covering 计算位置二选一（待评审）：FE 侧 Java S2 库（引依赖）或 FE 通过 BE RPC 求值（复用常量折叠通道）。倾向前者，避免计划期 RPC。

3. **正确性**：covering 是多边形的超集 ⇒ 补写谓词是必要条件，与 P1 同理严格不损结果。

### 4.3 与 P1 的关系

P1 是 P2 的低配版：P1 只需 double 列与 bbox，P2 需要 S2 covering 与 cell 语义但裁剪更准。两者互不排斥（可同时补写），建议 P1 先行验证改写通道，P2 复用同一条规则框架。

## 5. P3：多边形字典（反向地理编码专用）

**触发条件**：业务出现高频「亿级点 × 固定多边形集（行政区划等）找所属区域」，普通 `ST_Contains` join 是笛卡尔式扫描。

**形态**（参考 ClickHouse Polygon Dictionary）：

- 多边形集加载为内存字典，构建网格索引（递归划分，格内多边形数少于阈值则停）；
- 对外暴露表函数或 UDF：`geo_dict_lookup(dict_name, lon, lat) -> region_id`，点查代价 O(格内候选数)；
- 首期可实现为 BE 内置函数 + 从内部表加载字典，不动优化器。

P3 与 P1/P2 独立，纯业务驱动，本文只锁定形态与边界，立项时单独出详细设计。

## 6. 兼容性分析

| 维度 | 影响 | 结论 |
|------|------|------|
| 查询结果 | P1/P2 均为 AND 补强 + 原谓词兜底 | 严格不变 |
| 无派生列的表 | 规则不触发，走现路径 | 兼容 |
| 改写可见性 | explain 中出现补写谓词，plan shape 变化 | 需在 explain 文档说明；提供 session variable 关闭改写（排障用） |
| 生成列 + geo 函数 | 需确认 `GeneratedColumnDesc` 对 `st_x/st_y/st_s2cell` 的允许列表 | P1-a / P2 的前置检查项 |
| 导入开销 | 生成列在导入期计算（每行一次 `st_x` 等） | 用户建列时的已知代价，文档说明 |

## 7. 测试方案

| 类别 | 内容 |
|------|------|
| FE UT | 改写规则：触发/不触发条件（常量 WKT、跨反子午线放弃、非常量不改写）；bbox/covering 计算正确性 |
| 回归 | 开/关改写两态结果对拍；explain 断言补写谓词存在；`sql_functions/spatial_functions` 存量零 diff |
| 性能 | 城市多边形 × 全国点表：扫描行数（profile 的 rows read）与耗时对比；狭长多边形场景 P2 vs P1 |
| 正确性压测 | 随机多边形 + 随机点集，改写前后全量对拍 |

## 8. 实施计划

| PR | 内容 | 依赖 |
|----|------|------|
| PR-P1a | 模式 b 改写（`ST_Point(lon,lat)` 形态）+ WKT bbox 解析 + UT/回归 | 无 |
| PR-P1b | 生成列反向识别（模式 a）+ 生成列 geo 函数允许列表确认 | PR-P1a |
| PR-P2a | `st_s2cell` 函数 + 生成列支持 | 无（可并行） |
| PR-P2b | covering 计算 + cell 区间改写 | PR-P1a（复用规则框架）、PR-P2a |
| P3 | 业务立项后单独设计 | — |

## 9. 风险与应对

| 风险 | 等级 | 应对 |
|------|------|------|
| FE 侧 WKT/bbox/covering 计算与 BE 精算存在数值口径差，导致补写谓词过紧（漏结果） | 高 | 补写时按容差外扩 bbox/covering；随机对拍压测作为合入门槛 |
| 改写规则误触发（非常量、语义不满足必要条件的模式） | 中 | 触发条件白名单式收紧；session variable 可关闭 |
| covering 区间数过多导致 OR 谓词爆炸 | 中 | `max_cells` 上限（如 16/32），超限退化为 P1 bbox 或放弃改写 |
| FE 引入 Java S2 依赖的维护成本 | 中 | 备选 BE RPC 求值；评审定夺 |
| 生成列导入开销引发用户抱怨 | 低 | 文档明确代价与收益适用场景 |

## 10. 待评审确认项

1. **P1 关联方式**：先落模式 b（查询形态匹配）再做模式 a（生成列反查），还是直接做 a。
2. **covering 计算位置**：FE Java S2 依赖 vs 计划期 BE RPC。
3. **默认开关**：改写规则默认开启还是首期默认关闭（灰度一个版本）。
4. **P2 cell level 选择**：固定 level 生成列（简单）vs 多 level（复杂，首期不建议）。
5. **P3 是否本期立项**：取决于业务输入。

## 参考

- [`架构建议.md`](./架构建议.md) — 分层路线，本方案为第三层
- [`geo-l2-bbox-encoding-design.md`](./geo-l2-bbox-encoding-design.md) — 第二层（函数内快速拒绝，省 CPU；本层省 IO，互补）
- ClickHouse：`pointInPolygon` 主键 granule 裁剪、Polygon Dictionary
- Google S2：`S2CellId` / `S2RegionCoverer`（BE 已依赖 S2 库，coverer 为新增调用）
- FE 生成列框架：`org.apache.doris.nereids.trees.plans.commands.info.GeneratedColumnDesc`
