# Geo 函数常量折叠对比分析：Doris vs StarRocks

## 背景

在数据库优化器中有**常量折叠（Constant Folding）** 优化：如果一个函数的参数全是字面常量，FE（Frontend）在生成执行计划前直接算出结果，避免运行时重复计算。例如：

```sql
SELECT 1 + 1;      -- FE 直接折叠成 SELECT 3
SELECT ABS(-5);     -- FE 直接折叠成 SELECT 5
```

但对于地理空间函数（`st_*`），两个系统目前均不做常量折叠，但**原因和代码路径完全不同**。

---

## 1. Doris 的实现

### 1.1 类型系统

Doris **有** `GEOMETRY` 类型的概念（BE 端用自定义二进制编码），但 FE 的 Java 类型系统中**无法表示 GEOMETRY 字面量**——没有对应的 `Literal` 实现类。

### 1.2 常量折叠架构

Doris 有两条常量折叠路径：

| 路径 | 类名 | 方式 |
|------|------|------|
| FE 本地计算 | `FoldConstantRuleOnFE` | FE Java 直接算 |
| 委托 BE 计算 | `FoldConstantRuleOnBE` | 序列化成 Thrift 发给 BE 算，结果缓存 |

### 1.3 跳过逻辑

在 `FoldConstantRuleOnBE.shouldSkipFold()` 中**显式跳过**所有 `st_*` 函数：

```java
// FoldConstantRuleOnBE.java 第 239-242 行
// Frontend can not represent geo types
if (expr instanceof BoundFunction && ((BoundFunction) expr).getName().toLowerCase().startsWith("st_")) {
    return true;
}
```

这个方法在 `collectConst()` 中被调用：

```java
// FoldConstantRuleOnBE.java 第 206 行
if (expr.isConstant() && !expr.isLiteral() && !expr.anyMatch(e -> shouldSkipFold((Expression) e))) {
```

`anyMatch` 会遍历整个表达式树，**任何一个子节点匹配 `shouldSkipFold`，整个表达式就被跳过**。

### 1.4 跳过的深层原因

即使 `FoldConstantRuleOnBE` 把表达式发给 BE 算出了结果，BE 返回一个 GEOMETRY 二进制值，FE 也**没有对应的 `GeometryLiteral` 类型来承接这个结果**。所以连发都不发了。

---

## 2. StarRocks 的实现

### 2.1 类型系统

StarRocks **没有** `GEOMETRY` 类型（`PrimitiveType.java` 枚举中无 GEOMETRY）。Geo 函数的参数和返回值在 FE 视角都是 `VARCHAR` 类型。几何数据在 BE 中以 WKT 文本或字符串形式承载。

### 2.2 常量折叠架构

StarRocks 只有一条常量折叠路径：

```
FoldConstantsRule.visitCall()
  → ScalarOperatorEvaluator.evaluation()
    → 查找 @ConstantFunction 注册表
```

通过 `@ConstantFunction` 注解将 FE 端 Java 实现注册到求值器中：

```java
// ScalarOperatorEvaluator.registerFunctions()
Class<?> clazz = ScalarOperatorFunctions.class;
for (Method method : clazz.getDeclaredMethods()) {
    ConstantFunction annotation = method.getAnnotation(ConstantFunction.class);
    registerFunction(mapBuilder, method, annotation);
}
```

### 2.3 跳过逻辑（隐式）

StarRocks **没有**显式的 `shouldSkipFold` 逻辑。它的常量折叠流程是：

```java
// FoldConstantsRule.java 第 331-351 行
public ScalarOperator visitCall(CallOperator call, ...) {
    if (call.isAggregate()) return call;

    // 处理数组函数等特殊逻辑
    Optional<ScalarOperator> mayBeConstant = tryToProcessConstantArrayFunctions(call);
    if (mayBeConstant.isPresent()) return mayBeConstant.get();

    if (notAllConstant(call.getChildren())) return call;

    // 通过 @ConstantFunction 注册表求值
    return ScalarOperatorEvaluator.INSTANCE.evaluation(call, needMonotonicFunc);
}
```

而 `evaluation()` 方法：

```java
// ScalarOperatorEvaluator.java 第 183-189 行
FunctionSignature signature = new FunctionSignature(fn.functionName().toUpperCase(), argTypes, fn.getReturnType());
FunctionInvoker invoker = getFunctionInvoker(signature);
if (invoker == null) {
    return root;  // 没注册 → 不折叠 → 原样返回
}
```

由于 `ScalarOperatorFunctions.java` 和 `MetaFunctions.java` 中**没有任何 `st_*` 函数被 `@ConstantFunction` 注解注册**，所有 geo 函数的常量折叠请求都会走到 `invoker == null → return root`，即**静默跳过，不折叠**。

### 2.4 BE 实现特征

StarRocks 的 geo 函数注册在 `FunctionSet.java` 中，BE 实现在单个文件中：

```
be/src/exprs/geo_functions.h
be/src/exprs/geo_functions.cpp
```

使用 `TYPE_VARCHAR` 作为输入输出类型，在 BE 中以 `std::string` 承载几何数据。

---

## 3. 对比总结

### 3.1 核心差异

| 维度 | Doris | StarRocks |
|------|-------|-----------|
| **FE 是否有 GEOMETRY 类型** | 有概念但无法表示字面量 | 完全没有 |
| **跳过方式** | 显式 `shouldSkipFold(st_*)` | 不注册 `@ConstantFunction`，静默跳过 |
| **OnBE 兜底** | 有 `FoldConstantRuleOnBE`（但也跳过） | 无此机制 |
| **FE 能否表示 geo 常量** | ❌ 缺 `GeometryLiteral` | ✅ 理论上可以（VARCHAR Literal 即可） |
| **注释说明** | 有明确注释 "Frontend can not represent geo types" | 无特殊注释 |

### 3.2 代码路径对比

```
Doris:
  FoldConstantRule
    ├─ FoldConstantRuleOnFE   → st_* 不处理（FE 本地算不了）
    └─ FoldConstantRuleOnBE   → collectConst() → shouldSkipFold(st_*) = true → 跳过

StarRocks:
  FoldConstantsRule.visitCall()
    → ScalarOperatorEvaluator.evaluation()
      → getFunctionInvoker(st_*) = null → return root（不折叠）
```

### 3.3 最终效果

两者**完全一致**——`st_*` 函数即使参数全是常量，也不会在 FE 编译期折叠，必须下推到 BE 运行时执行：

```sql
-- Doris 和 StarRocks 都不做常量折叠
SELECT ST_Contains(
    ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'),
    ST_Point(1, 1)
);

-- 每次查询都会在 BE 重新执行，不会变成计划里的常量 TRUE
```

---

## 4. 优化可行性分析

### 4.1 StarRocks 的优化路径更简单

由于 StarRocks 用 `VARCHAR` 表示 geo 数据，不存在 Doris 中"FE 无法表示 GEOMETRY 字面量"的问题。理论上的优化方案：

```java
// 在 ScalarOperatorFunctions.java 中注册
@ConstantFunction(name = "st_point", argTypes = {DOUBLE, DOUBLE}, returnType = VARCHAR)
public static ConstantOperator stPoint(ConstantOperator lng, ConstantOperator lat) {
    // 直接返回 WKT 文本
    return ConstantOperator.createVarchar(
        "POINT (" + lng.getDouble() + " " + lat.getDouble() + ")");
}
```

但需要注意：
- 需要为每个想折叠的 `st_*` 函数提供 FE 端 Java 实现
- 需保持 FE 和 BE 端计算结果一致（精度、边界条件等）
- 纯字符串函数（如 `ST_AsText`）最容易实现折叠
- 空间关系函数（如 `ST_Contains`）需要 FE 端引入 S2/Spatial4j 等库

### 4.2 Doris 的优化路径更复杂

Doris 需要先解决类型表示问题：
- 新增 `GeometryLiteral` 类，能承载二进制编码的 GEOMETRY 值
- 然后才能在 `FoldConstantRuleOnFE` 中处理折叠
- 或让 `FoldConstantRuleOnBE` 不跳过（BE 返回结果后能构造 `GeometryLiteral` 承接）

---

## 5. 关键代码文件索引

### Doris

| 文件 | 作用 |
|------|------|
| `fe/.../nereids/rules/expression/rules/FoldConstantRuleOnBE.java` | `shouldSkipFold()` 显式跳过 `st_*` |
| `fe/.../nereids/rules/expression/rules/FoldConstantRuleOnFE.java` | FE 本地折叠（也不处理 geo） |
| `fe/.../nereids/rules/expression/rules/FoldConstantRule.java` | 组合 OnFE + OnBE 的入口 |

### StarRocks

| 文件 | 作用 |
|------|------|
| `fe/.../optimizer/rewrite/scalar/FoldConstantsRule.java` | 常量折叠主规则，`visitCall()` 入口 |
| `fe/.../optimizer/rewrite/ScalarOperatorEvaluator.java` | `@ConstantFunction` 注册表查询，`invoker == null` 则不折叠 |
| `fe/.../optimizer/rewrite/ScalarOperatorFunctions.java` | 所有 `@ConstantFunction` 注册的地方，**不含 st_*** |
| `fe/.../catalog/FunctionSet.java` | Geo 函数名常量注册 |
| `fe/.../type/PrimitiveType.java` | 无 GEOMETRY 枚举 |
