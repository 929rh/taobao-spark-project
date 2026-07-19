# Spark SQL 学习总结笔记

> 本笔记整理自 `data/02-SQL` 目录下的 16 个示例代码文件，系统梳理 Spark SQL 的核心 API 与典型用法。

---

## 一、SparkSession 入口

Spark 2.0 后，`SparkSession` 统一取代了 `SQLContext` 与 `HiveContext`，是 Spark SQL 的统一入口。

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 2) \  # 设置 shuffle 分区数
    .getOrCreate()

sc = spark.sparkContext  # 仍可获取 SparkContext 用于 RDD 操作
```

常用配置项：
- `spark.sql.shuffle.partitions`：shuffle 后分区数（默认 200）
- `spark.sql.warehouse.dir`：Hive 仓库目录
- `hive.metastore.uris`：Hive Metastore 地址
- `enableHiveSupport()`：开启 Hive 支持

对应示例：[00-sparksession.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/00-sparksession.py)

---

## 二、DataFrame 的多种构建方式

DataFrame 是 Spark SQL 的核心数据结构，相当于带 schema 的 RDD（类似关系数据库的表）。

### 2.1 RDD → DataFrame（List schema）

```python
rdd = sc.textFile("...").map(lambda x: x.split(",")).map(lambda x: (x[0], int(x[1])))
df = spark.createDataFrame(rdd, schema=["Name", "Age"])
```

对应示例：[01-dataframe.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/01-dataframe.py)

### 2.2 RDD → DataFrame（StructType schema）

```python
from pyspark.sql.types import StructType, StringType, IntegerType

schema = StructType() \
    .add("name", StringType(), nullable=True) \
    .add("age", IntegerType(), nullable=True)
df = spark.createDataFrame(rdd, schema=schema)
```

对应示例：[02-dataframe.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/02-dataframe.py)

### 2.3 RDD.toDF() 简写

```python
df = rdd.toDF(schema=schema)
```

对应示例：[03-dataframe.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/03-dataframe.py)

### 2.4 pandas DataFrame → Spark DataFrame

```python
import pandas as pd
pdf = pd.DataFrame({'id':[1,2,3], 'name':['a','b','c'], 'age':[10,20,30]})
df = spark.createDataFrame(pdf)
```

对应示例：[04-datframe.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/04-datframe.py)

---

## 三、数据读取（Read）

统一 API：`spark.read.format(格式).option(...).schema(...).load(路径)`

| 数据源 | format 参数 | 说明 |
|--------|-------------|------|
| text   | `text`      | 仅一列，默认列名 `value` |
| json   | `json`      | 自动推断 schema |
| csv    | `csv`       | 需指定 sep/header/encoding |
| parquet| `parquet`   | 列式存储，自带 schema |

CSV 读取示例：

```python
df = spark.read.format("csv") \
    .option("sep", ";") \
    .option("header", True) \
    .option("encoding", "utf-8") \
    .schema("name STRING, age INT, job STRING") \  # 也可传 StructType
    .load("file:///path/people.csv")
```

> 注：`schema` 可用字符串 DDL 语法或 `StructType` 对象两种方式。

对应示例：[05-api.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/05-api.py)

---

## 四、DSL 与 SQL 两种编程风格

Spark SQL 支持两种风格执行查询，二者底层编译为相同的 Catalyst 优化逻辑。

### 4.1 SQL 风格（注册临时视图）

```python
df.createTempView("people")                     # 注册临时视图（已存在则报错）
df.createOrReplaceTempView("people")            # 注册或替换
df.createGlobalTempView("people")               # 全局视图，使用时需加 global_temp. 前缀

spark.sql("SELECT job, count(*) AS cnt FROM people WHERE age>10 GROUP BY job").show()
```

### 4.2 DSL 风格（链式调用）

```python
df.select("name", "age").where("age>10").sort("name").show()
df.filter("age>10").select("name", "age").show()
df.groupBy("job").count().show()
```

对应示例：[06-DSL.api.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/06-DSL.api.py)

---

## 五、WordCount 两种实现

### 5.1 SQL 风格

```python
rdd = sc.textFile("...").flatMap(lambda x: x.split(" ")).map(lambda x: [x])
df = rdd.toDF(['word'])
df.createOrReplaceTempView("words")
spark.sql("SELECT word, count(*) FROM words GROUP BY word ORDER BY count(*) DESC").show()
```

### 5.2 DSL 风格（使用 functions）

```python
from pyspark.sql import functions as F

df = spark.read.format("text").load("...")
df2 = df.withColumn("value", F.explode(F.split(df['value'], ' ')))
df2.groupBy("value").count().show()
```

对应示例：[07-wordcount.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/07-wordcount.py)

---

## 六、数据清洗

### 6.1 去重

```python
df2 = df.dropDuplicates()                  # 全字段去重
df2 = df.dropDuplicates(subset=["name"])   # 按指定字段去重
```

### 6.2 缺失值删除

```python
df.dropna()                                       # 删除含 null 的行
df.dropna(thresh=2, subset=['name','age'])        # 子集中非空少于 2 的行删除
```

### 6.3 缺失值填充

```python
df.fillna(value='loss', subset="job")             # 单值填充
df.fillna({'name':'未知姓名', 'age':1, "job":"worker"})  # 按列规则填充
```

对应示例：[08-data_clear.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/08-data_clear.py)

---

## 七、数据写出（Write）

统一 API：`df.write.mode(...).format(...).option(...).save(路径)`

```python
# 1. text（仅支持单列，需用 concat_ws 拼接）
df.select(F.concat_ws("---", "user_id", "movie_id", "rank", "ts")) \
    .write.mode("overwrite").format("text").save(".../text")

# 2. csv
df.write.mode("overwrite").format("csv") \
    .option("sep", ";").option("header", True).save(".../csv")

# 3. json
df.write.mode("overwrite").format("json").save(".../json")

# 4. parquet
df.write.mode("overwrite").format("parquet").save(".../parquet")
```

`mode` 取值：`overwrite` | `append` | `ignore` | `error`(默认)

对应示例：[09-dataframe-write.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/09-dataframe-write.py)

---

## 八、JDBC 读写 MySQL

需通过 `spark.jars` 指定 MySQL 驱动 jar 包路径。

```python
spark = SparkSession.builder \
    .config("spark.jars", "/path/mysql-connector-java-8.0.13.jar") \
    .getOrCreate()

url = "jdbc:mysql://node1:3306/spark?useSSL=false&characterEncoding=utf8"
prop = {
    "user": "root",
    "password": "123456",
    "driver": "com.mysql.cj.jdbc.Driver"   # MySQL8+ ; MySQL5.x 用 com.mysql.jdbc.Driver
}

# 写入
df.write.mode("overwrite").jdbc(url=url, table="people_data", properties=prop)

# 读取
df2 = spark.read.jdbc(url=url, table="people_data", properties=prop)
```

对应示例：[10-dataframe-jdbc.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/10-dataframe-jdbc.py)

---

## 九、UDF 用户自定义函数

三种注册方式，可同时支持 SQL 与 DSL 调用。

```python
def num_ride_10(num):
    return num * 10

# 方式1：spark.udf.register —— 同时注册 SQL 函数与返回 DSL 可用的 Column
udf2 = spark.udf.register('udf1', num_ride_10, IntegerType())
df.selectExpr("udf1(num)").show()      # SQL 风格
df.select(udf2(df['num'])).show()      # DSL 风格

# 方式2：F.udf —— 仅 DSL 可用
udf3 = F.udf(num_ride_10, IntegerType())
df.select(udf3(df['num'])).show()
```

对应示例：[11-udf.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/11-udf.py)

---

## 十、窗口函数

通过 SQL 的 `OVER()` 子句实现分组排序、聚合等。

```python
spark.sql("""
    SELECT *,
        AVG(score) OVER(PARTITION BY id) AS avg_score,
        RANK()   OVER(PARTITION BY id ORDER BY score DESC) AS rank,
        NTILE(2) OVER(ORDER BY score DESC) AS ntile
    FROM stu
""").show()
```

常用窗口函数：
- 聚合类：`AVG / SUM / COUNT / MAX / MIN`
- 排序类：`RANK / DENSE_RANK / ROW_NUMBER`
- 分桶类：`NTILE(n)`

对应示例：[12-window-function.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/12-window-function.py)

---

## 十一、Spark on Hive 集成

通过 `enableHiveSupport()` 直接读取 Hive 表。

```python
spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://node1:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.table("sparkinhive").select("id")
row_list = df.collect()   # 收集到 Driver 端
```

对应示例：[13-spark_on-hive.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/13-spark_on-hive.py)

---

## 十二、通过 Thrift Server 访问（pyhive）

不依赖 Spark 运行环境，直接通过 JDBC/Thrift 协议访问 Hive/Spark SQL。

```python
from pyhive import hive

conn = hive.Connection(host='node1', port=10000, username='root')
cursor = conn.cursor()
cursor.execute("SELECT * FROM sparkinhive")
print(cursor.fetchall())
```

对应示例：[14-jdbc_spark_thrift_server.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/14-jdbc_spark_thrift_server.py)

---

## 十三、综合实战案例

完整流程：JSON 读取 → 清洗 → 多表 JOIN → 子查询 → 持久化 → 写出 MySQL/Hive。

核心要点：

1. **多步清洗**：`dropna` + `filter` + `select` 链式处理
2. **复杂 SQL**：支持子查询、JOIN、聚合、`HAVING`、`from_unixtime` 等函数
3. **持久化**：对多次使用的 DataFrame 调用 `persist(StorageLevel.MEMORY_AND_DISK_SER)` 提升性能，用完后 `unpersist()` 释放
4. **多目标输出**：同一 DataFrame 可同时写入 MySQL（`jdbc`）与 Hive（`saveAsTable`）

```python
from pyspark import StorageLevel

# 清洗
df = spark.read.format("json").load("...") \
    .dropna(thresh=1, subset=['storeProvince']) \
    .filter('storeProvince!="null"') \
    .filter('receivable<10000') \
    .select('storeProvince','storeID','receivable','dateTS','payType')

# 复用 DataFrame 持久化
table_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# 复杂 SQL：子查询 + JOIN + 聚合
spark.sql("""
    SELECT COUNT(DISTINCT storeID) AS qualified_shop_num, storeProvince
    FROM (
        SELECT storeID, SUM(receivable) AS day_total, storeProvince
        FROM table
        GROUP BY from_unixtime(dateTS/1000,'yyyy-MM-dd'), storeID, storeProvince
        HAVING SUM(receivable) > 1000
    ) t GROUP BY storeProvince
""")

table_df.unpersist()
```

对应示例：[main.py](file:///d:/Downloads/hadoop/spark/data/02-SQL/main.py)

---

## 速查表

| 主题 | 关键 API |
|------|----------|
| 创建会话 | `SparkSession.builder.appName().master().getOrCreate()` |
| RDD→DF | `spark.createDataFrame(rdd, schema=)` / `rdd.toDF(schema=)` |
| 读数据 | `spark.read.format().option().schema().load()` |
| 写数据 | `df.write.mode().format().save()` / `.jdbc()` / `.saveAsTable()` |
| 临时视图 | `createTempView` / `createOrReplaceTempView` / `createGlobalTempView` |
| SQL 查询 | `spark.sql("...")` |
| DSL 查询 | `select / where / filter / groupBy / sort / agg` |
| 函数库 | `pyspark.sql.functions as F` |
| 自定义函数 | `spark.udf.register()` / `F.udf()` |
| 窗口函数 | `OVER(PARTITION BY ... ORDER BY ...)` |
| 去重 | `dropDuplicates()` / `distinct()` |
| 缺失值 | `dropna()` / `fillna()` |
| 持久化 | `persist(StorageLevel)` / `cache()` / `unpersist()` |
| Hive 支持 | `.enableHiveSupport()` |
