from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 接收SparkSession实例
    spark = SparkSession.builder. \
        appName("tag_activity").  \
        config("spark.sql.shuffle.partitions", "10"). \
        getOrCreate()
    # config("spark.sql.shuffle.partitions", "2") shuffle算子分区数
    sc = spark.sparkContext

df = spark.read.format("csv") \
    .option("sep", ",") \
    .option("header", True) \
    .option("encoding", "utf-8") \
    .schema("user_id int ,item_id int,behavior_type int,item_category int,time timestamp") \
    .load("/tmp/pycharm_project_28777d0b/user_action.csv")

df.createOrReplaceTempView("user_action")

tag_activity= spark.sql("""SELECT
    tag,
    user_cnt AS activity_num,
    COUNT(user_id) AS user_num
FROM (
    SELECT
        user_id,
        COUNT(*) AS user_cnt,
        CASE WHEN date_format(MIN(time), 'yyyy-MM') < '2014-12' THEN 'old' ELSE 'new' END AS tag
    FROM user_action
    GROUP BY user_id
) t
GROUP BY tag, user_cnt
ORDER BY tag, user_cnt""")

tag_activity.write \
.mode("overwrite") \
.option("header", "true") \
.option("sep", ",") \
.csv("/tmp/user_stat_result")

spark.stop()

