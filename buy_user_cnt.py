from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 接收SparkSession实例
    spark = SparkSession.builder. \
        master("local[*]"). appName("buy_user_cnt_app").\
        config("spark.sql.shuffle.partitions", "3"). \
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

buy_user_cnt = spark.sql("""
SELECT
    DATE(time) AS day_dt,
    -- 第1个指标：当日购买总订单数 behavior_type=4
    SUM(IF(behavior_type = 4, 1, 0)) AS buy_total_cnt,
    -- 第2个指标：当日全行为活跃用户DAU
    COUNT(DISTINCT user_id) AS user_cnt,
    -- 第3个指标：当日购买用户数（付费DAU）
    COUNT(DISTINCT IF(behavior_type = 4, user_id, NULL)) AS buy_user_cnt
FROM user_action
GROUP BY day_dt
ORDER BY day_dt;
""")
buy_user_cnt.write \
.mode("overwrite") \
.option("header", "true") \
.option("sep", ",") \
.csv("/tmp/buy_user_cnt")
spark.stop()
