from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 接收SparkSession实例
    spark = SparkSession.builder. \
        appName("hour-behavior-cnt"). \
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

hour_behavior_cnt = spark.sql("""
select behavior_type,hour(time) hour,count(*) behavior_cnt  from user_action group by behavior_type,hour order by hour
""")
hour_behavior_cnt.write \
.mode("overwrite") \
.option("header", "true") \
.option("sep", ",") \
.csv("/tmp/buy_cnt")
spark.stop()