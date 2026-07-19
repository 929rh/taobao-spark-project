from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 接收SparkSession实例
    spark = SparkSession.builder. \
        master('local[*]'). appName("buy_user_cnt").\
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

sql="""
select user_id,item_id,time from user_action where item_id = 303205878 and behavior_type = 4
    order by user_id,time
"""
spark.sql(sql).show()
spark.stop()