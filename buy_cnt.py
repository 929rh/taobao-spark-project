from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 接收SparkSession实例
    spark = SparkSession.builder. \
        appName("tag_activity").  \
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

buy_cnt=spark.sql("""select item_category,count(*) total_cnt ,sum(if(behavior_type=4,1,0)) as buy_cnt  
          from user_action  
             group by item_category 
             order by (buy_cnt/total_cnt) desc """)


buy_cnt.write \
.mode("overwrite") \
.option("header", "true") \
.option("sep", ",") \
.csv("/tmp/buy_cnt")