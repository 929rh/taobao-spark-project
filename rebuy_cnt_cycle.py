from pyspark.sql import SparkSession

if __name__ == "__main__":
    # 接收SparkSession实例
    spark = SparkSession.builder. \
        appName("rebuy_cnt_cycle_app").\
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
with buy_record as (
    select user_id,item_id,time as buy_time
    from user_action
    where behavior_type=4
),
buy_with_last as (
     select user_id,item_id,buy_time,
     lag(buy_time,1) over (partition by user_id,item_id order by buy_time) as last_buy_time
     from buy_record
),
item_rep_days AS (
    SELECT
        item_id,
        DATEDIFF(buy_time, last_buy_time) AS repurchase_days
    FROM buy_with_last
    WHERE last_buy_time IS NOT NULL
)
select item_id,round(avg(repurchase_days),2) avg_rebuy_cycle,count(*) rebuy_cnt 
from item_rep_days 
group by item_id order by avg_rebuy_cycle;
"""
rebuy_cnt_cycle=spark.sql(sql)

rebuy_cnt_cycle.write \
.mode("overwrite") \
.option("header", "true") \
.option("sep", ",") \
.csv("/tmp/rebuy_cnt_cylce")
spark.stop()