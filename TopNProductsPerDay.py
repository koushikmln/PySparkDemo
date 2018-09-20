import configparser as cp, sys
from pyspark.sql import SparkSession

props = cp.RawConfigParser()
props.read('resources/application.properties')
env = sys.argv[1]
topN = int(sys.argv[2])

spark = SparkSession.\
    builder.\
    appName("Daily Product Revenue using Data Frame Operations").\
    master(props.get(env, 'executionMode')).\
    getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '2')

inputBaseDir = props.get(env, 'input.base.dir')
orders = spark.read. \
  csv(inputBaseDir + '/orders', inferSchema=True). \
  toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

orderItems = spark.read. \
  csv(inputBaseDir + '/order_items', inferSchema=True). \
  toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
       'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

from pyspark.sql.types import IntegerType, FloatType

from pyspark.sql.functions import sum, round
dailyProductRevenue = orders. \
    where('order_status in ("COMPLETE", "CLOSED")'). \
    join(orderItems, orders.order_id == orderItems.order_item_order_id). \
    groupBy('order_date', 'order_item_product_id'). \
    agg(round(sum(orderItems.order_item_subtotal), 2).alias('revenue'))


from pyspark.sql.window import Window
spec = Window. \
    partitionBy('order_date'). \
    orderBy(dailyProductRevenue.revenue.desc())

from pyspark.sql.functions import dense_rank
dailyProductRevenueRanked = dailyProductRevenue. \
    withColumn('rank', dense_rank().over(spec))

topNDailyProducts = dailyProductRevenueRanked. \
    where(dailyProductRevenueRanked.rnk <= topN). \
    drop('rank'). \
    orderBy(dailyProductRevenue.order_date, dailyProductRevenue.revenue.desc())

outputBaseDir = props.get(env, 'output.base.dir')
topNDailyProducts.write.csv(outputBaseDir + '/topn_daily_products')
