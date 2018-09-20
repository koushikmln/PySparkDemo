from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder.master("local").appName("Demo Spark App").getOrCreate()

orders = spark.read.csv("/Users/koushik/data/retail_db/orders"). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")


ordersDF = orders. \
    withColumn('order_id', orders.order_id.cast(IntegerType())). \
    withColumn('order_customer_id', orders.order_customer_id.cast(IntegerType()))

orderItemsCSV = spark.read. \
  csv('/Users/koushik/data/retail_db/order_items'). \
  toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
       'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

orderItems = orderItemsCSV.\
    withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

# ordersSQL = spark.read.format("jdbc").option('url', 'jdbc:mysql://ms.itversity.com'). \
#     option('dbtable', 'retail_db.orders'). \
#     option('user', 'retail_user'). \
#     option('password', 'itversity'). \
#     load()
#
# ordersSQL.show()
#
# ordersJDBC = spark.read.jdbc('jdbc:mysql://ms.itversity.com', 'retail_db.orders', properties={'user': 'retail_user', 'password': 'itversity'})
#
# ordersJDBC.show()

# ordersDF.where((ordersDF.order_status == "COMPLETE").__or__(ordersDF.order_status == "PENDING")).show()

# ordersDF.where(ordersDF.order_status.isin("COMPLETE", "PENDING")).show()
#
# ordersDF.where('order_status in ("COMPLETE", "PENDING") and order_date like "2013-08%"').show()

# ordersDF.join(orderItems, ordersDF.order_id == orderItems.order_item_order_id).show()

# orders.groupBy('order_status').count().show()

# from pyspark.sql.functions import *
# orders.groupBy('order_status').agg(count('order_status').alias('order_status_count')).show()
#
# orderItems.groupby('order_item_order_id').agg(round(sum('order_item_subtotal'), 2).alias('order_revenue')).show()

from pyspark.sql.functions import sum, round
from pyspark.sql.functions import *

from pyspark.sql import Window


ordersJoin = ordersDF.join(orderItems, ordersDF.order_id == orderItems.order_item_order_id)

dailyProductRevenue = ordersJoin.groupby(ordersJoin.order_date, ordersJoin.order_item_product_id).agg(round(sum(ordersJoin.order_item_subtotal), 2).alias("daily_revenue"))

spec = Window.partitionBy("order_date").orderBy(dailyProductRevenue.daily_revenue.desc())
rankedRevenue = dailyProductRevenue.withColumn("product_rank", lag('daily_revenue').over(spec)).show()
# rankedRevenue.where(rankedRevenue.product_rank <= 3).show()