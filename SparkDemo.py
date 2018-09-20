from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    master('local'). \
    appName('PySpark Demo'). \
    getOrCreate()

orders = spark.read.csv("/Users/koushik/data/retail_db/orders/part-00000",
                        inferSchema=True, header=False). \
    toDF("order_id", "order_date", "order_customer_id", "order_status")

orderItems = spark.read.csv("/Users/koushik/data/retail_db/order_items/part-00000",
                        inferSchema=True, header=False). \
    toDF("order_item_id", "order_item_order_id",
         "order_item_product_id", "order_item_quantity",
         "order_item_subtotal", "order_item_price")

# orders.withColumn("order_id", orders.order_id.cast(IntegerType())). \
# #     withColumn("order_customer_id", orders.order_customer_id.cast(IntegerType())). \
# #     printSchema()

# from pyspark.sql.functions import sum, round, count
#
# orderItems.groupby('order_item_order_id'). \
#     agg(round(sum('order_item_subtotal'), 2).alias("order_item_revenue")). \
#     show()
#
# orders.join(orderItems, orders.order_id == orderItems.order_item_order_id). \
#     groupby([orders.order_date, orderItems.order_item_product_id]). \
#     agg(sum(orderItems.order_item_quantity).alias("products_per_day")). \
#     show()

orders_items = spark.read.jdbc('jdbc:mysql://ms.itversity.com',
                            'retail_db.order_items',
                            properties={'user': 'retail_user',
                                        'password': 'itversity',
                                        'driver': 'com.mysql.jdbc.Driver'})
# orders_items.show()

from pyspark.sql import Window
from pyspark.sql.functions import sum, round, rank, lead, lag
#
# spec = Window.partitionBy('order_item_order_id')
#
# orderRevenue = orderItems.withColumn('order_revenue', sum('order_item_subtotal').over(spec)). \
#     where(orderItems.order_item_order_id == 2)
#
# orderRevenue.withColumn('order_percentage',
#         round(orderRevenue.order_item_subtotal/orderRevenue.order_revenue, 4) * 100). \
#     show()
#
# spec = Window.partitionBy()
#
# orderItems.withColumn('total_revenue', sum('order_item_subtotal').over(spec)).show()

# productRevenue = orderItems.groupby('order_item_product_id'). \
#     agg(round(sum('order_item_subtotal'), 2).alias('product_revenue'))
#
# spec = Window.partitionBy()
#
# productRevenueTotal = productRevenue.withColumn('total_revenue', round(sum('product_revenue').over(spec), 2))
#
# productRevenueTotal.withColumn('product_revenue_percentage',
#         round(productRevenueTotal.product_revenue/productRevenueTotal.total_revenue, 4) * 100). \
#     orderBy(productRevenueTotal.product_revenue.desc()). \
#     show()


# Get Top N Products Per Day
orderItemsJoin = orders.filter(orders.order_status == 'COMPLETE'). \
    join(orderItems, orders.order_id == orderItems.order_item_order_id)

productRevenuePerDay = orderItemsJoin.groupby(['order_date', 'order_item_product_id']). \
    agg(sum('order_item_subtotal').alias('product_revenue'))

spec = Window.partitionBy('order_date'). \
    orderBy(productRevenuePerDay.product_revenue.desc())

productRank = productRevenuePerDay.withColumn('product_rank', rank().over(spec))

productRank.where('product_rank <= 5'). \
    withColumn('product_lead', lag('product_revenue').over(spec)). \
    show()

