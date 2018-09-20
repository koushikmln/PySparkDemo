
# Run Command
# Requirements: Spark 2.3.1 Python 2.7
spark-submit --master yarn --deploy-mode client --conf spark.ui.port=12657 PySparkDemo/TopNProductsPerDay.py prod 5
