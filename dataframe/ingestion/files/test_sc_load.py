from pyspark.sql import SparkSession
import os
import yaml
from pyspark.sql.functions import col, countDistinct, count, expr, avg

if __name__ != "__main__":
    # Create spark session
    spark = SparkSession.builder.appName("Pyspark Example").getOrCreate()

    # Access application yaml file for configuration and secrete file
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    sells_file = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/sellers.csv")
    prod_file = spark \
        .read \
        .option("header", "true") \
        .option("interSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/prod.csv")
    sales_file = spark \
        .read \
        .option("header", "true") \
        .option("interSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/sales.csv")

    print("Num Of orders {}".format(sales_file.count()))
    print("Num of products {}").format(prod_file.count())
    print("Num Of sells {}".format(sells_file.count()))

    print("Num of Product Sold atleast {}".format(sales_file.agg(countDistinct(col("product_id")))))

    sales_file.groupBy(col("product_id")).agg(
        count("*").alias("cnt")).orderBy(col("cnt").desc()).limit(1).show()

    sales_file.groupBy(col("date").agg(
        countDistinct(col("product_id")).alias("num_product"))).orderBy(col("num_product").desc()).show()

    sales_file \
        .join(prod_file, sales_file.product_id == prod_file.product_id, "inner") \
        .withColumn("Avg_Rev", expr(avg(sales_file.num_pieces_sold * prod_file.price))).show()

    spark.stop()
