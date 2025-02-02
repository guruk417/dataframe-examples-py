from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import countDistinct, col, count, expr, avg
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('Test File Load') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    sells_file = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("csv") \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/sellers.csv")

    prod_file = spark \
        .read \
        .option("header", "true") \
        .option("interSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/products.csv")
    sales_file = spark \
        .read \
        .option("header", "true") \
        .option("interSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/sales.csv")

    print("Num Of sells {}".format(sells_file.count()))

    print("Num Of orders {}".format(sales_file.count()))

    print("Num of products {}".format(prod_file.count()))
    print("Num of Product Sold atleast {}".format(sales_file.agg(countDistinct(col("product_id")))))

    sales_file.groupBy(col("product_id")).agg(
        count("*").alias("cnt")).orderBy(col("cnt").desc()).limit(1).show()

    sales_file.groupBy(col("date")).agg(
        countDistinct(col("product_id")).alias("num_product")).orderBy(col("num_product").desc()).show()

    sales_file \
        .join(prod_file, sales_file.product_id == prod_file.product_id, "inner")\
        .agg(avg(sales_file.num_pieces_sold * prod_file.price).alias("avg_sales")) \
        .show()

    spark.stop()

'''
    fin_schema = StructType() \
        .add('id',  IntegerType(),  True) \
        .add("has_debt", BooleanType(), True) \
        .add("has_financial_dependents", BooleanType(), True) \
        .add("has_student_loans", BooleanType(), True) \
        .add("income", DoubleType(), True)

    fin_df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(fin_schema) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")

    fin_df.printSchema()
    fin_df.show()

    tot_debt = fin_df \
        .groupBy("has_debt") \
        .sum("income")

    tot_debt = tot_debt \
        .withColumnRenamed("sum(income)", "total income")
    tot_debt.show()
    spark.stop()

    tot_debt = tot_debt \
        .filter("has_debt" == "true" and "has_student_loans" == "True")'''

# spark-submit --master yarn --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/files/test_file_load