from pyspark.sql import SparkSession
import yaml
import os

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, IntegerType, BooleanType, DoubleType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Test File Load") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
    # .master("local[*]") \

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

    fin_format = StructType() \
        .add("id", IntegerType(), True) \
        .add("has_debt", BooleanType(), True) \
        .add("has_financial_dependents", BooleanType(), True) \
        .add("has_student_loan", BooleanType(), True) \
        .add("income", DoubleType(), True)

    fin_df = spark \
        .read \
        .option("header", False) \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(fin_format) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")

    fin_df.show(10)

    fin_df.filter(col("has_debt") == True)

    fin_df.repartition(2) \
        .write \
        .option("header", True) \
        .option("delimiter", "~") \
        .mode("overwrite") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/fin")
    #    .partitionBy("id")\
    spark.stop()
