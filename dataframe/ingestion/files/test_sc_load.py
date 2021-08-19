from pyspark.sql import SparkSession
import os.path
import yaml
from pyspark.sql.types import StructType, IntegerType, StringType

if __name__ == '__main__':
    # Start Spark Session
    spark = SparkSession \
        .builder \
        .appName('test sc file load') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('Error')
    # Define current path of application config path and secrets path
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + '/../../../' + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + '/../../../' + "secrets")

    # open and load application config and secret files
    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Set up to use S3
    hadoop_conf = spark.SparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    sc_schema = StructType() \
        .add('SCA-POSTAL-ID', IntegerType(), True) \
        .add('SCA-STATE-NAME', StringType(), True) \
        .add('SCA-COUNTY-CODE', StringType(), True) \
        .add('SCA-COUNTY-NAME', StringType(), True) \
        .add('SCA-STATE-CODE', StringType(), True)

    sc_read = spark \
        .read \
        .option('header','false') \
        .schema(sc_schema) \
        .csv("s3a://" + app_conf['s3_conf']['s3_bucket'] + 'SC_DB')

    sc_read.getNumPartition()
    # Stop Spark Session
    spark.stop()
