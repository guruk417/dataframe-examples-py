from pyspark.sql import SparkSession
import os.path
import yaml

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('SparkSQL') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../"+"application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    dep_delays_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/departuredelays.csv"
    #hi
    dep_delays_df = spark.read.csv(dep_delays_path)
    dep_delays_df.createOrReplaceTempView('departure')
    spark.sql('''
        select * from departure
    ''').show(10)
    spark.stop()
