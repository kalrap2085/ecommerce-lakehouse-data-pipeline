from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext


def get_sparkSession(_appName="Default") -> SparkSession:
    conf = (SparkConf()
            .setAppName(_appName)
           )
    
    spark = (SparkSession
             .builder
             .config(conf = conf)
             .enableHiveSupport()
             .getOrCreate()
            )
    
    return spark

