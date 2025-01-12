import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utilities.logging_config import *

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("carCrashAnalysisApp")\
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark