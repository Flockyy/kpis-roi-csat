# https://www.ge.com/digital/documentation/opm/IZjM5ZGU4MGQtOGQwZi00YjQyLWIwM2YtNGIzMzQ3MTBlNGRk.html

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.dataframe import *
from pyspark.sql.functions import *
import sys
import time
from datetime import datetime

class SimpleJob():

    def run_job(self, spark_session, runtime_config, job_json, context_dict, logger):
        try:
            spark = spark_session

            logger.info("Starting analytic...")
            configContext = context_dict["configDS"]
            tsContext = context_dict["timeseriesReadDS"]

            configDF = configContext.sql("select * from " + context_dict["configDS"].table_name)
            configDF.createOrReplaceTempView("configDF")
            inputTag = configContext.sql("select MappingValue from configDF where MappingType='InputMappings'").head()[0]
            timeseriesDF = tsContext.sql("select * from " + context_dict["timeseriesReadDS"].table_name + " where tag='" + inputTag + "'")
            timeseriesDF.createOrReplaceTempView("timeseriesDF")

            outputTag = configContext.sql("select MappingValue from configDF where MappingType='OutputMappings'").head()[0]
            resultDF = tsContext.sql("select '" + outputTag + "' as tag, timestamp as timestamp, 10*value as value, quality as quality from timeseriesDF")

            logger.info("Returning result...")
            result = {"timeseriesWriteDS" : resultDF}
            return result
        except Exception as e:
            logger.info("Error: " + str(e))
            exc_tb = sys.exc_info()[2]
            logger.info("Line number: " + str(exc_tb.tb_lineno))