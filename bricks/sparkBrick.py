'''
# ================================================================================#
#-- Authors: Manoj Kumar Das(manojkumardas7@gmail.com), Akshit Gattani(gattani.akshit@gmail.com)
#-- Date: July 11, 2020
#-- Description: collection of spark specific functions
#-- Version : 3.0
#-- Revisions: None
#-- Required Tools:
#       python 3.6/3.7/3.8
#       os
#       ast
#       argparse
#       configparser
# ================================================================================#
'''
import os
from pyspark.sql.types import BooleanType, IntegerType
# from pyspark.sql.functions import col
from pysparkling.ml import H2OMOJOPipelineModel

# labelGenerator = udf(lambda x:  x > 0.5, BooleanType())
# mojo = H2OMOJOPipelineModel.createFromMojo(os.path.join(*[absoluteCodePath, "model", "pipeline.mojo"]))
# _, df = getSparkFrameFromCSV(spark, os.path.join(*[absoluteCodePath, "model", "TestSet.csv"]))
# final = mojo.transform(df).select(df.columns + [col("prediction.*")]).select(df.columns + [col("`Label.b`").\
#     alias("pb"), col("`Label.s`").alias("ps")]).withColumn("prediction", labelGenerator("ps").cast(IntegerType()))

def getSparkFrameFromCSV(spark, datasetPath):
    """
    This function returns a spark dataframe from the provided csv file path
    Syntax:
        status, message, df = getSparkFrameFromCSV(spark, "/path/to/data/dataset.csv")
    Args:
        spark (spark context)      : spark context
        datasetPath (str)          : path of csv file to be converted to spark dataframe
    Returns:
        status (bool)              : True or False depending on state of data read  
        message (str)              : Log message according to action performed
        df (pyspark.sql.dataframe) : spark dataframe read from csv
    """
    try:
        df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(datasetPath)
        return True, "Dataset read into spark dataframe successfully", df
    except Exception as e:
        print("Error encountered in reading csv file.\n", e)
        return False, e, None

def mojoModelScoring(spark, absoluteCodePath, mojoFile, scoringDataset, selectionColumns=None, targetColumn=None):
    mojo = H2OMOJOPipelineModel.createFromMojo(os.path.join(*[absoluteCodePath, "model", mojoFile]))
    _,_, df = getSparkFrameFromCSV(spark, os.path.join(*[absoluteCodePath, "inputs", scoringDataset]))
    finalFrame = mojo.transform(df)
    return finalFrame