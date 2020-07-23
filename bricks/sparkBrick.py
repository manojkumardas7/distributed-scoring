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

def getSparkFrameFromCSV(spark, datasetPath, columnIndexList=None):
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
        
        # If all columns are to be selected, return
        if columnIndexList is None:
            return True, "Dataset read into spark dataframe successfully", df    
        
        # In case column index list is provided in config
        # Dictionary to map index to column names
        indexToColumnMap = dict(zip(list(range(0, len(df.columns))), df.columns))
        columnList = []

        # Adding mapped column names to those indices
        for i, j in indexToColumnMap.items(): 
            if i in columnIndexList: 
                columnList.append(j)
        df.createOrReplaceTempView("df_vw")
        
        # query to select just those columns
        subsetDf = spark.sql("select {} from df_vw".format(','.join(columnList)))

        return True, "Dataset read into spark dataframe successfully", subsetDf
    except Exception as e:
        print("Error encountered in reading csv file.\n", e)
        return False, e, None

def pickleModelScoring(spark, scoreFrame, pojoFile):
    """
    """
    pass

def pmmlModelScoring(spark, scoreFrame, pmmlFile):
    """
    """
    pass

def pojoModelScoring(spark, scoreFrame, pojoFile):
    """
    """
    pass

def mojoModelScoring(spark, scoreFrame, mojoFile):
    """
    Performs scoring on the dataset provided against the mojo file passed to this scoring function

    Syntax:
        status, message, df = mojoModelScoring(spark, scoreFrame, mojoFile)
    
    Args:
        spark (spark context)              : spark context
        mojoFile (model object)            : model object to be used for scoring
        scoreFrame (pyspark.sql.dataframe) : dataframe to be scored against
    
    Returns: 
        status (bool)                      :
        message (str)                      :
        df (pyspark.sql.dataframe)         :
    """
    try:
        # read the mojo file from the provided model object
        mojo = H2OMOJOPipelineModel.createFromMojo(mojoFile)
        
        finalFrame = mojo.transform(scoreFrame)

        return True, "dataset scored against mojo file", finalFrame
    except Exception as e:
        print("Error occured while scoring the mojo file {} on the provided dataset:\n{}".format(mojoFile, e))
        return False, e, None