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
        subsetDf = spark.sql("select {} from df_vw".format(tupe(columnList)))

        return True, "Dataset read into spark dataframe successfully", subsetDf
    except Exception as e:
        print("Error encountered in reading csv file.\n", e)
        return False, e, None

def mojoModelScoring(spark, absoluteCodePath, mojoFile, scoringDataset, selectionColumns=None, targetColumn=None):
    """
    Performs scoring on the dataset provided against the mojo file passed to this scoring function

    Syntax:
        status, message, df = mojoModelScoring(spark, absoluteCodePath, myMojoFile, myDatasetPath)
    
    Mandatory Args:

        spark (spark context)      : spark context
        absoluteCodePath (str)     : path of the location of the main file
        mojoFile (model object)    : model object to be used for scoring
        scoringDataset (str)       : data to be scored against
    
    Optional Args:
        selectionColumns (list)    : list of indices to be selected from the provided data
        targetColumn (str)         : target column
    
    Returns: 
        status (bool)              :  
        message (str)              : 
        df (pyspark.sql.dataframe) :
    """
    try:
        # read the mojo file from the provided model object
        mojo = H2OMOJOPipelineModel.createFromMojo(os.path.join(*[absoluteCodePath, "model", mojoFile]))
        
        # If all columns are to be selected from the passed dataframe
        if selectionColumns is None:
            _,_, df = getSparkFrameFromCSV(spark, os.path.join(*[absoluteCodePath, "inputs", scoringDataset]))
        # in case certain columns are to be selected for transformation on mojo file
        else:
            _,_, df = getSparkFrameFromCSV(spark, os.path.join(*[absoluteCodePath, "inputs", scoringDataset]), selectionColumns)

        finalFrame = mojo.transform(df)

        return True, "{} dataset scored against mojo file {}".format(scoringDataset, mojoFile), finalFrame
    except Exception as e:
        print("Error occured while scoring the mojo file {} on the provided dataset{}:\n{}".format(mojoFile, scoringDataset, e))
        return False, e, None