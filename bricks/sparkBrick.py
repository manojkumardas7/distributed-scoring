'''
# ================================================================================#
#-- Authors: Manoj Kumar Das(manojkumardas7@gmail.com), Akshit Gattani(gattani.akshit@gmail.com)
#-- Date: July 27, 2020
#-- Description: collection of sparkSession specific functions
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

# Library imports
import os
from pyspark.sql.types import BooleanType, IntegerType
# from pyspark.sql.functions import col

# labelGenerator = udf(lambda x:  x > 0.5, BooleanType())
# mojo = H2OMOJOPipelineModel.createFromMojo(os.path.join(*[absoluteCodePath, "model", "pipeline.mojo"]))
# _, df = getSparkFrameFromCSV(sparkSession, os.path.join(*[absoluteCodePath, "model", "TestSet.csv"]))
# final = mojo.transform(df).select(df.columns + [col("prediction.*")]).select(df.columns + [col("`Label.b`").\
#     alias("pb"), col("`Label.s`").alias("ps")]).withColumn("prediction", labelGenerator("ps").cast(IntegerType()))

def getSparkFrameFromCSV(sparkSession, datasetPath, selectionColumns=None):
    """
    This function returns a sparkSession dataframe from the provided csv file path
    
    Syntax:
        status, message, df = getSparkFrameFromCSV(sparkSession, "/path/to/data/dataset.csv")
    
    Args:
        sparkSession (sparkSession context)      : sparkSession context
        datasetPath (str)          : path of csv file to be converted to sparkSession dataframe
        selectionColumns (list)    : list of column nemes that should be only present in the return dataframe (default value: None)
    
    Returns:
        status (bool)              : True or False depending on state of data read  
        message (str)              : Log message according to action performed
        df (pyspark.sql.dataframe) : sparkSession dataframe read from csv
    """
    try:
        df = sparkSession.read.format("csv").option("header", "true").option("inferschema", "true").load(datasetPath)
        
        # If all columns are to be selected, return
        if selectionColumns:
            return True, "Dataset read into sparkSession dataframe successfully", df.select(*selectionColumns)
        else:
            return True, "Dataset read into sparkSession dataframe successfully", df
    except Exception as e:
        print("Error encountered in reading csv file.\n", e)
        return False, e, None

def pickleModelScoring(sparkSession, scoreFrame, pojoFile):
    """
    """
    pass

def pmmlModelScoring(sparkSession, scoreFrame, pmmlFile, selectionColumns=None, outColumns=None):
    """
    Performs scoring on the dataset provided against the mojo file passed to this scoring function

    Syntax:
        status, message, df = mojoModelScoring(sparkSession, scoreFrame, mojoFile)
    
    Args:
        sparkSession (sparkSession context)              : sparkSession context
        pmmlFile (model object)            : model object to be used for scoring
        scoreFrame (pyspark.sql.dataframe) : dataframe to be scored against
        selectionColumns (list)            : list of column names that should be considered to model, when set to None, all columsn are
                                                considered (Default: None)    
    Returns: 
        status (bool)                      :
        message (str)                      :
        df (pyspark.sql.dataframe)         :
    """
    from pypmml_spark import ScoreModel
    
    try:
        # read the mojo file from the provided model object
        pmml = ScoreModel.fromFile(pmmlFile)
        if selectionColumns:
            finalFrame = pmml.transform(scoreFrame.select(*selectionColumns))
            colDiction = dict(zip(selectionColumns, outColumns))
            finalFrame = finalFrame.select([col(c).alias(c_new) for c, c_new in colDiction.items()])
        else:
            finalFrame = pmml.transform(scoreFrame)
            colDiction = dict(zip(finalFrame.columns, outColumns))
            finalFrame = finalFrame.select([col(c).alias(c_new) for c, c_new in colDiction.items()])            
        return True, "Dataset scored against pmml file", finalFrame
    except Exception as e:
        print("Error occured while scoring the pmml file {} on the provided dataset:\n{}".format(pmmlFile, e))
        return False, e, None

def pojoModelScoring(sparkSession, scoreFrame, pojoFile):
    """
    """
    pass

def mojoModelScoring(sparkSession, scoreFrame, mojoFile, selectionColumns=None, outColumns=None, clusterResource=None):
    """
    Performs scoring on the dataset provided against the mojo file passed to this scoring function

    Syntax:
        status, message, df = mojoModelScoring(sparkSession, scoreFrame, mojoFile)
    
    Args:
        sparkSession (sparkSession context)              : sparkSession context
        mojoFile (model object)            : model object to be used for scoring
        scoreFrame (pyspark.sql.dataframe) : dataframe to be scored against
        selectionColumns (list)            : list of column names that should be considered to model, when set to None, all columsn are
                                                considered (Default: None)    
    Returns: 
        status (bool)                      :
        message (str)                      :
        df (pyspark.sql.dataframe)         :
    """
    if clusterResource:
        sparkSession.sparkContext.addPyFile(clusterResource)
    from pysparkling.ml import H2OMOJOPipelineModel

    try:
        # read the mojo file from the provided model object
        mojo = H2OMOJOPipelineModel.createFromMojo(mojoFile)
        if selectionColumns:
            finalFrame = mojo.transform(scoreFrame.select(*selectionColumns))
            colDiction = dict(zip(selectionColumns, outColumns))
            finalFrame = finalFrame.select([col(c).alias(c_new) for c, c_new in colDiction.items()])
        else:
            finalFrame = mojo.transform(scoreFrame)
            colDiction = dict(zip(finalFrame.columns, outColumns))
            finalFrame = finalFrame.select([col(c).alias(c_new) for c, c_new in colDiction.items()])
        return True, "dataset scored against mojo file", finalFrame
    except Exception as e:
        print("Error occured while scoring the mojo file {} on the provided dataset:\n{}".format(mojoFile, e))
        return False, e, None