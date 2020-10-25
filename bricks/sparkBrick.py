'''
# ==============================================================================================================================================#
#-- Authors: Manoj Kumar Das(manojkumardas7@gmail.com), Akshit Gattani(gattani.akshit@gmail.com), Prejith Premkumar(prajithpremg@gmail.com)
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
# ==============================================================================================================================================#
'''

# Library imports
import os
from functools import reduce
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType, IntegerType

def getSparkFrameFromCSV(sparkSession, localFileSystemPath, selectionColumns=None):
    """
    This function returns a sparkSession dataframe from the provided csv file path
    
    Syntax:
        status, message, df = getSparkFrameFromCSV(sparkSession, "/path/to/data/dataset.csv")
    
    Args:
        sparkSession (sparkSession context): sparkSession context
        localFileSystemPath (str)          : path of csv file to be converted to sparkSession dataframe
        selectionColumns (list)            : list of column nemes that should be only present in the return dataframe (default value: None)
    
    Returns:
        status (bool)              : True or False depending on state of data read  
        message (str)              : Log message according to action performed
        df (pyspark.sql.dataframe) : sparkSession dataframe read from csv
    """
    try:
        df = sparkSession.read.format("csv").option("header", "true").option("inferschema", "true").load("file://" + localFileSystemPath)
        
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
        status (bool)                      : True/False based on execution of the function
        message (str)                      : message from execution of the function
        df (pyspark.sql.dataframe)         : scored data as a pyspark dataframe
    """
    from pypmml_spark import ScoreModel
    
    try:
        # read the mojo file from the provided model object
        pmml = ScoreModel.fromFile(pmmlFile)
        if selectionColumns:
            transformFrame = pmml.transform(scoreFrame.select(*selectionColumns))
        else:
            transformFrame = pmml.transform(scoreFrame)
        if outColumns:
            colsToBeRenamed = (lambda y: list(filter(lambda x: x not in y, transformFrame.columns)))(scoreFrame.columns)
            finalFrame = eval("transformFrame" + reduce(lambda x, y: x + ".withColumnRenamed('" + y[0] + "','" + y[1] + "')",
                                                     [''] + list(zip(colsToBeRenamed, outColumns))))
            return True, "dataset scored against pmml file", finalFrame
        else:
            return True, "dataset scored against pmml file", transformFrame
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
        selectionColumns (list)            : list of column names that should be considered to model, when set to None, all columns are
                                                considered (Default: None)    
    Returns: 
        status (bool)                      : True/False based on execution of the function
        message (str)                      : message from execution of the function
        df (pyspark.sql.dataframe)         : scored data as a pyspark dataframe
    """
    if clusterResource:
        sparkSession.sparkContext.addPyFile(clusterResource)
    from pysparkling.ml import H2OMOJOPipelineModel

    try:
        # read the mojo file from the provided model object
        mojo = H2OMOJOPipelineModel.createFromMojo("file://" + mojoFile)
        if selectionColumns:
            transformFrame = mojo.transform(scoreFrame.select(*selectionColumns))
        else:
            transformFrame = mojo.transform(scoreFrame)
        if outColumns:
            colsToBeRenamed = (lambda y: list(filter(lambda x: x not in y, transformFrame.columns)))(scoreFrame.columns)
            finalFrame = eval("transformFrame" + reduce(lambda x, y: x + ".withColumnRenamed('" + y[0] + "','" + y[1] + "')",
                                                     [''] + list(zip(colsToBeRenamed, outColumns))))
            return True, "dataset scored against mojo file", finalFrame
        else:
            return True, "dataset scored against mojo file", transformFrame
    except Exception as e:
        print("Error occured while scoring the mojo file {} on the provided dataset:\n{}".format(mojoFile, e))
        return False, e, None
