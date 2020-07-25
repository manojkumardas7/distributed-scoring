'''
# ================================================================================#
#-- Authors: Manoj Kumar Das(manojkumardas7@gmail.com), Akshit Gattani(gattani.akshit@gmail.com)
#-- Date: July 11, 2020
#-- Description: Main file to the distributed-scoring utility
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
import ast
import glob
import argparse
import configparser
from pyspark.sql import sparkSession
from bricks.sparkBrick import getSparkFrameFromCSV, mojoModelScoring, pmmlModelScoring, pojoModelScoring, pickleModelScoring

######### Operational Functions
myTitle = lambda x: "my" + x[0].title() + x[1:]

def checkAndTerminate(checkValue, message, sparkSession=None):
    """
    This function takes in a boolean value, checkValue and a string, terminateMessage. 
    It returns doing nothing if checkValue is not False/None, else
    it prints out terminateMessage and exits from the python execution/terminal.

    Syntax:
        checkAndTerminate(checkValue, terminateMessage)

    Args:
        checkValue (bool)      : Boolean value to determine if exit from execution/terminal of python should be done
        terminateMessage (str) : message to be displayed if execution/terminal of python is goint to happen
    """
    if checkValue:
        return
    else:
        print(message)
        None if sparkSession is None else sparkSession.stop()
        quit()

def createGlobalObject(objectName, objectValue):
    """
    This function is used to create a global object give you have the variable name, 'objectName' for the object 
    as a string and the value, objectValue whihc could of be any data or object type

    Syntax: 
        checkAndTerminate(checkValue, terminateMessage)
        
    Args:
        objectName (str)  : Name of the object as string
        objectValue (any) : data or object of any type the object should have 
    """
    globals()[objectName] = objectValue

def modelFinder(modelPath, supportedFormats):
    """
    This function will return the type of model file present in the model folder

    Syntax:
        status, modelFileFormat = modelFinder()

    Args:
        None

    Returns:
        status (bool)         : True/False based on availability of model file
        message (str)         : message explaining the status of function execution 
        modelFile (str)       : name of model file found in the directory
    """
    if not os.pathisdir(modelPath):
        message = "Error! Specified mdoel path does not exist: " + modelPath
        print("\n" + message + "\n")
        return False, message, None, None
    modelFile = list(filter(lambda x: x.split('.')[-1] in supportedFormats, os.listdir(modelPath)))
    if modelFile:
        if len(modelFile) == 1:
            message = "Model file found!: " + modelFile[0]
            print("\n" + message + "\n")
            return True, message, modelFile[0].split(".")[-1], None
        else:
            message = "Error! 0 or more than 1 supported model files present in " + modelPath + ": "  + " ,".join(modelFile)
            print("\n" + message + "\n")
            return False, message, None, None
    else :
        message =  "Error! No supported model file formats found in " + modelPath
        print("\n" + message + "\n")
        return False, message, None, None

def listParser(x):
    if x.isdigit():
        return int(x) 
    try:
        return float(x)
    except: 
        return (lambda x: bool(1) if x == "True" else bool(0) if x == "False" else None if x == "None" else x)(x.strip())

def dataParse(x, y, z):
    try:
        return z(x)
    except:
        checkAndTerminate(False, 'Argument ' + y + ' placed incorrectly  in inConfig file')

generateList = lambda x, y: (list(map(listParser, x[1:-1].split(","))) if x[0]+x[-1] == '[]' else \
                    checkAndTerminate(False, "Argument "+ y + \
                        " not correctly placed, need to be enclosed with bar brackets")) \
                    if x is not None else None

def tryExcept(x, y): 
    try: 
        return x() 
    except: 
        return y()

if __name__ == "__main__":
    # Argument parsing using config file if not specified and final check of arguments
    inConfigFileName = "test_config.cfg"
    #change the below dictionary accordingly, update the config file accordingly
    inArgsDiction = \
        {
            "mainAttributes": [["appName", "str"], ["datasetName", "str"]],
            "scoringAttributes": [["columnSelection", "list"], ["columnOut", "list"]]
        }
    # Main Arguments handling
    try:
        inAbsoluteCodePath = os.path.split(os.path.realpath(__file__))[0]
    except Exception as e:
        inAbsoluteCodePath = os.path.realpath(".")
    inConfigFile = os.path.join(inAbsoluteCodePath, inConfigFileName)
    dataTypeDecode = {"str": "str", "int": "int", "bool": "bool", "list": "str", "float": "float"}
    inParser = argparse.ArgumentParser()
    list(map(lambda x: list(
        map(lambda y: eval("inParser.add_argument('--" + y[0] + "', type=" + dataTypeDecode[y[1]] + \
                                                                        ", help='specify " + y[0] + "')"),
            inArgsDiction[x])), inArgsDiction.keys())) is None
    inParseArgs = inParser.parse_args()
    if os.path.isfile(inConfigFile):
        inConfig = configparser.ConfigParser()
        inConfig.read(inConfigFile)
        inParameterTypeParsersConf = {"str": lambda x, y: (lambda x: None if x == '' else x)(inConfig.get(x, y).strip()),
                                      "bool": lambda x, y: (lambda x, y: bool(1) if x == "True" else \
                                                      bool(0) if x == "False" else None if x == '' else \
                                                      checkAndTerminate(False, "Argument " + y + \
                                                          " not correctly placed, \
                                                           can be left blank or acceptable case-sensitive values are \
                                                           'True' and 'False'"))(inConfig.get(x, y).strip(), y),
                                      "int": lambda x, y: (lambda x, y: None if x == '' else \
                                                      dataParse(x, y, int))(inConfig.get(x, y).strip(), y),
                                      "float": lambda x, y: (lambda x, y: None if x == '' else \
                                                      dataParse(x, y, float))(inConfig.get(x, y).strip(), y),
                                      "list": lambda x, y: tryExcept(lambda: generateList((lambda x: None if x == '' else x) \
                                                            (inConfig.get(x, y).strip()), y),
                                                            lambda: checkAndTerminate(False, 
                                                                "Argument "+ y + " not correctly placed in config file"))}
        inParameterTypeParsersArg = {"str": lambda x, y: x,
                                     "bool": lambda x, y: x,
                                     "int": lambda x, y: x,
                                     "float": lambda x, y: x,
                                     "list": lambda x, y: tryExcept(lambda: generateList(x.strip(), y), 
                                                                   lambda: checkAndTerminate(False, 
                                                                    "Argument "+ y + " not correctly parsed"))}
        inGetConfig = lambda p, q, r: (lambda x, y: y if x is None else inParameterTypeParsersArg[p](x, r))(eval("inParseArgs." + r),
                                                                         inParameterTypeParsersConf[p](q, r))
        list(map(lambda x: list(map(lambda y: createGlobalObject(myTitle(y[0]), inGetConfig(y[1], x, y[0])), inArgsDiction[x])),
                inArgsDiction.keys())) is None
    else:
        print("Warning: Config file not present")
        inGetConfig = lambda p, q, r: eval("inParseArgs." + r)
        list(map(lambda x: list(map(lambda y: createGlobalObject(myTitle(y[0]), inGetConfig(y[1], x, y[0])), inArgsDiction[x])),
                inArgsDiction.keys())) is None
        list(map(lambda x: list(
            map(lambda y: checkAndTerminate(eval(myTitle(y[0])) is not None, y[0] + " is not specified"), inArgsDiction[x])),
                inArgsDiction.keys())) is None
    
    inModelDiction = {'mojo': mojoModelScoring, 'pojo': pojoModelScoring, 'pickle': pickleModelScoring, 'pmml': pmmlModelScoring}

    # Looking for model file
    inStatus, inMessage, modelType, modelFile = modelFinder(os.path.join(inAbsoluteCodePath, model), inModelDiction.keys())
    checkAndTerminate(inStatus, inMessage)

    # Initialising spark    
    inSpark = SparkSession.builder.appName(myAppName).getOrCreate()
    _,_, inScoreFrame = getSparkFrameFromCSV(inSpark, os.path.join(inAbsoluteCodePath, "inputs", myDatasetName))
    
    # Calling the appropriate function to score the model according to the model file format found
    inStatus, inMessage, df = inModelDiction[modelType](inSpark, inScoreFrame, os.path.join(inAbsoluteCodePath, "model", modelFile), 
                                            myColumnSelection, myColumnOut)
    try:
        get_ipython().__class__.__name__ is None
    except Exception as e:
        inSpark.stop()