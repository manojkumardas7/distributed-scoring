'''
# ==============================================================================================================================================#
#-- Authors: Manoj Kumar Das(manojkumardas7@gmail.com), Akshit Gattani(gattani.akshit@gmail.com), Prejith Premkumar(prajithpremg@gmail.com)
#-- Date: July 27, 2020
#-- Description: Main file to the distributed-scoring utility
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
import re
import os
import ast
import glob
import argparse
import configparser
import pandas as pd
from functools import reduce
from pyspark.sql import SparkSession
from bricks.utils import modelFileFinder

########## Operational Functions
myTitle = lambda x: "my" + x[0].title() + x[1:]

def checkAndTerminate(checkValue, message, logger=None, sparkSession=None):
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
    logger.warn(message) if logger else None
    if checkValue:
        return
    print(message)
    try:
        get_ipython().__class__.__name__ is None
    except Exception as e:
        if sparkSession:
            sparkSession.stop()
            print('spark session terminated')
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
    try:
        inAbsoluteCodePath = os.path.split(os.path
            .realpath(__file__))[0]
    except Exception as e:
        inAbsoluteCodePath = os.path.realpath(".")

    inConfigFileName = "config.cfg"
    inPipelineArgsFileName = "pipelineArguments.csv"
    inQueryFileName = "fr_query.csv"
    inConfigFile = os.path.join(inAbsoluteCodePath, inConfigFileName)
    inPipelineArgsFile = os.path.join(inAbsoluteCodePath, inPipelineArgsFileName)
    inQueryFile = os.path.join(inAbsoluteCodePath, inQueryFileName)
    inRegulerExpression = r"\b[a-z][a-z-_0-9]+\b"

    #change the below dictionary accordingly, update the config file accordingly
    inArgsDiction = \
        {
            "mainAttributes": [["appName", "str"]],
            "scoringAttributes": [["columnSelection", "list"], ["columnOut", "list"]],
            "modelOutput": [["hiveTable", "str"]]
        }
    # inPipelineArgs = set(map(lambda x: x.strip().lower(), open(inPipelineArgsFile).readlines()))
    inPipelineArgs = pd.read_csv(inPipelineArgsFile, header=None, names=["arg", "value"]).iloc[:, :2]
    inPipelineArgs["arg"] = inPipelineArgs["arg"].apply(lambda x: x.strip().lower())
    inPipelineArgs = inPipelineArgs.set_index("arg")
    checkAndTerminate(len(inPipelineArgs.index) == len(set(inPipelineArgs.index)), 
                inPipelineArgsFile + " contains duplicates: " + \
                ", ".join((lambda y: set([x for x in y if y.count(x) > 1]))(list(inPipelineArgs.index))))
    inQueryFrame = pd.read_csv(inQueryFile, header=None).iloc[:, 0]
    if set(inQueryFrame.apply(lambda x:  re.findall(inRegulerExpression, x)).sum()) - set(inPipelineArgs.index):
        checkAndTerminate(False, "\n" + \
            ", ".join(set(inQueryFrame.apply(lambda x:  re.findall(inRegulerExpression, x)).sum()) - \
            set(inPipelineArgs.index)) + " are not present in the pipeline arguments file: " + inPipelineArgsFile + "\n")
    if set(inPipelineArgs.index) - set(inQueryFrame.apply(lambda x:  re.findall(inRegulerExpression, x)).sum()):
        print("Warning: " + \
            ", ".join(set(inPipelineArgs.index) - set(inQueryFrame.apply(lambda x:  re.findall(inRegulerExpression, x)).sum())) + \
            " are not used in query file: " + inQueryFileName)
    # Main Arguments handling
    dataTypeDecode = {"str": "str", "int": "int", "bool": "bool", "list": "str", "float": "float"}
    inParser = argparse.ArgumentParser()
    list(map(lambda y: eval("inParser.add_argument('--" + y + "', type=str" + \
                                                                        ", required=True, help='specify " + y + "')"),
            inPipelineArgs[inPipelineArgs.value.isnull()].index)) is None
    list(map(lambda y: eval("inParser.add_argument('--" + y + "', type=str" + \
                                                                        ", help='specify " + y + "')"),
            inPipelineArgs[inPipelineArgs.value.notnull()].index)) is None
    list(map(lambda x: list(
        map(lambda y: eval("inParser.add_argument('--" + y[0] + "', type=" + dataTypeDecode[y[1]] + \
                                                                        ", help='specify " + y[0] + "')"),
            inArgsDiction[x])), inArgsDiction.keys())) is None
    inParseArgs = inParser.parse_args()
    inParameterTypeParsersArg = {"str"  : lambda x, y: x,
                                 "bool" : lambda x, y: x,
                                 "int"  : lambda x, y: x,
                                 "float": lambda x, y: x,
                                 "list" : lambda x, y: tryExcept(lambda: generateList(x.strip(), y), 
                                                           lambda: checkAndTerminate(False, 
                                                            "Argument "+ y + " not correctly parsed"))}
    inGetConfig = lambda p, r: (lambda x: None if x is None else inParameterTypeParsersArg[p](x, r))(eval("inParseArgs." + r))
    list(map(lambda y: createGlobalObject(y, inGetConfig("str", y)), inPipelineArgs[inPipelineArgs.value.isnull()].index)) is None
    list(map(lambda y: createGlobalObject(y, (lambda m, n: m if n is None else n)(str(inPipelineArgs.loc[y]["value"]), inGetConfig("str", y))), 
        inPipelineArgs[inPipelineArgs.value.notnull()].index)) is None
    if not os.path.isfile(inConfigFile):
        print("Warning: Config file not present")
        list(map(lambda x: list(map(lambda y: createGlobalObject(myTitle(y[0]), inGetConfig(y[1], y[0])), inArgsDiction[x])),
                inArgsDiction.keys())) is None
        list(map(lambda x: list(
            map(lambda y: checkAndTerminate(eval(myTitle(y[0])) is not None, y[0] + " is not specified"), inArgsDiction[x])),
                inArgsDiction.keys())) is None
    else:
        inConfig = configparser.ConfigParser()
        inConfig.read(inConfigFile)
        inParameterTypeParsersConf = {"str"  : lambda x, y: (lambda x: None if x == '' else x)(inConfig.get(x, y).strip()),
                                      "bool" : lambda x, y: (lambda x, y: bool(1) if x == "True" else \
                                                      bool(0) if x == "False" else None if x == '' else \
                                                      checkAndTerminate(False, "Argument " + y + \
                                                          " not correctly placed, \
                                                           can be left blank or acceptable case-sensitive values are \
                                                           'True' and 'False'"))(inConfig.get(x, y).strip(), y),
                                      "int"  : lambda x, y: (lambda x, y: None if x == '' else \
                                                      dataParse(x, y, int))(inConfig.get(x, y).strip(), y),
                                      "float": lambda x, y: (lambda x, y: None if x == '' else \
                                                      dataParse(x, y, float))(inConfig.get(x, y).strip(), y),
                                      "list" : lambda x, y: tryExcept(lambda: generateList((lambda x: None if x == '' else x) \
                                                            (inConfig.get(x, y).strip()), y),
                                                            lambda: checkAndTerminate(False, 
                                                                "Argument "+ y + " not correctly placed in config file"))}
        inGetConfig = lambda p, q, r: (lambda x, y: y if x is None else inParameterTypeParsersArg[p](x, r))(eval("inParseArgs." + r),
                                                                         inParameterTypeParsersConf[p](q, r))
        list(map(lambda x: list(map(lambda y: createGlobalObject(myTitle(y[0]), inGetConfig(y[1], x, y[0])), inArgsDiction[x])),
                inArgsDiction.keys())) is None

    # Looking for model file
    inModelDiction = {'mojo': 'mojoModelScoring', 'pmml': 'pmmlModelScoring'}
    inStatus, inMessage, modelType, modelFile = modelFileFinder(os.path.join(inAbsoluteCodePath, "model"), inModelDiction.keys())
    checkAndTerminate(inStatus, inMessage)
    inQueryFrame = inQueryFrame.apply(lambda n: reduce(lambda x, y: x.replace(y[0], y[1]), [n] + \
                    list(map(lambda x: (x, eval(x)), inPipelineArgs.index))))
    
    # Initialising spark    
    inSpark = SparkSession.builder.appName(myAppName).getOrCreate()
    inLog4jLogger = inSpark.sparkContext._jvm.org.apache.log4j
    inLogger = inLog4jLogger.LogManager.getLogger('mylogger')
    inLogger.setLevel(inLog4jLogger.Level.WARN)
    inLogger.warn("spark session initialized")

    inClusterResource = (lambda x: x[0] if x else None)(glob.glob(os.path.join(inAbsoluteCodePath, "codeZips", "*")))

    # from bricks.sparkBrick import getSparkFrameFromCSV, mojoModelScoring, pmmlModelScoring, pojoModelScoring, pickleModelScoring
    from bricks.sparkBrick import mojoModelScoring, pmmlModelScoring
    inModelDiction = dict(zip(inModelDiction.keys(), list(map(lambda x: eval(inModelDiction[x]), inModelDiction.keys()))))

    inLogger.warn("ETL initialized")
    try:
        if inQueryFrame.shape[0] > 1:
            # Running Data preperation queries to get feature data
            for query in inQueryFrame.iloc[:-1]:
                print(query)
                inSpark.sql(query)
        print(inQueryFrame.iloc[-1])
        inScoreFrame = inSpark.sql(inQueryFrame.iloc[-1])
        inStatus, inMessage = (False, "ETL completed but returned no data") if inScoreFrame.rdd.isEmpty() else (True, "ETL completed")
    except Exception as e:
        inStatus, inMessage = (False, "ETL failed:\n{}".format(e))
    checkAndTerminate(inStatus, inMessage, inLogger, inSpark)
    
    # Calling the appropriate function to score the model according to the model file format found
    inStatus, inMessage, inOutputFrame = inModelDiction[modelType](inSpark, inScoreFrame, os.path.join(inAbsoluteCodePath, "model", modelFile), 
                                            myColumnSelection, myColumnOut, inClusterResource)
    checkAndTerminate(inStatus, inMessage, inLogger, inSpark)
    inStatus, inMessage = (True, 'intializing write to model output table') if myHiveTable else (False, 'no hive table specified for writing model output')
    checkAndTerminate(inStatus, inMessage, inLogger, inSpark)
    try:
        inOutputFrame.write.mode('append').saveAsTable(myHiveTable)
        inStatus, inMessage = (True, "model output written to hive table: {}".format(myHiveTable))
    except Exception as e:
        inStatus, inMessage = (True, "model output failed to write to hive table: {}\n{}".format(myHiveTable, e))
    checkAndTerminate(inStatus, inMessage, inLogger, inSpark)
    checkAndTerminate(False, "Scoring pipeline completed successfully", inLogger, inSpark)