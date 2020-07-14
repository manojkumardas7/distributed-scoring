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
from bricks.sparkBrick import getSparkFrameFromCSV, mojoModelScoring

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
        checkAndTerminate(False, 'Argument ' + y + ' placed incorrectly  in config file')

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
    configFileName = "test_config.cfg"
    #change the below dictionary accordingly, update the config file accordingly
    argsDiction = \
        {
            "modelParameters"  : [["mojoFile", "str"], ["pickleFile", "str"]],
            "scoringParameters": [["targetColumn", "str"], ["columnSelection", "list"], ["datasetPath", "str"]]
        }
    try:
        absoluteCodePath = os.path.split(os.path.realpath(__file__))[0]
    except Exception as e:
        absoluteCodePath = os.path.realpath(".")
    configFile = os.path.join(absoluteCodePath, configFileName)
    dataTypeDecode = {"str": "str", "int": "int", "bool": "bool", "list": "str", "float": "float"}
    parser = argparse.ArgumentParser()
    list(map(lambda x: list(
        map(lambda y: eval("parser.add_argument('--" + y[0] + "', type=" + dataTypeDecode[y[1]] + \
                                                                        ", help='specify " + y[0] + "')"),
            argsDiction[x])), argsDiction.keys())) is None
    args = parser.parse_args()
    if os.path.isfile(configFile):
        config = configparser.ConfigParser()
        config.read(configFile)
        parameterTypeParsersConf = {"str": lambda x, y: (lambda x: None if x == '' else x)(config.get(x, y).strip()),
                                    "bool": lambda x, y: (lambda x, y: bool(1) if x == "True" else \
                                                    bool(0) if x == "False" else None if x == '' else \
                                                    checkAndTerminate(False, "Argument " + y + \
                                                        " not correctly placed, \
                                                         can be left blank or acceptable case-sensitive values are \
                                                         'True' and 'False'"))(config.get(x, y).strip(), y),
                                    "int": lambda x, y: (lambda x, y: None if x == '' else \
                                                    dataParse(x, y, int))(config.get(x, y).strip(), y),
                                    "float": lambda x, y: (lambda x, y: None if x == '' else \
                                                    dataParse(x, y, float))(config.get(x, y).strip(), y),
                                    "list": lambda x, y: tryExcept(lambda: generateList((lambda x: None if x == '' else x) \
                                                            (config.get(x, y).strip()), y),
                                                            lambda: checkAndTerminate(False, 
                                                                "Argument "+ y + " not correctly placed in config file"))}
        parameterTypeParsersArg = {"str": lambda x, y: x,
                                   "bool": lambda x, y: x,
                                   "int": lambda x, y: x,
                                   "float": lambda x, y: x,
                                   "list": lambda x, y: tryExcept(lambda: generateList(x.strip(), y), 
                                                                   lambda: checkAndTerminate(False, 
                                                                    "Argument "+ y + " not correctly parsed"))}
        getConfig = lambda p, q, r: (lambda x, y: y if x is None else parameterTypeParsersArg[p](x, r))(eval("args." + r),
                                                                         parameterTypeParsersConf[p](q, r))
        list(map(lambda x: list(map(lambda y: createGlobalObject(myTitle(y[0]), getConfig(y[1], x, y[0])), argsDiction[x])),
                argsDiction.keys())) is None
    else:
        print("Warning: Config file not present")
        getConfig = lambda p, q, r: eval("args." + r)
        list(map(lambda x: list(map(lambda y: createGlobalObject(myTitle(y[0]), getConfig(y[1], x, y[0])), argsDiction[x])),
                argsDiction.keys())) is None
        list(map(lambda x: list(
            map(lambda y: checkAndTerminate(eval(myTitle(y[0])) is not None, y[0] + " is not specified"), argsDiction[x])),
                argsDiction.keys())) is None

    df = mojoModelScoring(spark, absoluteCodePath, myMojoFile, myDatasetPath)
# initiate spark frame code, put code lines here to initiate spark given that spark-submit command will be used usinig properties file as well
# create a spark frame from a sample csv (can create a function for that on the bricks)
# utilize your scoring funtion