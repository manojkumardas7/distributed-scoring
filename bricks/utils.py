'''
# ================================================================================#
#-- Authors: Manoj Kumar Das(manojkumardas7@gmail.com), Akshit Gattani(gattani.akshit@gmail.com)
#-- Date: July 27, 2020
#-- Description: collection of useful utilities
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

def modelFileFinder(modelPath, supportedFormats):
    """
    This function will return the type of model file present in the model folder

    Syntax:
        status, modelFileFormat = modelFileFinder() 

    Args:
        None

    Returns:
        status (bool)         : True/False based on availability of model file
        message (str)         : message explaining the status of function execution 
        modelFile (str)       : name of model file found in the directory
    """
    if not os.path.isdir(modelPath):
        message = "Error! Specified mdoel path does not exist: " + modelPath
        print("\n" + message + "\n")
        return False, message, None, None
    modelFile = list(filter(lambda x: x.split('.')[-1] in supportedFormats, os.listdir(modelPath)))
    if modelFile:
        if len(modelFile) == 1:
            message = "Model file found!: " + modelFile[0]
            print("\n" + message + "\n")
            return True, message, modelFile[0].split(".")[-1], modelFile[0]
        else:
            message = "Error! 0 or more than 1 supported model files present in " + modelPath + ": "  + " ,".join(modelFile)
            print("\n" + message + "\n")
            return False, message, None, None
    else :
        message =  "Error! No supported model file formats found in " + modelPath
        print("\n" + message + "\n")
        return False, message, None, None