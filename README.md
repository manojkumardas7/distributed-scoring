# Distributed Scoring
This is a utility code base that will enable distributed scoring on spark engine on big datasets over exported standard model objects, added features includes performing data preparation for feature input to the model

## Environment Setup

### Creating a scoring pipeline in 5 easy steps
- Step 1: Download the files from the master branch
- Step 2: Edit the csv file 'fr_query.csv' with your hive queries. Standards to follow are:
                
    - queries need to be in uppper case
    - associated parameters need to be lower case, parameters can be alpha-numeric and contain special characters, but needs to start with a letter
    - last query in the csv file is expected to generate data, that will be captured as a spark frame
- Step 3: Edit the csv file 'pipelineArguments.csv' with the parameters present in your query
    - first column should contain the parameters (case-insensitive)
    - second column can be an associated default value (optional) for the parameter
    - if no default value is provided, the parameter will be considered as a mandatory argument to be parsed to the scoring pipeline
- Step 4: Drop/Replace your exported model file (MOJO/PMML) inside the model folder: 
    - Supported formats are: **PMML**, **MOJO** 
    - Make sure you have only one model file of the acceptable format present in the model folder
    - Additional steps required, depending on the model format, are specifed in below sections

- Step 5: Edit the config.cfg file, for their arguments, below are the descriptions

        - mainAttributes
            appName: application name the spark job is going to take

        - scoringAttributes (optional arguments, comma separated with square brackets or blank)
            columnSelection: list of columns to be considered for the scoring post ETL  
            columnOut: list of columns the scoring columns should be renamed with
        -modelOutput
            hiveTable: name of the hive table the final scored data is appeneded to

### Run your pipeline
Your pipeline can now be executed by runnig required spark-submit command to main.py. Mandatory arguments need be parsed to the main.py file using double hyphens, optional arguments can also be parsed similarly. main.py will now impliment the ETL using the parameterized queries, supplemented by parameter values, after which the last query will be captured as a spark frame and utilized to performm scoring. You can have additional arguments as well included in 'pipelineArguments.csv' file to do more customization in the main.py file. You can run the scoring pipeline as per the below syntax:

    spark-submit --properties-file config.conf main.py <arguments to be parsed>
---
**NOTE**

- Arguments in the config.cfg file can also be parsed (over-ride) to the main.py file
- All relevant places where edit/entries are to be made have pre-filled values for the purpose of demonstration and are to be replaced

---

### MOJO
Place the required pysparkling water zip file in the directory codeZips from https://www.h2o.ai/download/, to use a mojo model

### PMML
Download and install this [library](https://github.com/autodeployai/pypmml-spark). If you already have a PySpark environment setup, follow the below steps:
* Clone the repository to your local machine
* Edit setup.py and remove **install_requires** section
* In the root directory of your cloned repository, execute below:

        pip install -e .
* Now run the below command

        link_pmml4s_jars_into_spark