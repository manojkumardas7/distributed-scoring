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
from pysparkling.ml import H2OMOJOPipelineModel
from pyspark.sql.functions import udf as pandas_udf

labelGenerator = pandas_udf(lambda x:  x > 0.5, BooleanType())
mojo = H2OMOJOPipelineModel.createFromMojo(os.path.join(*[absoluteCodePath, "model", "pipeline.mojo"]))
_, df = getSparkFrameFromCSV(spark, os.path.join(*[absoluteCodePath, "model", "TestSet.csv"]))
final = mojo.transform(df).select(df.columns + [col("prediction.*")]).select(df.columns + [col("`Label.b`").alias("pb"), col("`Label.s`").alias("ps")]).withColumn("prediction", labelGenerator("ps").cast(IntegerType()))