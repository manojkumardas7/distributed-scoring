class TestDistributedScoring:
  def setup_class(self):
    from pyspark.sql import SparkSession
    self.spark = SparkSession.builder.appName("pyTestScript").getOrCreate()

  def teardown_class(self):
    self.spark.stop()

  def test_mojoScoring(self):
    from sparkBrick import mojoModelScoring
    testingFrame = self.spark.read.csv("../inputs/TestSetMini.csv")
    mojoFile = "../model/pipeline.mojo"
    status, message, _ = mojoModelScoring(self.spark, testingFrame, mojoFile)
    assert status == True

  def test_pmmlModelScoring(self):
    from sparkBrick import pmmlModelScoring
    testingFrame = self.spark.read.csv("../inputs/TestSetMini.csv")
    pmmlFile = "../model/model.pmml"
    status, message, _ = pmmlModelScoring(self.spark, testingFrame, pmmlFile)
    print(message)
    assert status == True