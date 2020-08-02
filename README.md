# distributed-scoring
This is a utility code base that will enable distributed scoring on spark engine on big datasets over exported standard model objects, added features includes performing data preparation for feature input to the model

## Environment Setup

### PMML

Download and install this [library](https://github.com/autodeployai/pypmml-spark). If you already have a PySpark environment setup, follow the below steps:
* Clone the repository to your local machine
* Edit setup.py and remove `install_requires` section
* In the root directory of your cloned repository, execute `pip install -e .`
* Execute `link_pmml4s_jars_into_spark`