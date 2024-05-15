import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713016575984 = glueContext.create_dynamic_frame.from_catalog(database="stedi-hba", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1713016575984")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713016576224 = glueContext.create_dynamic_frame.from_catalog(database="stedi-hba", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1713016576224")

# Script generated for node SQL Query
SqlQuery2647 = '''
select  stt.sensorReadingTime, stt.serialNumber, 
stt.distanceFromObject, 
act.user,act.x, act.y, act.z 
from stt join act
on stt.sensorReadingTime = act.timestamp
'''
SQLQuery_node1713016627549 = sparkSqlQuery(glueContext, query = SqlQuery2647, mapping = {"act":AWSGlueDataCatalog_node1713016576224, "stt":AWSGlueDataCatalog_node1713016575984}, transformation_ctx = "SQLQuery_node1713016627549")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1713016680430 = glueContext.getSink(path="s3://stedi-hba/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1713016680430")
MachineLearningCurated_node1713016680430.setCatalogInfo(catalogDatabase="stedi-hba",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1713016680430.setFormat("json")
MachineLearningCurated_node1713016680430.writeFrame(SQLQuery_node1713016627549)
job.commit()