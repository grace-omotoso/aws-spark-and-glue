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
AWSGlueDataCatalog_node1713014410190 = glueContext.create_dynamic_frame.from_catalog(database="stedi-hba", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1713014410190")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1713014445319 = glueContext.create_dynamic_frame.from_catalog(database="stedi-hba", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1713014445319")

# Script generated for node Join Customer Curated with Step Trainer
SqlQuery2532 = '''
select distinct sl.sensorReadingTime, sl.serialNumber,
sl.distanceFromObject
from cc join sl
ON cc.serialNumber = sl.serialNumber
'''
JoinCustomerCuratedwithStepTrainer_node1713014840675 = sparkSqlQuery(glueContext, query = SqlQuery2532, mapping = {"cc":AWSGlueDataCatalog_node1713014445319, "sl":AWSGlueDataCatalog_node1713014410190}, transformation_ctx = "JoinCustomerCuratedwithStepTrainer_node1713014840675")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1713014959994 = glueContext.getSink(path="s3://stedi-hba/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1713014959994")
StepTrainerTrusted_node1713014959994.setCatalogInfo(catalogDatabase="stedi-hba",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1713014959994.setFormat("json")
StepTrainerTrusted_node1713014959994.writeFrame(JoinCustomerCuratedwithStepTrainer_node1713014840675)
job.commit()