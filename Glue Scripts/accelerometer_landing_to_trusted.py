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
AWSGlueDataCatalog_node1712958397706 = glueContext.create_dynamic_frame.from_catalog(database="stedi-hba", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1712958397706")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712958400403 = glueContext.create_dynamic_frame.from_catalog(database="stedi-hba", table_name="accelerator_landing", transformation_ctx="AWSGlueDataCatalog_node1712958400403")

# Script generated for node SQL Query
SqlQuery2137 = '''
select user, timestamp, x, y, z from al join  ct
on al.user = ct.email

'''
SQLQuery_node1712958676295 = sparkSqlQuery(glueContext, query = SqlQuery2137, mapping = {"al":AWSGlueDataCatalog_node1712958400403, "ct":AWSGlueDataCatalog_node1712958397706}, transformation_ctx = "SQLQuery_node1712958676295")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1712958721146 = glueContext.getSink(path="s3://stedi-hba/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1712958721146")
AccelerometerTrusted_node1712958721146.setCatalogInfo(catalogDatabase="stedi-hba",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1712958721146.setFormat("json")
AccelerometerTrusted_node1712958721146.writeFrame(SQLQuery_node1712958676295)
job.commit()