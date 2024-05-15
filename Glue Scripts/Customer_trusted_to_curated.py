import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712970684672 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-hba",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1712970684672",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712970723786 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-hba",
    table_name="accelerator_landing",
    transformation_ctx="AWSGlueDataCatalog_node1712970723786",
)

# Script generated for node Join
Join_node1712970751205 = Join.apply(
    frame1=AWSGlueDataCatalog_node1712970684672,
    frame2=AWSGlueDataCatalog_node1712970723786,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1712970751205",
)

# Script generated for node SQL Query
SqlQuery74 = """
select  serialnumber,
sharewithpublicasofdate,
birthday,
registrationdate,
sharewithresearchasofdate,
customername,
email,
lastupdatedate,
phone,
sharewithfriendsasofdate
from myDataSource
"""
SQLQuery_node1712970877267 = sparkSqlQuery(
    glueContext,
    query=SqlQuery74,
    mapping={"myDataSource": Join_node1712970751205},
    transformation_ctx="SQLQuery_node1712970877267",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1712971549102 = DynamicFrame.fromDF(
    SQLQuery_node1712970877267.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1712971549102",
)

# Script generated for node Customer Curated
CustomerCurated_node1712970976117 = glueContext.getSink(
    path="s3://stedi-hba/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1712970976117",
)
CustomerCurated_node1712970976117.setCatalogInfo(
    catalogDatabase="stedi-hba", catalogTableName="customer_curated"
)
CustomerCurated_node1712970976117.setFormat("json")
CustomerCurated_node1712970976117.writeFrame(DropDuplicates_node1712971549102)
job.commit()
