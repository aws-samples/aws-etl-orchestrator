# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import pyspark.sql.functions as func
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path', 'database_name', 'table_name'])
s3_output_path = args['s3_output_path']
database_name = args['database_name']
table_name = args['table_name']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



sales_DyF = glueContext.create_dynamic_frame\
                .from_catalog(database=database_name, table_name=table_name)

sales_DyF = ApplyMapping.apply(frame=sales_DyF, mappings=[
    ('date', 'string', 'date', 'string'),
    ('lead name', 'string', 'lead_name', 'string'),
    ('forecasted monthly revenue', 'bigint', 'forecasted_monthly_revenue', 'bigint'),
    ('opportunity stage', 'string', 'opportunity_stage', 'string'),
    ('weighted revenue', 'bigint', 'weighted_revenue', 'bigint'),
    ('target close', 'string', 'target_close', 'string'),
    ('closed opportunity', 'string', 'closed_opportunity', 'string'),
    ('active opportunity', 'string', 'active_opportunity', 'string'),
    ('last status entry', 'string', 'last_status_entry', 'string'),
        ], transformation_ctx='applymapping1')

print 'Count:  ', sales_DyF.count()
sales_DyF.printSchema()

sales_DF = sales_DyF.toDF()

salesForecastedByDate_DF = \
    sales_DF\
        .where(sales_DF['opportunity_stage'] == 'Lead')\
        .groupBy('date')\
        .agg({'forecasted_monthly_revenue': 'sum', 'opportunity_stage': 'count'})\
        .orderBy('date')\
        .withColumnRenamed('count(opportunity_stage)', 'leads_generated')\
        .withColumnRenamed('sum(forecasted_monthly_revenue)', 'forecasted_lead_revenue')\
        .coalesce(1)

salesForecastedByDate_DF.write\
    .format('parquet')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(s3_output_path)

job.commit()
