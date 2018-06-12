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


mktg_DyF = glueContext.create_dynamic_frame\
    .from_catalog(database=database_name, table_name=table_name)

mktg_DyF = ApplyMapping.apply(frame=mktg_DyF, mappings=[
    ('date', 'string', 'date', 'string'),
    ('new visitors seo', 'bigint', 'new_visitors_seo', 'bigint'),
    ('new visitors cpc', 'bigint', 'new_visitors_cpc', 'bigint'),
    ('new visitors social media', 'bigint', 'new_visitors_social_media', 'bigint'),
    ('return visitors', 'bigint', 'return_visitors', 'bigint'),
        ], transformation_ctx='applymapping1')

print 'Count:  ', mktg_DyF.count()
mktg_DyF.printSchema()

mktg_DF = mktg_DyF.toDF()

mktg_DF.write\
    .format('parquet')\
    .option('header', 'true')\
    .mode('overwrite')\
    .save(s3_output_path)

job.commit()