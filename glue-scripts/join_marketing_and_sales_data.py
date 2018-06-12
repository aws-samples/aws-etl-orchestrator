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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path', 's3_sales_data_path', 's3_marketing_data_path'])
s3_output_path = args['s3_output_path']
s3_sales_data_path = args['s3_sales_data_path']
s3_marketing_data_path = args['s3_marketing_data_path']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

salesForecastedByDate_DF = \
    glueContext.spark_session.read.option("header", "true")\
        .load(s3_sales_data_path, format="parquet")

mktg_DF = \
    glueContext.spark_session.read.option("header", "true")\
        .load(s3_marketing_data_path, format="parquet")


salesForecastedByDate_DF\
    .join(mktg_DF, 'date', 'inner')\
    .orderBy(salesForecastedByDate_DF['date']) \
    .write \
    .format('csv') \
    .option('header', 'true') \
    .mode('overwrite') \
    .save(s3_output_path)
