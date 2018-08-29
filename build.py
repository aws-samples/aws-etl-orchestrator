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

import os
import shutil
import zipfile
import time
from pynt import task
import boto3
import botocore
from botocore.exceptions import ClientError
import json
import re

def write_dir_to_zip(src, zf):
    '''Write a directory tree to an open ZipFile object.'''
    abs_src = os.path.abspath(src)
    for dirname, subdirs, files in os.walk(src):
        for filename in files:
            absname = os.path.abspath(os.path.join(dirname, filename))
            arcname = absname[len(abs_src) + 1:]
            print 'zipping %s as %s' % (os.path.join(dirname, filename),
                                        arcname)
            zf.write(absname, arcname)

def read_json(jsonf_path):
    '''Read a JSON file into a dict.'''
    with open(jsonf_path, 'r') as jsonf:
        json_text = jsonf.read()
        return json.loads(json_text)

def check_bucket_exists(s3path):
    s3 = boto3.resource('s3')

    result = re.search('s3://(.*)/', s3path)
    bucketname = s3path if result is None else result.group(1)
    bucket = s3.Bucket(bucketname)
    exists = True

    try:

        s3.meta.client.head_bucket(Bucket=bucketname)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
    return exists

@task()
def clean():
    '''Clean build directory.'''
    print 'Cleaning build directory...'
    
    if os.path.exists('build'):
    	shutil.rmtree('build')
    
    os.mkdir('build')

@task()
def packagelambda(* functions):
    '''Package lambda functions into a deployment-ready zip files.''' 
    if not os.path.exists('build'):
        os.mkdir('build')

    os.chdir("build")

    if(len(functions) == 0):
        functions = ("athenarunner", "gluerunner", "ons3objectcreated")

    for function in functions:
        print 'Packaging "{}" lambda function in directory'.format(function)
        zipf = zipfile.ZipFile("%s.zip" % function, "w", zipfile.ZIP_DEFLATED)
        
        write_dir_to_zip("../lambda/{}/".format(function), zipf)

        zipf.close()

    os.chdir("..")
    
    return


@task()
def updatelambda(*functions):
    '''Directly update lambda function code in AWS (without upload to S3).'''
    lambda_client = boto3.client('lambda')

    if(len(functions) == 0):
        functions = ("athenarunner", "gluerunner", "ons3objectcreated")

    for function in functions:
        with open('build/%s.zip' % function, 'rb') as zipf:
            lambda_client.update_function_code(
                FunctionName=function,
                ZipFile=zipf.read()
            )
    return


@task()
def deploylambda(*functions, **kwargs):
    '''Upload lambda functions .zip file to S3 for download by CloudFormation stack during creation.'''

    if (len(functions) == 0):
        functions = ("athenarunner", "gluerunner", "ons3objectcreated")

    region_name = boto3.session.Session().region_name

    s3_client = boto3.client("s3")

    print("Reading .lambda/s3-deployment-descriptor.json...")

    params = read_json("./lambda/s3-deployment-descriptor.json")

    for function in functions:

        src_s3_bucket_name = params[function]['SourceS3BucketName']
        src_s3_key = params[function]['SourceS3Key']

        if not src_s3_key and not src_s3_bucket_name:
            print(
                "ERROR: Both Source S3 bucket name and S3 key must be specified for function '{}'. FUNCTION NOT DEPLOYED.".format(
                    function))
            continue

        print("Checking if S3 Bucket '{}' exists...".format(src_s3_bucket_name))

        if (not check_bucket_exists(src_s3_bucket_name)):
            print("Bucket %s not found. Creating in region {}.".format(src_s3_bucket_name, region_name))

            if (region_name == "us-east-1"):
                s3_client.create_bucket(
                    # ACL="authenticated-read",
                    Bucket=src_s3_bucket_name
                )
            else:
                s3_client.create_bucket(
                    # ACL="authenticated-read",
                    Bucket=src_s3_bucket_name,
                    CreateBucketConfiguration={
                        "LocationConstraint": region_name
                    }
                )

        print "Uploading function '{}' to '{}'".format(function, src_s3_key)

        with open('build/{}.zip'.format(function), 'rb') as data:
            s3_client.upload_fileobj(data, src_s3_bucket_name, src_s3_key)

    return


@task()
def createstack(* stacks, **kwargs):
    '''Create stacks using CloudFormation.'''

    if (len(stacks) == 0):
        print("ERROR: Please specify a stack to create. Valid values are glue-resources, gluerunner-lambda, step-functions-resources.")
        return

    for stack in stacks:
        cfn_path = "cloudformation/{}.yaml".format(stack)
        cfn_params_path = "cloudformation/{}-params.json".format(stack)
        cfn_params = read_json(cfn_params_path)
        stack_name = stack

        cfn_file = open(cfn_path, 'r')
        cfn_template = cfn_file.read(51200) #Maximum size of a cfn template

        cfn_client = boto3.client('cloudformation')

        print("Attempting to CREATE '%s' stack using CloudFormation." % (stack_name))
        start_t = time.time()
        response = cfn_client.create_stack(
            StackName=stack_name,
            TemplateBody=cfn_template,
            Parameters=cfn_params,
            Capabilities=[
                'CAPABILITY_NAMED_IAM',
            ],
        )

        print("Waiting until '%s' stack status is CREATE_COMPLETE" % stack_name)

        try:

            cfn_stack_delete_waiter = cfn_client.get_waiter('stack_create_complete')
            cfn_stack_delete_waiter.wait(StackName=stack_name)
            print("Stack CREATED in approximately %d secs." % int(time.time() - start_t))

        except Exception as e:
            print("Stack creation FAILED.")
            print(e.message)


@task()
def updatestack(* stacks, **kwargs):
    '''Update a CloudFormation stack.'''

    if (len(stacks) == 0):
        print("ERROR: Please specify a stack to create. Valid values are glue-resources, gluerunner-lambda, step-functions-resources.")
        return

    for stack in stacks:
        stack_name = stack
        cfn_path = "cloudformation/{}.yaml".format(stack)
        cfn_params_path = "cloudformation/{}-params.json".format(stack)
        cfn_params = read_json(cfn_params_path)

        cfn_file = open(cfn_path, 'r')
        cfn_template = cfn_file.read(51200) #Maximum size of a cfn template

        cfn_client = boto3.client('cloudformation')

        print("Attempting to UPDATE '%s' stack using CloudFormation." % (stack_name))
        try:
            start_t = time.time()
            response = cfn_client.update_stack(
                StackName=stack_name,
                TemplateBody=cfn_template,
                Parameters=cfn_params,
                Capabilities=[
                    'CAPABILITY_NAMED_IAM',
                ],
            )

            print("Waiting until '%s' stack status is UPDATE_COMPLETE" % stack_name)
            cfn_stack_update_waiter = cfn_client.get_waiter('stack_update_complete')
            cfn_stack_update_waiter.wait(StackName=stack_name)

            print("Stack UPDATED in approximately %d secs." % int(time.time() - start_t))
        except ClientError as e:
            print "EXCEPTION: " + e.response["Error"]["Message"]

@task()
def stackstatus(* stacks):
    '''Check the status of a CloudFormation stack.'''

    if (len(stacks) == 0):
        stacks = ("glue-resources", "gluerunner-lambda", "step-functions-resources")

    for stack in stacks:
        stack_name = stack

        cfn_client = boto3.client('cloudformation')

        try:
            response = cfn_client.describe_stacks(
                StackName=stack_name
            )

            if(response["Stacks"][0]):
                print("Stack '%s' has the status '%s'" % (stack_name, response["Stacks"][0]["StackStatus"]))

        except ClientError as e:
            print "EXCEPTION: " + e.response["Error"]["Message"]


@task()
def deletestack(* stacks):
    '''Delete stacks using CloudFormation.'''

    if (len(stacks) == 0):
        print("ERROR: Please specify a stack to delete.")
        return

    for stack in stacks:
        stack_name = stack
    
        cfn_client = boto3.client('cloudformation')

        print("Attempting to DELETE '%s' stack using CloudFormation." % stack_name)
        start_t = time.time()
        response = cfn_client.delete_stack(
            StackName=stack_name
        )

        print("Waiting until '%s' stack status is DELETE_COMPLETE" % stack_name)
        cfn_stack_delete_waiter = cfn_client.get_waiter('stack_delete_complete')
        cfn_stack_delete_waiter.wait(StackName=stack_name)
        print("Stack DELETED in approximately %d secs." % int(time.time() - start_t))


@task()
def deploygluescripts(**kwargs):
    '''Upload AWS Glue scripts to S3 for download by CloudFormation stack during creation.'''

    region_name = boto3.session.Session().region_name

    s3_client = boto3.client("s3")

    glue_scripts_path = "./glue-scripts/"

    glue_cfn_params = read_json("cloudformation/glue-resources-params.json")

    s3_etl_script_path = ''

    for param in glue_cfn_params:
        if param['ParameterKey'] == 'S3ETLScriptPath':
            s3_etl_script_path = param['ParameterValue']

    if not s3_etl_script_path:
        print(
            "ERROR: S3ETLScriptPath must be set in 'cloudformation/glue-resources-params.json'.")
        return

    result = re.search('s3://(.+?)/(.*)', s3_etl_script_path)
    if(result is None):
        print("ERROR: S3ETLScriptPath is malformed.")
        return

    s3_bucket_name = result.group(1)
    s3_key = result.group(2)

    print("Checking if S3 Bucket '{}' exists...".format(s3_bucket_name))

    if (not check_bucket_exists(s3_bucket_name)):
        print("ERROR: S3 bucket for path '{}' not found.".format(s3_etl_script_path))
        return

    for dirname, subdirs, files in os.walk(glue_scripts_path):
        for filename in files:
            absname = os.path.abspath(os.path.join(dirname, filename))
            print "Uploading AWS Glue script '{}' to '{}/{}'".format(absname, s3_bucket_name, s3_key)
            with open(absname, 'rb') as data:
                s3_client.upload_fileobj(data, s3_bucket_name, '{}/{}'.format(s3_key, filename))

    return


@task()
def deletes3bucket(name):
    '''DELETE ALL objects in an Amazon S3 bucket and THE BUCKET ITSELF. Use with caution!'''

    proceed = raw_input(
        "This command will DELETE ALL DATA in S3 bucket '%s' and the BUCKET ITSELF.\nDo you wish to continue? [Y/N] " \
        % (name))

    if (proceed.lower() != 'y'):
        print("Aborting deletion.")
        return

    print("Attempting to DELETE ALL OBJECTS in '%s' S3 bucket." % name)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(name)
    bucket.objects.delete()
    bucket.delete()
    return