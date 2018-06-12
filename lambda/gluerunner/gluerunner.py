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

import boto3
import logging, logging.config
import json
from botocore.client import Config
from boto3.dynamodb.conditions import Key, Attr


def load_log_config():
    # Basic config. Replace with your own logging config if required
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root

def load_config():
    with open('gluerunner-config.json', 'r') as conf_file:
        conf_json = conf_file.read()
        return json.loads(conf_json)

def is_json(jsonstring):
    try:
        json_object = json.loads(jsonstring)
    except ValueError, e:
        return False
    return True

def start_glue_jobs(config):

    ddb_table = dynamodb.Table(config["ddb_table"])

    logger.debug('Glue runner started')

    glue_job_capacity = config['glue_job_capacity']
    sfn_activity_arn = config['sfn_activity_arn']
    sfn_worker_name = config['sfn_worker_name']

    # Loop until no tasks are available from Step Functions
    while True:

        logger.debug('Polling for Glue tasks for Step Functions Activity ARN: {}'.format(sfn_activity_arn))

        try:
            response = sfn.get_activity_task(
                activityArn=sfn_activity_arn,
                workerName=sfn_worker_name
            )

        except Exception as e:
            logger.critical(e.message)
            logger.critical(
                'Unrecoverable error invoking get_activity_task for {}.'.format(sfn_activity_arn))
            raise

        # Keep the new Task Token for reference later
        task_token = response.get('taskToken', '')

        # If there are no tasks, return
        if not task_token:
            logger.debug('No tasks available.')
            return


        logger.info('Received task. Task token: {}'.format(task_token))

        # Parse task input and create Glue job input
        task_input = ''
        try:

            task_input = json.loads(response['input'])
            task_input_dict = json.loads(task_input)

            glue_job_name = task_input_dict['GlueJobName']
            glue_job_capacity = int(task_input_dict.get('GlueJobCapacity', glue_job_capacity))

        except (KeyError, Exception):
            logger.critical('Invalid Glue Runner input. Make sure required input parameters are properly specified.')
            raise

        # Extract additional Glue job inputs from task_input_dict
        glue_job_args = task_input_dict

        # Run Glue job
        logger.info('Running Glue job named "{}"..'.format(glue_job_name))

        try:
            response = glue.start_job_run(
                JobName=glue_job_name,
                Arguments=glue_job_args,
                AllocatedCapacity=glue_job_capacity
            )

            glue_job_run_id = response['JobRunId']

            # Store SFN 'Task Token' and Glue Job 'Run Id' in DynamoDB

            item = {
                'sfn_activity_arn': sfn_activity_arn,
                'glue_job_name': glue_job_name,
                'glue_job_run_id': glue_job_run_id,
                'sfn_task_token': task_token
            }

            ddb_table.put_item(Item=item)


        except Exception as e:
            logger.error('Failed to start Glue job named "{}"..'.format(glue_job_name))
            logger.error('Reason: {}'.format(e.message))
            logger.info('Sending "Task Failed" signal to Step Functions.')

            response = sfn.send_task_failure(
                taskToken=task_token,
                error='Failed to start Glue job. Check Glue Runner logs for more details.'
            )
            return


        logger.info('Glue job run started. Run Id: {}'.format(glue_job_run_id))


def check_glue_jobs(config):

    # Query all items in table for a particular SFN activity ARN
    # This should retrieve records for all started glue jobs for this particular activity ARN
    ddb_table = dynamodb.Table(config['ddb_table'])
    sfn_activity_arn = config['sfn_activity_arn']

    ddb_resp = ddb_table.query(
        KeyConditionExpression=Key('sfn_activity_arn').eq(sfn_activity_arn),
        Limit=config['ddb_query_limit']
    )

    # For each item...
    for item in ddb_resp['Items']:

        glue_job_run_id = item['glue_job_run_id']
        glue_job_name = item['glue_job_name']
        sfn_task_token = item['sfn_task_token']

        logger.debug('Polling Glue job run status..')

        # Query glue job status...
        glue_resp = glue.get_job_run(
            JobName=glue_job_name,
            RunId=glue_job_run_id,
            PredecessorsIncluded=False
        )

        job_run_state = glue_resp['JobRun']['JobRunState']
        job_run_error_message = glue_resp['JobRun'].get('ErrorMessage', '')

        logger.debug('Job with Run Id {} is currently in state "{}"'.format(glue_job_run_id, job_run_state))

        # If Glue job completed, return success:
        if job_run_state in ['SUCCEEDED']:

            logger.info('Job with Run Id {} SUCCEEDED.'.format(glue_job_run_id))

            # Build an output dict and format it as JSON
            task_output_dict = {
                "GlueJobName": glue_job_name,
                "GlueJobRunId": glue_job_run_id,
                "GlueJobRunState": job_run_state,
                "GlueJobStartedOn": glue_resp['JobRun'].get('StartedOn', '').strftime('%x, %-I:%M %p %Z'),
                "GlueJobCompletedOn": glue_resp['JobRun'].get('CompletedOn', '').strftime('%x, %-I:%M %p %Z'),
                "GlueJobLastModifiedOn": glue_resp['JobRun'].get('LastModifiedOn', '').strftime('%x, %-I:%M %p %Z')
            }

            task_output_json = json.dumps(task_output_dict)


            logger.info('Sending "Task Succeeded" signal to Step Functions..')
            sfn_resp = sfn.send_task_success(
                taskToken=sfn_task_token,
                output=task_output_json
            )

            # Delete item
            resp = ddb_table.delete_item(
                Key={
                    'sfn_activity_arn': sfn_activity_arn,
                    'glue_job_run_id': glue_job_run_id
                }
            )

            # Task succeeded, next item

        elif job_run_state in ['STARTING', 'RUNNING', 'STARTING', 'STOPPING']:
            logger.debug('Job with Run Id {} hasn\'t succeeded yet.'.format(glue_job_run_id))

            # Send heartbeat
            sfn_resp = sfn.send_task_heartbeat(
                taskToken=sfn_task_token
            )

            logger.debug('Heartbeat sent to Step Functions.')

            # Heartbeat sent, next item

        elif job_run_state in ['FAILED', 'STOPPED']:

            message = 'Glue job "{}" run with Run Id "{}" failed. Last state: {}. Error message: {}'\
                .format(glue_job_name, glue_job_run_id[:8] + "...", job_run_state, job_run_error_message)

            logger.error(message)

            message_json={
                'glue_job_name': glue_job_name,
                'glue_job_run_id': glue_job_run_id,
                'glue_job_run_state': job_run_state,
                'glue_job_run_error_msg': job_run_error_message
            }

            sfn_resp = sfn.send_task_failure(
                taskToken=sfn_task_token,
                cause=json.dumps(message_json),
                error='GlueJobFailedError'
            )

            # Delete item
            resp = ddb_table.delete_item(
                Key={
                    'sfn_activity_arn': sfn_activity_arn,
                    'glue_job_run_id': glue_job_run_id
                }
            )

            logger.error(message)

            # Task failed, next item


glue = boto3.client('glue')
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=50, read_timeout=70)
sfn = boto3.client('stepfunctions', config=sfn_client_config)
dynamodb = boto3.resource('dynamodb')

# Load logging config and create logger
logger = load_log_config()

def handler(event, context):

    logger.debug('*** Glue Runner lambda function starting ***')

    try:

        # Get config (including a single activity ARN) from local file
        config = load_config()

        # One round of starting Glue jobs
        start_glue_jobs(config)

        # One round of checking on Glue jobs
        check_glue_jobs(config)

        logger.debug('*** Master Glue Runner terminating ***')

    except Exception as e:
        logger.critical('*** ERROR: Glue runner lambda function failed ***')
        logger.critical(e.message)
        raise

