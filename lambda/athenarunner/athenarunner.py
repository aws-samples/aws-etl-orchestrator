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
    with open('athenarunner-config.json', 'r') as conf_file:
        conf_json = conf_file.read()
        return json.loads(conf_json)

def is_json(jsonstring):
    try:
        json_object = json.loads(jsonstring)
    except ValueError, e:
        return False
    return True

def start_athena_queries(config):

    ddb_table = dynamodb.Table(config["ddb_table"])

    logger.debug('Athena runner started')

    sfn_activity_arn = config['sfn_activity_arn']
    sfn_worker_name = config['sfn_worker_name']

    # Loop until no tasks are available from Step Functions
    while True:

        logger.debug('Polling for Athena tasks for Step Functions Activity ARN: {}'.format(sfn_activity_arn))

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

        # Parse task input and create Athena query input
        task_input = ''
        try:

            task_input = json.loads(response['input'])
            task_input_dict = json.loads(task_input)

            athena_named_query = task_input_dict.get('AthenaNamedQuery', None)
            if athena_named_query is None:
                athena_query_string = task_input_dict['AthenaQueryString']
                athena_database = task_input_dict['AthenaDatabase']
            else:
                logger.debug('Retrieving details of named query "{}" from Athena'.format(athena_named_query))

                response = athena.get_named_query(
                    NamedQueryId=athena_named_query
                )

                athena_query_string = response['NamedQuery']['QueryString']
                athena_database = response['NamedQuery']['Database']

            athena_result_output_location = task_input_dict['AthenaResultOutputLocation']
            athena_result_encryption_option = task_input_dict.get('AthenaResultEncryptionOption', None)
            athena_result_kms_key = task_input_dict.get('AthenaResultKmsKey', "NONE").capitalize()

            athena_result_encryption_config = {}
            if athena_result_encryption_option is not None:

                athena_result_encryption_config['EncryptionOption'] = athena_result_encryption_option

                if athena_result_encryption_option in ["SSE_KMS", "CSE_KMS"]:
                    athena_result_encryption_config['KmsKey'] = athena_result_kms_key


        except (KeyError, Exception):
            logger.critical('Invalid Athena Runner input. Make sure required input parameters are properly specified.')
            raise



        # Run Athena Query
        logger.info('Querying "{}" Athena database using query string: "{}"'.format(athena_database, athena_query_string))

        try:

            response = athena.start_query_execution(
                QueryString=athena_query_string,
                QueryExecutionContext={
                    'Database': athena_database
                },
                ResultConfiguration={
                    'OutputLocation': athena_result_output_location,
                    'EncryptionConfiguration': athena_result_encryption_config
                }
            )

            athena_query_execution_id = response['QueryExecutionId']

            # Store SFN 'Task Token' and 'Query Execution Id' in DynamoDB

            item = {
                'sfn_activity_arn': sfn_activity_arn,
                'athena_query_execution_id': athena_query_execution_id,
                'sfn_task_token': task_token
            }

            ddb_table.put_item(Item=item)


        except Exception as e:
            logger.error('Failed to query Athena database "{}" with query string "{}"..'.format(athena_database, athena_query_string))
            logger.error('Reason: {}'.format(e.message))
            logger.info('Sending "Task Failed" signal to Step Functions.')

            response = sfn.send_task_failure(
                taskToken=task_token,
                error='Failed to start Athena query. Check Athena Runner logs for more details.'
            )
            return


        logger.info('Athena query started. Query Execution Id: {}'.format(athena_query_execution_id))


def check_athena_queries(config):

    # Query all items in table for a particular SFN activity ARN
    # This should retrieve records for all started athena queries for this particular activity ARN
    ddb_table = dynamodb.Table(config['ddb_table'])
    sfn_activity_arn = config['sfn_activity_arn']

    ddb_resp = ddb_table.query(
        KeyConditionExpression=Key('sfn_activity_arn').eq(sfn_activity_arn),
        Limit=config['ddb_query_limit']
    )

    # For each item...
    for item in ddb_resp['Items']:

        athena_query_execution_id = item['athena_query_execution_id']
        sfn_task_token = item['sfn_task_token']

        logger.debug('Polling Athena query execution status..')

        # Query athena query execution status...
        athena_resp = athena.get_query_execution(
            QueryExecutionId=athena_query_execution_id
        )

        query_exec_resp = athena_resp['QueryExecution']
        query_exec_state = query_exec_resp['Status']['State']
        query_state_change_reason = query_exec_resp['Status'].get('StateChangeReason', '')

        logger.debug('Query with Execution Id {} is currently in state "{}"'.format(query_exec_state, query_state_change_reason))

        # If Athena query completed, return success:
        if query_exec_state in ['SUCCEEDED']:

            logger.info('Query with Execution Id {} SUCCEEDED.'.format(athena_query_execution_id))

            # Build an output dict and format it as JSON
            task_output_dict = {
                "AthenaQueryString": query_exec_resp['Query'],
                "AthenaQueryExecutionId": athena_query_execution_id,
                "AthenaQueryExecutionState": query_exec_state,
                "AthenaQueryExecutionStateChangeReason": query_state_change_reason,
                "AthenaQuerySubmissionDateTime": query_exec_resp['Status'].get('SubmissionDateTime', '').strftime('%x, %-I:%M %p %Z'),
                "AthenaQueryCompletionDateTime": query_exec_resp['Status'].get('CompletionDateTime', '').strftime(
                    '%x, %-I:%M %p %Z'),
                "AthenaQueryEngineExecutionTimeInMillis": query_exec_resp['Statistics'].get('EngineExecutionTimeInMillis', 0),
                "AthenaQueryDataScannedInBytes": query_exec_resp['Statistics'].get('DataScannedInBytes', 0)
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
                    'athena_query_execution_id': athena_query_execution_id
                }
            )

            # Task succeeded, next item

        elif query_exec_state in ['RUNNING', 'QUEUED']:
            logger.debug('Query with Execution Id {} is in state hasn\'t completed yet.'.format(athena_query_execution_id))

            # Send heartbeat
            sfn_resp = sfn.send_task_heartbeat(
                taskToken=sfn_task_token
            )

            logger.debug('Heartbeat sent to Step Functions.')

            # Heartbeat sent, next item

        elif query_exec_state in ['FAILED', 'CANCELLED']:

            message = 'Athena query with Execution Id "{}" failed. Last state: {}. Error message: {}'\
                .format(athena_query_execution_id, query_exec_state, query_state_change_reason)

            logger.error(message)

            message_json={
                "AthenaQueryString": query_exec_resp['Query'],
                "AthenaQueryExecutionId": athena_query_execution_id,
                "AthenaQueryExecutionState": query_exec_state,
                "AthenaQueryExecutionStateChangeReason": query_state_change_reason,
                "AthenaQuerySubmissionDateTime": query_exec_resp['Status'].get('SubmissionDateTime', '').strftime('%x, %-I:%M %p %Z'),
                "AthenaQueryCompletionDateTime": query_exec_resp['Status'].get('CompletionDateTime', '').strftime(
                    '%x, %-I:%M %p %Z'),
                "AthenaQueryEngineExecutionTimeInMillis": query_exec_resp['Statistics'].get('EngineExecutionTimeInMillis', 0),
                "AthenaQueryDataScannedInBytes": query_exec_resp['Statistics'].get('DataScannedInBytes', 0)
            }

            sfn_resp = sfn.send_task_failure(
                taskToken=sfn_task_token,
                cause=json.dumps(message_json),
                error='AthenaQueryFailedError'
            )

            # Delete item
            resp = ddb_table.delete_item(
                Key={
                    'sfn_activity_arn': sfn_activity_arn,
                    'athena_query_execution_id': athena_query_execution_id
                }
            )

            logger.error(message)

            # Task failed, next item


athena = boto3.client('athena')
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=50, read_timeout=70)
sfn = boto3.client('stepfunctions', config=sfn_client_config)
dynamodb = boto3.resource('dynamodb')

# Load logging config and create logger
logger = load_log_config()

def handler(event, context):

    logger.debug('*** Athena Runner lambda function starting ***')

    try:

        # Get config (including a single activity ARN) from local file
        config = load_config()

        # One round of starting Athena queries
        start_athena_queries(config)

        # One round of checking on Athena queries
        check_athena_queries(config)

        logger.debug('*** Master Athena Runner terminating ***')

    except Exception as e:
        logger.critical('*** ERROR: Athena runner lambda function failed ***')
        logger.critical(e.message)
        raise

