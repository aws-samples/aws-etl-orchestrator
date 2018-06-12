from __future__ import print_function

import json
import urllib
import boto3
import logging, logging.config
from botocore.client import Config

s3 = boto3.client('s3')
# Because Step Functions client uses long polling, read timeout has to be > 60 seconds
sfn_client_config = Config(connect_timeout=50, read_timeout=70)
sfn = boto3.client('stepfunctions', config=sfn_client_config)

sts = boto3.client('sts')
account_id = sts.get_caller_identity().get('Account')
region_name = boto3.session.Session().region_name

def load_log_config():
    # Basic config. Replace with your own logging config if required
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root

def map_activity_arn(bucket, key):
    # Map s3 key to activity ARN based on a convention
    # Here, we simply map bucket name plus last element in the s3 object key (i.e. filename) to activity name
    key_elements = [x.strip() for x in key.split('/')]
    activity_name = '{}-{}'.format(bucket, key_elements[-1])

    return 'arn:aws:states:{}:{}:activity:{}'.format(region_name, account_id, activity_name)

# Load logging config and create logger
logger = load_log_config()

def handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    # Based on a naming convention that maps s3 keys to activity ARNs, deduce the activity arn
    sfn_activity_arn = map_activity_arn(bucket, key)
    sfn_worker_name = 'on_s3_object_created'

    try:
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

        # Get the Task Token
        sfn_task_token = response.get('taskToken', '')

        logger.info('Sending "Task Succeeded" signal to Step Functions..')

        # Build an output dict and format it as JSON
        task_output_dict = {
            'S3BucketName': bucket,
            'S3Key': key,
            'SFNActivityArn': sfn_activity_arn
        }

        task_output_json = json.dumps(task_output_dict)

        sfn_resp = sfn.send_task_success(
            taskToken=sfn_task_token,
            output=task_output_json
        )


    except Exception as e:
        logger.critical(e)
        raise e
