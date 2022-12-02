import logging
import time

import botocore
from botocore.exceptions import ClientError

from smartpy.constants import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class Logs:

    def __init__(self, region_name):
        self.boto3_client = boto3.session.Session(profile_name=SMART_UNIVERSE_ENTITY_NAME).client('logs',
                                                                                                  region_name=region_name)

    def log(self, group, stream, msg):
        msg = msg.lower().capitalize()
        events = [{
            'timestamp': int(round(time.time() * 1000)),
            'message': msg
        }]
        self.boto3_client.put_log_events(logGroupName=group, logStreamName=stream, logEvents=events)

    def createGroupsAndStreams(self, logs_structure: dict):
        """
        Takes in a dictionary of form {group_name: [stream_name, stream_name]}
        """
        # Create groups
        for group in logs_structure.keys():
            try:
                response = self.boto3_client.create_log_group(
                    logGroupName=group,
                )
                print(response)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                    print("Group {} already exists".format(group))
                    continue
                else:
                    raise e
            # Create streams
            for stream in logs_structure[group]:
                try:
                    response = self.boto3_client.create_log_stream(
                        logGroupName=group,
                        logStreamName=stream
                    )
                    print(response)
                except Exception as e:
                    if e == botocore.errorfactory.ResourceAlreadyExistsException:
                        continue
                    else:
                        print('Exception creating log stream')
                        raise (e)

    def deleteLogsGroups(self, group_list):
        for group in group_list:
            response = self.boto3_client.delete_log_group(
                logGroupName=group,
            )
