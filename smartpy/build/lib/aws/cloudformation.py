import time

import pandas as pd

from smartpy.aws.s3 import S3
from smartpy.utility.aws_util import processResponse

# https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/protect-stack-resources.html

s3 = S3()



class CloudFormation:

    def __init__(self, region_name):
        self.boto3_client =  boto3.session.Session(profile_name=SMART_UNIVERSE_ENTITY_NAME).client('cloudformation', region_name=region_name)
        self.current_stack_id = None
        self.setCurrentStackID()

    def setCurrentStackID(self):
        current_stacks = self.listStacks()
        last_stack_status = \
            current_stacks[current_stacks['CreationTime'] == max(current_stacks['CreationTime'])]['StackStatus'].values[
                0]
        if last_stack_status == 'CREATE_COMPLETE':
            self.current_stack_id = \
                current_stacks[current_stacks['CreationTime'] == max(current_stacks['CreationTime'])]['StackId'].values[
                    0]
        else:
            self.current_stack_id = None

    def createStack(self, stack_name, bucket, key, cloudformation_template_url, cloudformation_template_file_path, entity, timeout_minutes=20):
        """
        Upload the local template to S3, and create a stack for the entity in question
        """
        print('Cloudformation : creating stack "{}"'.format(stack_name))
        # Check if a stack already exists for this environment
        if self.current_stack_id:
            raise Exception('Cloudformation : A stack already exists')

        # We first need to upload the cloudformation template to S3 so boto3 can access it
        s3.uploadFile(bucket=bucket,
                      key=key,
                      file_path=cloudformation_template_file_path)
        time.sleep(1)
        # Check that the template exists in S3
        if not s3.isFile(bucket=bucket, key=key):
            raise Exception('Cloudformation : template doesn\'t exit in S3')

        response = self.boto3_client.create_stack(
            StackName=stack_name,
            TemplateURL=cloudformation_template_url,
            TimeoutInMinutes=timeout_minutes,
            OnFailure='DELETE',
            Tags=[{'Key': 'entity', 'Value': entity}],
            EnableTerminationProtection=False
        )
        response = processResponse(response)

        stack_id = response['StackId']
        # Get creation updates
        while True:
            current_stacks = self.describeStack(stack_name)
            stack_status = current_stacks[current_stacks['StackId'] == stack_id]['StackStatus'].values[0]
            if stack_status != 'CREATE_COMPLETE':
                print('Cloudformation : stack creation status: {}'.format(stack_status))
            else:
                print('Cloudformation : stack created successfully')
                break
            time.sleep(10)
        time.sleep(1)
        self.setCurrentStackID()
        return response

    def deleteStack(self, stack_name):
        self.setCurrentStackID()
        if self.current_stack_id:
            response = self.boto3_client.delete_stack(StackName=stack_name)
            response = processResponse(response)
            while True:
                historical_stacks = self.listStacks()
                current_stack_new_status = \
                    historical_stacks[historical_stacks['StackId'] == self.current_stack_id]['StackStatus'].values[0]
                if current_stack_new_status != 'DELETE_COMPLETE':
                    print('Cloudformation : stack deletion status: {}'.format(current_stack_new_status))
                else:
                    print('Cloudformation : stack deleted successfully')
                    break
                time.sleep(10)
            self.setCurrentStackID()
            return response
        else:
            print('Cloudformation : no stack to delete')

    def describeStack(self, stack_name):
        response = self.boto3_client.describe_stacks(StackName=stack_name)
        response = processResponse(response)
        return pd.DataFrame(response['Stacks'])

    def listStacks(self):
        response = self.boto3_client.list_stacks()
        response = processResponse(response)
        return pd.DataFrame(response['StackSummaries'])
