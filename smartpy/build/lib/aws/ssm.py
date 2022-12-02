from smartpy.constants import *

class ParameterStore:

    def __init__(self, region_name):
        self.boto3_client =  boto3.session.Session(profile_name=SMART_UNIVERSE_ENTITY_NAME).client('ssm', region_name)

    def getParameterValue(self, name):
        return self.boto3_client.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']
