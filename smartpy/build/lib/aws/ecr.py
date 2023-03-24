class ECR:

    def __init__(self, region_name):
        self.boto3_client = boto3.session.Session(profile_name=SMART_UNIVERSE_ENTITY_NAME).client('ecr', region_name=region_name)

    def getRepositoriesURLS(self) -> dict:
        repos = self.boto3_client.describe_repositories()['repositories']
        uris = [dic['repositoryUri'] for dic in repos]
        name_uri_dic = {uri.split('/')[1]: uri for uri in uris}
        return name_uri_dic
