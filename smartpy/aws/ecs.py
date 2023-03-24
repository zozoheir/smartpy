import pandas as pd
from smartpy.utility.aws_util import processResponse


class ECS:

    def __init__(self, region_name):
        self.boto3_client = boto3.session.Session(profile_name=SMART_UNIVERSE_ENTITY_NAME).client('ecs',
                                                                                                  region_name=region_name)

    def registerTask(self, task_definition):
        response = self.boto3_client.register_task_definition(**task_definition)
        print('Registering ECS Task {}'.format(task_definition['family']))
        response = processResponse(response)
        return response

    def deregisterTask(self, task_definition_arn):
        response = self.boto3_client.deregister_task_definition(taskDefinition=task_definition_arn)
        print('Deregistering ECS Task ARN {}'.format(task_definition_arn))
        response = processResponse(response)
        return response

    def runTask(self, run_configuration):
        response = self.boto3_client.run_task(**run_configuration)
        print('Running task ECS Task {}'.format(run_configuration['taskDefinition']))
        response = processResponse(response)
        return response

    def stopTask(self, cluster, task_arn, reason):
        print('Stopping ECS Task Arn {}'.format(task_arn))
        response = self.boto3_client.stop_task(cluster=cluster, task=task_arn, reason=reason)
        response = processResponse(response)
        return response

    def describeLiveTasks(self, cluster_name):
        # We use the default cluster for now until we might need another 1
        task_list = self.listTasks(cluster_name)
        if len(task_list["taskArns"]) > 0:
            current_tasks_descriptions = self.boto3_client.describe_tasks(cluster=cluster_name,
                                                                          tasks=task_list["taskArns"])
            return pd.DataFrame(current_tasks_descriptions)
        else:
            return {'tasks': []}

    def listTasks(self, cluster_name):
        response = self.boto3_client.list_tasks(cluster=cluster_name)
        response = processResponse(response)
        return response

    def getTaskNamesArnMapping(self):
        arns = self.ecs.listTaskDefinitions()['taskDefinitionArns']
        return {self.ecs._getTaskNameFromDefinitionArn(i): i for i in arns}

    def listTaskDefinitions(self):
        return self.boto3_client.list_task_definitions()

    def listTaskDefinitionFamilies(self):
        return self.boto3_client.list_task_definition_families()

    def desribeLiveClusters(self):
        clusters_list = self.boto3_client.list_clusters(
            maxResults=10
        )
        clusters_list = processResponse(clusters_list)
        if len(clusters_list["clusterArns"]) > 0:
            current_cluster_descriptions = self.boto3_client.describe_clusters(clusters=clusters_list["clusterArns"])
            return pd.DataFrame(current_cluster_descriptions['clusters'])
        else:
            return {'clusters': pd.DataFrame()}

    def createCluster(self, cluster_name):
        print('Creating cluster {}'.format(cluster_name))
        response = self.boto3_client.create_cluster(clusterName=cluster_name)
        return processResponse(response)

    def deleteCluster(self, cluster_name):
        response = self.boto3_client.delete_cluster(cluster=cluster_name)
        return processResponse(response)

    def _getTaskNameFromDefinitionArn(self, task_definition_arn):
        return task_definition_arn.split('task-definition/')[1].split(':')[0]
