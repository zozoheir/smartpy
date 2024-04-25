class LambdaClient:

    def __init__(self,
                 redis_client):
        self.redis_client = redis_client

    def get(self,
            namespace,
            key,
            **kwargs):
        return self.redis_client.get(f"{namespace}:{key}&{kwargs}")