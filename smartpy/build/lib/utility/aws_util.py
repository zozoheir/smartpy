import time

def processResponse(response, wait=0):
    if str(response['ResponseMetadata']['HTTPStatusCode']) == '200':
        # print('OK')
        if wait > 0:
            print('Waiting {} sec...'.format(wait))
        time.sleep(wait)
        return response
    else:
        print('ERROR')
        print(response)
        raise Exception('ECS response error')

