import json
import logging

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)


def handler(event, context):
    logger.info(json.dumps(event, indent=2))
    
    for rec in event['Records']:
        if rec['eventName'] == 'error':
            raise Exception('Test Error')
        
    return {'message': 'ok'}
