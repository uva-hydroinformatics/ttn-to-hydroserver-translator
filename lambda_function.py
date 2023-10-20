import json
import urllib3
import secrets  
    
def lambda_handler(event, context):
    
    TTN_PASSWORD = secrets.TTN_PASSWORD
    
    if "authorization" in event["headers"]: 
        if event["headers"]["authorization"]!=TTN_PASSWORD:
            return {
            'statusCode': 200,
            'body': "Unknown message origin."
        }
    else:
        return {
            'statusCode': 200,
            'body': "Unknown message format."
        }

    HS_USERNAME = secrets.USERNAME
    HS_PASSWORD = secrets.PASSWORD 
    
    TTN_TO_DATA_STREAM_INFO = secrets.TTN_TO_DATA_STREAM_INFO
    hydroserver_sensorthings_api_url = secrets.SERVER_URL
    
    request_url = f'{hydroserver_sensorthings_api_url}/Observations'
    
    event_body = json.loads(event["body"])
    
    decoded_payload = event_body['uplink_message']['decoded_payload']
    
    device_id = str(event_body['uplink_message']['decoded_payload']['device_id'])
    
    time = event_body['uplink_message']['received_at']
    
    data_list = []
    if device_id in TTN_TO_DATA_STREAM_INFO:
        for variable_tuple in TTN_TO_DATA_STREAM_INFO[device_id]:
            data_list.append({
                    "Datastream": {
                      "@iot.id": variable_tuple[1]
                    },
                    "components": ["phenomenonTime", "result"],
                    "dataArray": [
                      [time, float(decoded_payload[variable_tuple[0]]['value'])]
                    ]
            })
    
        http = urllib3.PoolManager()
        headers = urllib3.make_headers(basic_auth=HS_USERNAME+':'+HS_PASSWORD)
        
        
        response = http.request('POST',
            request_url,
            body = json.dumps(data_list),
            headers = headers
            )

    return {
        'statusCode': 200,
        'body': json.dumps("message received and parsed!")
    }