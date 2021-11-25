import pymysql
import os
import sys
import logging
import boto3
from operator import itemgetter
from decimal import Decimal
import botocore

# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)

# Configuration Values
endpoint = os.environ['db_endpoint']
username = os.environ['db_admin_user']
password = os.environ['db_admin_password']
db_name = "configuration"

dynamo_table = "budgets"   

def scanRecursive(table):
    """
    NOTE: Anytime you are filtering by a specific equivalency attribute such as id, name 
    or date equal to ... etc., you should consider using a query not scan

    kwargs are any parameters you want to pass to the scan operation
    """
    response = table.scan()
    data = response.get('Items')
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


# Handler
def lambda_handler(event, context):
    # Connection
    try:
        conn = pymysql.connect(host=endpoint, user=username, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        # logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        # logger.error(e)
        sys.exit()

    # logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(dynamo_table)

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        query = "SELECT id, budget FROM advertiser_campaigns"
        cursor.execute(query)
    
    if not (sql_data := cursor.fetchall()):
        raise Exception('No existe el advertiser o campaign.')
    
    # dynamo_data = scanRecursive(table)

    # logger.debug(sql_data)
    # sql_keys = [str(i['id'] )for i in sql_data]
    # logger.debug(sql_keys)
    for item in sql_data:
        try:
            table.put_item(
                Item={
                    "campaign_id":str(item['id']),
                    "budget":Decimal(str(item['budget'])),
                    "balance": Decimal("0")
                },
                ConditionExpression='attribute_not_exists(campaign_id)'
            )
            # logger.debug(f"created: {item['id']}")
        except botocore.exceptions.ClientError as e:
                try:
                    # logger.debug('update')

                    table.update_item(
                        Key={
                            'campaign_id' : str(item['id'])
                        },
                        UpdateExpression="SET budget = :B",            
                        ConditionExpression="NOT budget = :B",
                        ExpressionAttributeValues={':B': Decimal(str(item['budget']) )}
                    )
                    # logger.debug(f"update: {item['id']}")
                except botocore.exceptions.ClientError as e:
                    pass
                    # logger.debug(e)

    # for item in dynamo_data:
    #     # logger.debug(item['campaign_id'] not in sql_keys)
    #     if(item['campaign_id'] not in sql_keys):
    #         try:
    #             table.delete_item(
    #                 Key={
    #                         'campaign_id' : str(item['campaign_id'])
    #                     }
    #             )
    #             # logger.debug(f"deleted: {item['campaign_id']}")
    #         except botocore.exceptions.ClientError as e:
    #             pass
    #             # logger.debug(e)