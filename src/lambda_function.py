import pymysql
import os
import sys
import logging
import boto3
from operator import itemgetter
from decimal import Decimal
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(dynamo_table)

    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        query = "SELECT id, budget FROM advertiser_campaigns"
        cursor.execute(query)
    
    if not (sql_data := cursor.fetchall()):
        raise Exception('No existe el advertiser o campaign.')
    
    dynamo_data = scanRecursive(table)
    # from operator import itemgetter

    # sorted_sql = sorted(sql_data, key=itemgetter('id')) 

    # sorted_dynamo = sorted(dynamo_data, key=itemgetter('campaign_id')) 

    # changes = []
    # for x in sorted_sql:
    #     match = 0
    #     for y in sorted_dynamo:

    #         if(int(x['id']) == int(y['campaign_id'])):
    #             match =1

    #         if ((int(x['id']) == int(y['campaign_id'])) and (float(x['budget']) != float(y['budget']))):
    #             changes.append({
    #                 "campaign_id": y['campaign_id'],
    #                 "budget": Decimal(x['budget']),
    #                 "balance":y["balance"]
    #             })
    #             break
        
    #     if(match==0):
    #         changes.append({
    #             "campaign_id": x['id'],
    #             "budget": Decimal(x['budget']),
    #             "balance": Decimal(0)
    #         })
    
    # with table.batch_writer() as batch:
    print(sql_data)
    sql_keys = [str(i['id'] )for i in sql_data]
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
            print(f"created: {item['id']}")
        except botocore.exceptions.ClientError as e:
                try:
                    print('update')

                    table.update_item(
                        Key={
                            'campaign_id' : str(item['id'])
                        },
                        UpdateExpression="SET budget = :B",            
                        ConditionExpression="NOT budget = :B",
                        ExpressionAttributeValues={':B': Decimal(str(item['budget']) )}
                    )
                    print(f"update: {item['id']}")
                except botocore.exceptions.ClientError as e:
                    print(e)

    for item in dynamo_data:
        try:
            table.delete_item(
                Key={
                        'campaign_id' : str(item['campaign_id'])
                    },
                ConditionExpression="campaign_id NOT IN :arr",
                ExpressionAttributeValues={':arr': {'SS':sql_keys}}
            )
            print(f"deleted: {item['id']}")
        except botocore.exceptions.ClientError as e:
            print(e)
                
    

lambda_handler("","")
                







    
    




    # with table.batch_writer() as batch:
    #     for item in items:
    #         batch.put_item(
    #             Item=item
    #         )
    # return table

# dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
# table = dynamodb.Table(dynamo_table)

# dynamo_items = [
#     {
#         "campaign_id":"1",
#         "budget":"10.100",
#         "balance":"1"
#     },
#     {
#         "campaign_id":"2",
#         "budget":"1234.100",
#         "balance":"5"
#     },
#     {
#         "campaign_id":"3",
#         "budget":"11.100",
#         "balance":"4"
#     }
# ]

# sql_items = [
#     {
#         "id":"1",
#         "budget":"10.100"
#     },
#     {
#         "id":"3",
#         "budget":"3333.333"
#     },
#     {
#         "id":"2",
#         "budget":"2222.222"
#     },
#     {
#         "id":"4",
#         "budget":"444.444"
#     },
#     {
#         "id":"5",
#         "budget":"444.444"
#     }
# ]


# with table.batch_writer() as batch:
#     for item in dynamo_items:
#         batch.put_item(
#             Item=item
#         )

# data = scanRecursive(table)
# # print(data)


# from operator import itemgetter
# sorted_sql = sorted(sql_items, key=itemgetter('id')) 

# sorted_dynamo = sorted(dynamo_items, key=itemgetter('campaign_id')) 

# changes = []
# for x in sorted_sql:
#     match = 0
#     for y in sorted_dynamo:

#         if(int(x['id']) == int(y['campaign_id'])):
#             match =1

#         if ((int(x['id']) == int(y['campaign_id'])) and (float(x['budget']) != float(y['budget']))):
#             changes.append({
#                 "campaign_id": y['campaign_id'],
#                 "budget": Decimal(x['budget']),
#                 "balance":y["balance"]
#             })
#             break
    
#     if(match==0):
#         changes.append({
#             "campaign_id": x['id'],
#             "budget": Decimal(x['budget']),
#             "balance": Decimal(0)
#         })



# print(changes)

    
    
    


