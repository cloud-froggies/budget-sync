import pymysql
import os
import json
import sys
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Configuration Values
endpoint = os.environ['db_endpoint']
username = os.environ['db_admin_user']
password = os.environ['db_admin_password']
db_name = "configuration"

dynamo_table = "budgets"   

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

    # with table.batch_writer() as batch:
    #     for item in items:
    #         batch.put_item(
    #             Item=item
    #         )
    # return table


