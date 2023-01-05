# debit

from __future__ import print_function
import re
import json
import boto3
from decimal import Decimal 
from datetime import datetime
import time
from botocore.exceptions import ClientError

TABLE_SESSION = 'session'
TABLE_TRANSACTION = 'transaction'
TABLE_USER = 'user'
# TABLE_SESSION = 'OneWallet-session'
# TABLE_TRANSACTION = 'OneWallet-transaction2'
# TABLE_USER = 'OneWallet-user'

def verify_session(sessionID, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_SESSION)
    try:
        response = table.get_item(Key={'sid': sessionID})
        res = response['Item']['sid']
    except:
        return 0
    else:
        return 1

def find_transaction(transactionRefID, userID, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_TRANSACTION)
    
    transactions = []
    try:
        response = table.scan(
            FilterExpression='refId=:refId and userId=:userId',
            ExpressionAttributeValues={
                ':refId': transactionRefID,
                ':userId': userID
            },
            ProjectionExpression='transactionId, isdelete, transactionType'
        )
        transactions = response['Items']
    except ClientError as e:
        print(e)
    except:
        pass
    
    return transactions

def updateCancelStatus(transactionId, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_TRANSACTION)
    response = table.update_item(
        Key={
            'transactionId': transactionId,
        },
        UpdateExpression="set isdelete=:val",
        ExpressionAttributeValues={
            ':val': Decimal('1')
        },
        ReturnValues="UPDATED_NEW"
    )
    #return response

def put_transaction(transactionID, transactionAmount, transactionType, transactionIsDelete, transactionUserId, transactionRefId,dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_TRANSACTION)
    now = time.time()
    timestamp = datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')
    response = table.put_item(
       Item={
            'transactionId': transactionID,
            'amount': transactionAmount,
            'transactionType': transactionType,
            'isdelete': transactionIsDelete,
            'userId': transactionUserId,
            'refId': transactionRefId,
            'createTime': timestamp
        }
    )
    #return response

def add_balance(userId, addAmount, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_USER)
    response = table.update_item(
        Key={
            'userId': userId,
        },
        UpdateExpression="set balance = balance + :val",
        ExpressionAttributeValues={
            ':val': addAmount
        },
        ReturnValues="UPDATED_NEW"
    )
    #return response
    
def get_balance(userId, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_USER)
    try:
        response = table.get_item(Key={'userId': userId})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']['balance']
        
def lambda_handler(event, context):
    try:
        qsParam = event['queryStringParameters']['authToken']
        if not (qsParam == "s3cr3tV4lu3"):
            context = {
                "statusCode": 200,
                "headers": {
                    "my_header": "my_value"
                },
                "body": json.dumps({'status': 'INVALID_TOKEN_ID'}),
                "isBase64Encoded": False
            }
            return context
    except:
        context = {
                "statusCode": 200,
                "headers": {
                    "my_header": "my_value"
                },
                "body": json.dumps({'status': 'INVALID_TOKEN_ID'}),
                "isBase64Encoded": False
            }
        return context

    try:
        bodyParam = event['body']
        bodyKeyTransaction = json.loads(bodyParam)['transaction']
        bodyKeyTid = bodyKeyTransaction['id']
        bodyKeyTrefId = bodyKeyTransaction['refId']
        bodyKeyTamount = bodyKeyTransaction['amount']    
        bodyKeyUuid = json.loads(bodyParam)['uuid']

        try:
            bodyKeySid = json.loads(bodyParam)['sid']
            if not (bodyKeySid == "111ssss3333rrrrr45555" or bodyKeySid == "111ssss3333rrrrr46666" or verify_session(bodyKeySid)):
                context = {
                    "statusCode": 200,
                    "headers": {
                        "my_header": "my_value"
                    },
                    "body": json.dumps({'status': 'INVALID_SID'}),
                    "isBase64Encoded": False
                }
                return context
        except:
            context = {
                    "statusCode": 200,
                    "headers": {
                        "my_header": "my_value"
                    },
                    "body": json.dumps({'status': 'INVALID_SID'}),
                    "isBase64Encoded": False
                }
            return context

        try:
            bodyKeyId = json.loads(bodyParam)['userId']
            if not (bodyKeyId == "a1a2a3a4" or bodyKeyId == "b1b2b3b4"):
                context = {
                    "statusCode": 200,
                    "headers": {
                        "my_header": "my_value"
                    },
                    "body": json.dumps({'status': 'INVALID_PARAMETER'}),
                    "isBase64Encoded": False
                }
                return context
        except:
            context = {
                    "statusCode": 200,
                    "headers": {
                        "my_header": "my_value"
                    },
                    "body": json.dumps({'status': 'INVALID_PARAMETER'}),
                    "isBase64Encoded": False
                }
            return context

    except:
        context = {
                "statusCode": 200,
                "headers": {
                    "my_header": "my_value"
                },
                "body": json.dumps({'status': 'INVALID_PARAMETER'}),
                "isBase64Encoded": False
            }
        return context
    
    balance = get_balance(bodyKeyId)
    if float(balance) - bodyKeyTamount <0:
        context = {
                "statusCode": 200,
                "headers": {
                    "my_header": "my_value"
                },
                "body": json.dumps({'status': 'INSUFFICIENT_FUNDS'}),
                "isBase64Encoded": False
            }
        return context
    
    status = 'OK'
    transactions = find_transaction(bodyKeyTrefId, bodyKeyId)
    if transactions: # transaction exists
        status = 'BET_ALREADY_EXIST'
        try:
            if transactions[0]['transactionType'] == 'cancel' and transactions[0]['isdelete'] == Decimal('0'):
                status = 'FINAL_ERROR_ACTION_FAILED'
                updateCancelStatus(transactions[0]['transactionId'])
        except:
            pass
    else:
        put_transaction(bodyKeyTid, Decimal(str(bodyKeyTamount)), "debit" , Decimal('0'), bodyKeyId, bodyKeyTrefId)
        add_balance(bodyKeyId, Decimal(str(bodyKeyTamount)) * Decimal('-1'))
        balance += Decimal(str(bodyKeyTamount)) * Decimal('-1')

    responseBody = {
        'status': status,
        'balance': float(balance),
        'uuid': bodyKeyUuid
    }
    
    context = {
        "statusCode": 200,
        "headers": {
            "my_header": "my_value"
        },
        "body": json.dumps(responseBody),
        "isBase64Encoded": False
    }
    return context