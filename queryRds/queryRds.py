#lambda function to query RDS
import psycopg2  # Use psycopg2-binary instead of psycopg
import boto3
import json
import os
import sys

def get_secret():
    secret_name = os.getenv('SECRET_NAME')  # Store the secret name in environment variables
    region_name = os.getenv('AWS_REGION')

    # Create a Secrets Manager client
    client = boto3.client('secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e

    # Parse and return the secret string as a dictionary
    return json.loads(get_secret_value_response['SecretString'])

def get_db_connection():
    credentials = get_secret()

    if credentials:
        try:
            # Extract database connection information from secrets
            db_host = "database-1-instance-1.c3qm0wkmu4zb.us-west-2.rds.amazonaws.com"
            db_name = "postgres"
            db_user = credentials['username']
            db_password = credentials['password']
            db_port = credentials.get('port', '5432')

            # Establish a connection to the PostgreSQL database using psycopg2
            connection = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password,
                port=db_port
            )
            return connection
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            sys.exit(1)
    else:
        print("Failed to retrieve database credentials.")
        sys.exit(1)

def execute_sql(sql_statement):
    connection = get_db_connection()

    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(sql_statement)
            # Fetch all results
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"Error executing SQL: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
    else:
        print("Failed to establish database connection.")
        return None


import json

def lambda_handler(event, context):
    try:
        # Parse the request body (API Gateway sends the body as a raw string)
        body = json.loads(event.get('body', '{}'))
        
        # Extract the SQL query from the parsed body
        sql_statement = body.get('sql_query')
        
        if not sql_statement:
            return {
                'statusCode': 400,
                'body': json.dumps({"error": "SQL query parameter is required."})
            }
        
        # Execute the provided SQL query
        data = execute_sql(sql_statement)
        
        if data:
            return {
                'statusCode': 200,
                'body': json.dumps(data, default=str)  # Convert the data to JSON
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({"error": "Failed to retrieve data."})
            }
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({"error": "Invalid JSON in request body"})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }

