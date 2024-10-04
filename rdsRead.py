import boto3
import json
from flask import Flask, jsonify

app = Flask(__name__)

def get_secret():
    secret_name = "rds!cluster-5dfdda0f-7553-48a3-ac44-83595bb9fd10"  # Replace with your actual secret name
    region_name = "us-west-2"  # Replace with your AWS region

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

    # Parse and return the secret string
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)  # Convert secret string to a dictionary
def execute_sql(sql_statement):
    credentials = get_secret()

    if credentials:
        # RDS Data API setup
        rds_client = boto3.client('rds-data', region_name='us-west-2')  # Specify your AWS region

        # Construct the request payload
        database = "postgres"  # Replace with your DB name
        cluster_arn = "arn:aws:rds:us-west-2:891377173529:cluster:database-1"  # Replace with your cluster ARN
        secret_arn = "arn:aws:secretsmanager:us-west-2:891377173529:secret:rds!cluster-5dfdda0f-7553-48a3-ac44-83595bb9fd10-HXSmrT"  # Replace with your secret ARN

        # Execute the SQL statement via RDS Data API
        try:
            response = rds_client.execute_statement(
                resourceArn=cluster_arn,
                secretArn=secret_arn,
                database=database,
                sql=sql_statement
            )
            return response['records']
        except Exception as e:
            print(f"Error executing SQL: {e}")
            return None
    else:
        print("Failed to retrieve database credentials.")
        return None

# Define a route to fetch data from the RDS database via API
@app.route('/api/data', methods=['GET'])
def get_data():
    sql_statement = "SELECT * FROM ets_data LIMIT 50 OFFSET 0"  # Replace with your table/query
    data = execute_sql(sql_statement)
    
    if data:
        return jsonify(data)
    else:
        return jsonify({"error": "Failed to retrieve data."})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
