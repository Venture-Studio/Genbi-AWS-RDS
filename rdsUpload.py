import boto3
import pandas as pd
import json
from sqlalchemy import create_engine
from botocore.exceptions import ClientError
#to upload new data change on line 9 & line 51

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('./ETFprices.csv')                                             #to upload new data change path

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

# Fetch the credentials from Secrets Manager
credentials = get_secret()

if credentials:
    # Extract the details
    db_host = "database-1-instance-1.c3qm0wkmu4zb.us-west-2.rds.amazonaws.com"
    db_name = "postgres"
    db_user = credentials['username']
    db_pass = credentials['password']
    db_port = credentials.get('port', '5432')  # Default PostgreSQL port
    print("db_user: ", db_user)
    print("db_pass: ", db_pass)
    # Create the connection string using SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    try:
        # Upload the DataFrame to the specified table in RDS
        df.to_sql('etf_prices_data', con=engine, if_exists='replace', index=False)              #to upload new data change name of the table
        print("Data uploaded successfully!")
    except Exception as e:
        print(f"Failed to upload data: {e}")
    finally:
        # Close the connection
        engine.dispose()

else:
    print("Failed to retrieve database credentials.")
