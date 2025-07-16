import json
import pymysql
import boto3
import pandas as pd
import os
from datetime import datetime

def lambda_handler(event, context):
    DB_HOST = os.environ['DB_HOST']
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_NAME = os.environ['DB_NAME']
    
    s3_bucket  = 'zomato-daily-data'

    TABLES = ['Orders', 'customers', 'restaurants', 'riders', 'deliveries']

    
    # Connect to RDS MySQL
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    try:
        for table in TABLES:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()

                if rows:
                    df = pd.DataFrame(rows)
                    
                    # Create CSV in memory
                    csv_buffer = df.to_csv(index=False).encode('utf-8')

                    # Construct S3 key
                    s3_key = f"raw_data/{table}/{table}_{timestamp}.csv"

                    # Upload to S3
                    s3.put_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Body=csv_buffer
                    )
                    print(f"Uploaded {table} to S3 at {s3_key}")
                else:
                    print(f"No data found in table: {table}")
    finally:
        connection.close()

    