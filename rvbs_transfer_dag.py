from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ["marlon.kraus@tasc-solutions.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_on_delay':timedelta(minutes=2)

}

#Define s3 bucket & file details

S3_FILEPATH = "s3://rvbsclient1/Inventory Details.csv"
BUCKET_NAME = None
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"



with DAG('snowflake_s3_with_email_notification_etl', 
         default_args=default_args, 
         schedule_interval="@daily",
         catchup=False) as dag:

         is_s3_file_available = S3KeySensor(
                 task_id='is_s3_file_available',
                 bucket_key=S3_FILEPATH,
                 bucket_name=BUCKET_NAME,
                 aws_conn_id =S3_CONN_ID,
                 wildcard_match=False,
                 poke_interval=3,
        )
         
         create_table_snowflake = SnowflakeOperator(
                 task_id = 'create_table_snowflake',
                 snowflake_conn_id= SNOWFLAKE_CONN_ID,
                 sql = '''DROP TABLE IF EXISTS Inventory_details;
                    CREATE TABLE IF NOT EXISTS Inventory_details(
                    trailer_type TEXT NOT NULL,
                    brand TEXT NOT NULL,
                    model TEXT NOT NULL,
                    age numeric NOT NULL ,
                    lcn_code TEXT NOT NULL
            )
             '''
         )
         
         copy_csv_into_snowflake = SnowflakeOperator(
                 task_id='copy_csv_into_snowflake',
                 snowflake_conn_id= SNOWFLAKE_CONN_ID,
                 sql = '''COPY INTO rvbs_db.rvbs_schema.Inventory_details from @rvbs_db.rvbs_schema.snowflake_stage FILE_FORMAT = csv_format
                 
                 '''

         )

         is_s3_file_available >> create_table_snowflake  >> copy_csv_into_snowflake



        