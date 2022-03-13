from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from weather_api import weather_api_method


defalut_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
# creating dag with some task
with DAG("Weather_Dag",default_args=defalut_args,schedule_interval='* 18 * * *',template_searchpath=['/usr/local/airflow/sql_files'],catchup=False) as dag:
    # using python operator we call weather api method to store data in csv file
    t1 = PythonOperator(task_id="check_file_exist_or_create_new_file",python_callable=weather_api_method)
    # using postgreoperator we are creating a new table here
    t2 = PostgresOperator(task_id="create_new_table",postgres_conn_id='postgres_conn',sql="create_new_table.sql")
    #using postgresoperator we are inserting data from csv file to  postgresql table which is created in 2nd task
    t3 = PostgresOperator(task_id="insert_data_into_table",postgres_conn_id='postgres_conn',sql="copy weather FROM '/store_files_postgresql/weather_data.csv' DELIMITER ',' CSV HEADER;")
    t1>>t2>>t3