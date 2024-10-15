import os
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

# Fetch data and save it as CSV (stub function - replace with your ServiceNow fetching logic)
def fetch_data_from_servicenow(table_name, **kwargs):
    # This function should fetch data from ServiceNow and return the path to the CSV file.
    # You would add your ServiceNow fetching logic here and return the path of the CSV file.

    file_path = f'/tmp/{table_name}.csv'
    
    # Example data - Replace this with actual fetched data
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['sample1', 'sample2', 'sample3']
    })
    
    # Save to CSV
    df.to_csv(file_path, index=False)
    
    return file_path

# Load data into Snowflake stage using SnowflakeHook
def load_to_snowflake_stage(table_name, file_path, **kwargs):
    # Use SnowflakeHook to connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Upload the CSV to Snowflake's staging area
    stage_path = f'@my_snowflake_stage/{table_name}.csv'
    cursor.execute(f"PUT file://{file_path} {stage_path} OVERWRITE = TRUE")
    conn.commit()

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Create the DAG
with DAG('dynamic_servicenow_to_snowflake_parallel_with_start_end',
         default_args=default_args,
         schedule_interval='@daily',  # Adjust the schedule interval as needed
         catchup=False) as dag:

    # Start task
    start_task = DummyOperator(
        task_id='start_task',
        dag=dag
    )

    # Read the CSV to get the list of tables
    csv_file = '/path/to/servicenow_to_snow.csv'
    table_df = pd.read_csv(csv_file)

    # TaskGroup for organizing parallel tasks
    with TaskGroup('fetch_and_load_tasks', tooltip='Fetch and Load Tables in Parallel') as fetch_and_load_group:
        
        for index, row in table_df.iterrows():
            table_name = row['table_name']  # Assuming 'table_name' column exists in the CSV
            
            # Task 1: Fetch data for each table
            fetch_task = PythonOperator(
                   =f'fetch_data_from_{table_name}',
                python_callable=fetch_data_from_servicenow,
                op_kwargs={'table_name': table_name},
                provide_context=True
            )

            # Task 2: Load data into Snowflake for each table
            load_task = PythonOperator(
                task_id=f'load_data_to_snowflake_{table_name}',
                python_callable=load_to_snowflake_stage,
                op_kwargs={'table_name': table_name, 'file_path': f'/tmp/{table_name}.csv'},
                provide_context=True
            )

            # Set task dependencies within the TaskGroup
            fetch_task >> load_task

    # End task
    end_task = DummyOperator(
        task_id='end_task',
        dag=dag
    )

    # Set the dependencies: start -> fetch_and_load_group -> end
    start_task >> fetch_and_load_group >> end_task
