from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from extract_stocks import extract,transform,load

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('stock_data_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    
    # Tasks for AAPL (Repeat similarly for other stocks)
  symbols=['AAPL','TSLA','AMZN','ATW']   
  for symbol in symbols :
    # Extract step
    download_task = PythonOperator(
        task_id=f'download_{symbol}',
        python_callable=extract,
        op_args=[symbol],
        provide_context=True
    )
    
    # Transform step
    transform_task = PythonOperator(
        task_id=f'transform_{symbol}',
        python_callable=transform,
        op_args=[symbol],
        provide_context=True
    )
    
    # Load step
    load_task = PythonOperator(
        task_id=f'load_{symbol}',
        python_callable=load,
        op_args=[symbol],
        provide_context=True
    )

    # Define dependencies for AAPL
    download_task >> transform_task >> load_task