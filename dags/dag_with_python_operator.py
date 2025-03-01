from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'daniel',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def print_hello_daniel_1():
    print("task 1: Hello Daniel")
    
def print_hello_daniel_2():
    print("task 2: Hello Daniel")
    
def get_name():
    return 'Daniel'

def print_hello_name(task_instance):
    output = task_instance.xcom_pull(task_ids='task1')
    print(f'Hello {output}')
    


with DAG(
    dag_id='python_dag_1',
    default_args=default_args,
    description='my first python dag',
    start_date=datetime(2024,1,7),
    schedule_interval='@daily'

) as dag:
    # Define tasks
   
   task1 = PythonOperator(
       task_id='task1',
       python_callable=get_name
   )
   
   task2 = PythonOperator(
       task_id='task2',
       python_callable=print_hello_name
   )
   
#    task3 = PythonOperator(
#         task_id='task3',
#         python_callable=print_hello_name,
#         op_kwargs={'name':'Tupac'}
#     )
   
#    task4 = PythonOperator(
#        task_id='task4',
#        python_callable=get_name
#    )
   
   task1 >> task2
   
   