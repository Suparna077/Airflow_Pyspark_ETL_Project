''' Basic ETL DAG'''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    dag_id='basic_etl_dag',
    schedule_interval=None,
    start_date=datetime(2025,6,10),
    catchup=False) as dag:

    # The location of the top-level-domain-names.csv file has changed. Now, it is hosted in the same repository as this project.
    extract_task = BashOperator(
            task_id='extract_task',
            bash_command='wget -c https://raw.githubusercontent.com/LinkedInLearning/hands-on-introduction-data-engineering-4395021/main/data/top-level-domain-names.csv -O /home/airflowsup/ETL_PRJ_1/orchestrated/airflow-extract-data.csv'
            )

    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd.read_csv('/home/airflowsup/ETL_PRJ_1/orchestrated/airflow-extract-data.csv')
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime('%Y-%m-%d')
        generic_type_df.to_csv('/home/airflowsup/ETL_PRJ_1/orchestrated/airflow-transform-data.csv', index=False)

    transform_task = PythonOperator(
      task_id='transform_task',
      python_callable=transform_data,
      dag=dag)

    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import  /home/airflowsup/ETL_PRJ_1/orchestrated/airflow-transform-data.csv top_level_domains" | sqlite3 /home/airflowsup/ETL_PRJ_1/orchestrated/airflow-load-db.db',
        dag=dag
    )

    # extract_task >> transform_task >> load_task

