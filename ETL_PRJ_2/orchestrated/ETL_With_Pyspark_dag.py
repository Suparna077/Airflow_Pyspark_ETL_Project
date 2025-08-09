from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

with DAG(
    dag_id = "ETL_With_Pyspark_dag",
    start_date = datetime(2025,6,14),
    schedule_interval = None,
    catchup = False) as dag:

    Extract_Task = BashOperator(
        task_id = "Extract_Task",
        bash_command = "wget -c https://datahub.io/core/s-and-p-500-companies/r/constituents.csv -O /home/airflowsup/ETL_PRJ_2/orchestrated/airflow-extract-data_2.csv",
        dag = dag
    )


    PySpark_Task = SparkSubmitOperator(
    task_id="PySpark_Task",
    application="/home/airflowsup/ETL_PRJ_2/orchestrated/ETL_PRJ_2_PysparkScript.py",
    conn_id="spark_local",
    conf={
        "spark.master": "local",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.ui.enabled": "false"
        # "spark.ui.port": "18080",  # Pick a port not in use
        # "spark.port.maxRetries": "50"  # Optional: increase retries
    },
    verbose=True,
    dag=dag
)

    Load_Task = BashOperator(
        task_id="Load_Task",
        bash_command="""
                        awk 'FNR==1 && NR!=1{next} {print}' /home/airflowsup/ETL_PRJ_2/orchestrated/airflow-transform-data_2/part-*.csv > /home/airflowsup/ETL_PRJ_2/orchestrated/airflow-transform-data_2.csv && \
                        echo -e ".separator \",\"\n.import /home/airflowsup/ETL_PRJ_2/orchestrated/airflow-transform-data_2.csv sp500_companies" | sqlite3 /home/airflowsup/ETL_PRJ_2/orchestrated/airflow-load-db_2.db
                        """,
        dag=dag
    )

    def check_condition():
            # Your logic here
        return False  # or False

    short_circuit = ShortCircuitOperator(
        task_id='short_circuit',
        python_callable=check_condition,
        trigger_rule='all_done',
        dag=dag
        )



    Extract_Task >> PySpark_Task >>  Load_Task >> short_circuit  