from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import \
    BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from dags.python_scripts.airflow_callback import callback

# Airflow Config
default_args = {
    'owner': 'farhan@data.com',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 31, 17),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [],
    'retries': 5,
    'retry_delay': timedelta(seconds=300),
    'on_failure_callback': callback
}

dag_id = "ingest_order"
dag = DAG(
    dag_id,
    default_args=default_args,
    schedule_interval='0 17 * * *',
    catchup=False,
    concurrency=50,
    max_active_runs=1
)

project_id = Variable.get('PROJECT_ID')
location = Variable.get('PROJECT_LOCATION')
target_project_id = Variable.get('TARGET_PROJECT_ID')

start_date = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").start_of("day")'
    '.in_timezone("UTC").date() }}'
)
end_date = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").date() }}'
)
run_date_ds_nodash = (
    '{{ execution_date'
    '.in_timezone("Asia/Jakarta").add(days=1).start_of("day")'
    '.in_timezone("UTC").date().format("YYYYMMDD") }}'
)
table_name = "orders"
target_table = f"{target_project_id}.source.{table_name}"
op = BigQueryExecuteQueryOperator(
    task_id='orders',
    sql='sql/orders.sql',
    destination_dataset_table=f'{target_table}${run_date_ds_nodash}',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    schema_update_options=['ALLOW_FIELD_ADDITION'],
    query_params=[
        {
            'name': 'start_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': start_date}
        },
        {
            'name': 'end_date',
            'parameterType': {'type': 'DATE'},
            'parameterValue': {'value': end_date}
        }
    ],
    use_legacy_sql=False,
    time_partitioning={
        'type': 'DAY',
        'field': 'order_date'
    },
    location=location,
    params={
        'project_id': target_project_id
    },
    dag=dag
)
orders_op = op
