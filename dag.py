from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

from airflow.operators.python import PythonOperator

def cloud_op(wasb_conn_id: str, blob_name: str, container_name: str):
    wasb_hook = WasbHook(wasb_conn_id=wasb_conn_id)
    file_content = wasb_hook.read_file(container_name=container_name, blob_name=blob_name)
    print("Hello from the Cloud! I got the local file, the content is: {}".format(file_content))



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'opc',
    default_args=default_args,
    description='S3 List',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    opLocal = LocalFilesystemToWasbOperator(
        task_id='upload_local',
        file_path="/opt/airflow/local_file.csv",
        wasb_conn_id="wasb",
        blob_name='local_file.csv',
        container_name='test',
        queue='local'
    )

    opRemote = PythonOperator(
        task_id='cloud_op',
        python_callable=cloud_op,
        op_kwargs={
            "wasb_conn_id": "wasb",
            "blob_name": "local_file.csv",
            "container_name": "test",
        }
    )
    
    deleteLocalBlob = WasbDeleteBlobOperator(
        task_id='deleteLocalBlob',
        wasb_conn_id="wasb",
        container_name='test',
        blob_name='local_file.csv',
        ignore_if_missing=True
    )

    deleteLocalBlob >> opLocal >> opRemote


    
