from datetime import datetime, timedelta

from airflow import DAG

from providers.airlaunch.opcua.transfers.opcua_to_s3 import OPCUAToS3Operator
from providers.airlaunch.opcua.transfers.opcua_to_wasb import OPCUAToWasbOperator

from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

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

    deleteBlob = WasbDeleteBlobOperator(
        task_id='deleteBlob',
        wasb_conn_id="wasb",
        container_name='test',
        blob_name='opc_data.json',
        ignore_if_missing=True
    )
    
    deleteLocalBlob = WasbDeleteBlobOperator(
        task_id='deleteLocalBlob',
        wasb_conn_id="wasb",
        container_name='test',
        blob_name='local_file.csv',
        ignore_if_missing=True
    )

    opWasb = OPCUAToWasbOperator(
        task_id = "opcWasb",
        opcua_conn_id="opc",
        opcua_node="ns=3;i=1003",
        wasb_container="test",
        wasb_blob="opc_data.json",
        wasb_conn_id="wasb",
        upload_format="json",
        opcua_startdate="2021-12-09 20:00:00",
        opcua_enddate="2021-12-09 20:01:00",
        opcua_numvalues=10,
        queue="local",
    )
    
    opLocal = LocalFilesystemToWasbOperator(
        task_id='upload_local',
        file_path="/opt/airflow/local_file.csv",
        wasb_conn_id="wasb",
        blob_name='local_file.csv',
        container_name='test',
        queue='local'
    )

    [deleteBlob,deleteLocalBlob] >> [opWasb, opLocal]


    
