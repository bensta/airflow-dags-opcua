from datetime import datetime, timedelta

from airflow import DAG

from providers.airlaunch.opcua.transfers.opcua_to_s3 import OPCUAToS3Operator
from providers.airlaunch.opcua.transfers.opcua_to_wasb import OPCUAToWasbOperator

from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
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
    opS3 = OPCUAToS3Operator(
        task_id="opcS3",
        opcua_conn_id="opc",
        opcua_node="ns=3;i=1003",
        s3_bucket="afb",
        aws_conn_id="s3",
        opcua_startdate="2021-12-09 20:00:00",
        opcua_enddate="2021-12-09 20:01:00",
        opcua_numvalues=10,
        upload_format="json",
        s3_key="opc_file.json",
        replace=True,
        queue="local"
    )

#    sleep_remote = BashOperator(
#        task_id='sleep_remote',
#        bash_command='sleep 300'
#    )
#
#    sleep_local = BashOperator(
#        task_id='sleep_local',
#        bash_command='sleep 300',
#        queue='local'
#    )

    deleteBlob = WasbDeleteBlobOperator(
        task_id='deleteBlob',
        wasb_conn_id="wasb",
        container_name='test',
        blob_name='opc_data.json',
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



    
