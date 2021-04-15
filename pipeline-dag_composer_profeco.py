from datetime import datetime,timedelta , date 
from airflow import models,DAG 
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataProcPySparkOperator,DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators import BashOperator 
from airflow.models import *
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
import time

BUCKET = "gs://gnp-storage"

PROJECT_ID = "dataengineer-bigdata"

PYSPARK_JOB = BUCKET + "/Scripts/profeco_etl.py"

DEFAULT_DAG_ARGS = {
    'owner':"airflow",
    'depends_on_past' : False,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5),
    "project_id":PROJECT_ID,
    "scheduled_interval":"30 7 * * *"
}

with DAG("gnp-etl", default_args=DEFAULT_DAG_ARGS) as dag:
    
    unzip_files = BashOperator(
        task_id = "unzip_file",
        bash_command="gcloud dataflow jobs run UNZIP-FILE --region=us-central1 --gcs-location gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files --parameters inputFilePattern=gs://gnp-storage/Profeco/zip/*.zip,outputDirectory=gs://gnp-storage/Profeco/resources/Sin-fecha,\outputFailureFile=gs://gnp-storage/Profeco/resources/Sin-fecha/failure"
    )

    PythonOperator = PythonOperator(task_id="delay_python_task",
        python_callable=lambda: time.sleep(480))

    create_cluster = DataprocClusterCreateOperator(

        task_id ="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        master_disk_size=50,
        worker_machine_type="n1-standard-1",
        worker_disk_size=50,
        num_workers=2,
        region="us-east1",
        zone="us-east1-b",
        init_actions_uris=['gs://dataproc-initialization-actions/python/pip-install.sh'],
        optional_components=["ANACONDA"],
        metadata={'PIP_PACKAGES': 'google-cloud-storage'},
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id = "run_pyspark_etl",
        main = PYSPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1"
    )

    bq_load_profeco_data = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_csv_profeco",
        bucket='gnp-storage',                                      
        source_objects=["Profeco/resources/Sin-fecha/profeco.pdf"],
        destination_project_dataset_table=PROJECT_ID+".GNP.Profeco_table",
        autodetect = True,
        source_format="CSV",
        field_delimiter=',',
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    delete_cluster = DataprocClusterDeleteOperator(

        task_id ="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1",
        trigger_rule = TriggerRule.ALL_DONE
    )


    unzip_files.dag = dag

    unzip_files.set_downstream(create_cluster)

    create_cluster.set_downstream(PythonOperator)

    PythonOperator.set_downstream([submit_pyspark,bq_load_profeco_data])

    submit_pyspark.set_downstream(delete_cluster)
    