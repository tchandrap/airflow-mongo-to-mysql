from datetime import datetime, timedelta
import airflow
from airflow import DAG
from mongo_to_mysql_operator import MongoToMySqlOperator

dag = DAG(
    dag_id="job_trial_user",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="@daily",
    concurrency=100
)

start = MongoToMySqlOperator(
    task_id=f"extract_vaccine_daily_to_mysql",
    mongo_collection='vaccine',
    mongo_database='telkom',
    mongo_query="""db.vaccine.find({$and: [{"updatedAt": {$gte: {{ yesterday_ds }} }}, {"updatedAt": {$lte:  {{ ds }} }}]})""",
    target_table='pubcic.mongo_vaccine',
    mongo_conn_id='mongo_default',
    mysql_conn_id='mysql_default', 
    dag=dag,
)