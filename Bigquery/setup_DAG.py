import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import google.auth
import google.auth.transport.requests





credentials, project_id = google.auth.default(scopes=['https://storage.cloud.google.com/ashu_bucket/incremental.py'])
authed_session = google.auth.transport.requests.AuthorizedSession(credentials)
location = 'asia-south2' 


default_args = {
        'retries': 1,
        'owner' : 'airflow',
        'execution_timeout' : timedelta(seconds=300),
        'start_date' : airflow.utils.dates.days_ago(1)
}

# gcsfuse gs://ashu_bucket/ /path/to/mount/

# service = googleapiclient.discovery.build('storage', 'v1')


# #### List Bucket
# fields_to_return = \
#         'nextPageToken,items(name,size,contentType,metadata(my-key))'
# req = service.objects().list(bucket='ashu-bucket', fields=fields_to_return)
# resp = req.execute()

# ### Get Object 
# req = service.objects().get_media(bucket='ashu-bucket', object='incremental.py')

dag = DAG(
        dag_id='ashu-dag',
        default_args=default_args,
        #dagrun_timeout=timedelta(minutes=20),
        schedule_interval='@once', 
        catchup=False,
    )

# Step 4: Creating task
# Creating first task
start = DummyOperator(task_id = 'start',
                      dag = dag
                        )

# download = BashOperator(
#                 task_id = 'download',
#                 bash_command = 'gsutil -m cp -r  gs://ashu_bucket ~/',
#                 dag = dag)

# download1 = BashOperator(
#                 task_id = 'download1',
#                 bash_command = 'cd ashu_bucket',
#                 dag = dag)

# gsutil -m cp -r incremental.py gs://ashu_bucket ~/
# gcsfuse ashu-bucket /path/to/mount/

sql_to_bq = BashOperator(
                task_id = 'sql_to_bq',

                bash_command =  "gsutil cat gs://ashu_bucket/incremental.py",
                dag = dag) 
    
end = DummyOperator(task_id = 'end',
                     dag = dag)

start >> sql_to_bq >>end 
