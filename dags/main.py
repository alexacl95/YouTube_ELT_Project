from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

# define local timezone
local_tz = pendulum.timezone("America/Bogota")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "eamil": "alexacl95@gmail.com",
    # "retries": 1,
    # "retry_delay":timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026,11,3, tzinfo=local_tz),
    # "end_date": datetime(2026,12,31, tzinfo=local_tz)
}

with DAG(
    dag_id = 'produce_json',
    default_args = default_args,
    description = "DAG to produce JSON file with raw data",
    schedule = "0 10 * * *", #cron expression
    catchup = False
) as dag:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    #Define dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task