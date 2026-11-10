import logging
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)
SODA_PATH = "/opt/airflow/include/soda"

def yt_elt_data_quality(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command = f"soda contract verify -ds /opt/airflow/include/soda/configuration.yml -c /opt/airflow/include/soda/checks.yml --set DATASET={schema} -v"
        )
        return task
    except Exception as e:
        logger.error(f"Error running data quality check for schema: {schema}")
        raise e
    
