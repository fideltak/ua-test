import airflow
from airflow.decorators import dag, task
from airflow.models import Variable
import os

file_path = Variable.get("file_path", default_var="/tmp/test.txt")

args = {
    'owner': 'tak',
}

@dag(
    dag_id='etl_sample',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule=None,
)
def etl_sample():

    # Dag Tasks
    @task
    def extract_phase():
        def _do():
            print("Extract")

        _do()

    @task
    def transform_phase():
        def _do():
            print("Transform")
        _do()

    @task
    def load_phase():
        def _do():
            print("Load")
        _do()

    extract_phase() >> transform_phase() >> load_phase()

etl_sample()
