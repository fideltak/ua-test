from kubernetes.client import models as k8s
import airflow
from airflow.decorators import dag, task
from airflow.models import Variable

args = {
    'owner': 'tak',
}

@dag(
    dag_id='k8s_test',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule=None,
)

def k8s_custom():
    k8s_config = {"pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))}
    
    @task(executor_config=k8s_config)
    def hello():
        print("Hello!")

    hello()

k8s_custom()