from kubernetes.client import models as k8s
import airflow
from airflow.decorators import dag, task
from airflow.models import Variable

args = {
    'owner': 'tak',
}

@dag(
    dag_id='pod_test',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule=None,
)


def pod_custom():
    cpu_lim="500m"
    cpu_req=cpu_lim
    ram_lim="1000Mi"
    ram_req=ram_lim

    pod_config = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="I-am-rich",
                            resources=k8s.V1ResourceRequirements(
                                limits={"cpu": cpu_lim, "memory": ram_lim},
                                requests={"cpu": cpu_req, "memory": ram_req}
                            )
                        )
                    ]
                )
            )
        }
    
    @task(executor_config=pod_config)
    def hello():
        print("Hello!")

    hello()

pod_custom()