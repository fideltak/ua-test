import airflow
from airflow.decorators import dag, task
import os
from test01.modules import mod01
from common import utils

args = {
    'owner': 'tak',
}


@dag(
    dag_id='dag_depend',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule=None,
)
def dag_depend():
    # Dag Tasks
    @task
    def task01():
        def _do():
            mod01.hello()
            print("Task01")

        _do()

    @task
    def task02():
        def _do():
            utils.bye()
            print("Task02")

        _do()

    @task
    def task03():
        def _do():
            print("Task03")

        _do()

    task01() >> task02() >> task03()


dag_depend()