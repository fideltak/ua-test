import airflow
from airflow.decorators import dag, task
from airflow.models import Variable
from dag_test import dag_test
import os

file_path = Variable.get("file_path",default_var="/tmp/test.txt")


args = {
    'owner': 'tak',
}

@dag(
    dag_id='dag_sample',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule=None,
)


def dag_sample():
    @task
    @dag_test('task01')
    def task01():
        print("Dag Task: Creating a file")
        with open(file_path, 'w') as f:
            f.write('Hello!')
        print("Created a file: {}".format(file_path))

    @task
    @dag_test('task02')
    def task02():
        print("Dag Task: Creating file and Remove it")
        with open(file_path, 'w') as f:
            f.write('Hello!')
        os.remove(file_path)
        
    @task
    @dag_test('dummy')
    def dummy():
        print("Dag Task: Dummy task")
        print("This is dummy")

    task01()>>task02()>>dummy()

dag_sample()