import airflow
from airflow.decorators import dag, task
from airflow.models import Variable
from dag_test import unit_test, task_test
import os

file_path = Variable.get("file_path", default_var="/tmp/test.txt")

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

    # Dag Task Methods
    @unit_test('create_file')  # Test for method
    def create_file():
        print("Dag Task: Creating a file")
        with open(file_path, 'w') as f:
            f.write('Hello!')
        print("Created a file: {}".format(file_path))

    @unit_test('create_file_and_remove')  # Test for method
    def create_file_and_remove():
        create_file()
        os.remove(file_path)

    @unit_test('dummy01')  # Test for method
    def dummy01():
        print("Dag Task: Dummy task")
        print("This is dummy")

    # Dag Tasks
    @task
    def task01():

        @task_test('task01')  # Test for Dag Task
        def _do():
            create_file()

        _do()

    @task
    def task02():

        @task_test('task02')  # Test for Dag Task
        def _do():
            create_file_and_remove()

        _do()

    @task
    def dummy01_task():

        @task_test('dummy01_task')  # Test for Dag Task
        def _do():
            dummy01()

        _do()

    task01() >> task02() >> dummy01_task()


dag_sample()
