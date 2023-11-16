import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from pip import _internal

args = {
    'owner': 'tak',
}

dag = DAG(
    dag_id='pip',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None,
)


def get_pip_list():
    p_list = _internal.main(['list'])
    print(p_list)
    return p_list


run_this = PythonOperator(
    task_id='execute_pip_list',
    provide_context=True,
    python_callable=get_pip_list,
    dag=dag,
)

run_this
