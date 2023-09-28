import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from ftplib import FTP

#ip_addr = Variable.get("ftp_ip_addr", default_var="10.7.16.51")
#ftp_user = Variable.get("ftp_user", default_var="tak")
#ftp_user_password = Variable.get("ftp_user_password", default_var="password")

ip_addr = "10.7.16.51"
ftp_user = "tak"
ftp_user_password = "password"

args = {
    'owner': 'tak',
}

dag = DAG(
    dag_id='ftp',
    default_args=args,
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None,
)


def ftp_list_data():
    ftp = FTP(host=ip_addr, user=ftp_user, passwd=ftp_user_password)
    ftp.login()
    print(ftp.retrlines('LIST'))
    ftp.quit()
    return 'Success'


run_this = PythonOperator(
    task_id='execute_ftp_pull',
    provide_context=True,
    python_callable=ftp_list_data,
    dag=dag,
)

run_this
