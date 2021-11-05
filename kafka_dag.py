from datetime import datetime , timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

home_folder = '/Users/jiahaotan/airflow/dags/'


default_args = {
		'start_date': datetime(2021,11,3,1,41,00),
		'retries': 1,
		'retry_delay': timedelta(minutes=1)
	}

with DAG(
	'kafka_dag',
	default_args = default_args,
	schedule_interval = timedelta(minutes=1),
	catchup = False
	) as dag:
		t1 = BashOperator(
			task_id = 'ltaProducer',
			bash_command = 'python3 {}tasks/mongoConsumer.py'.format(home_folder))

		t2 = BashOperator(
			task_id = 'mongodbConsumer',
			bash_command = 'python3 {}tasks/ltaProducer.py'.format(home_folder))

		t1,t2