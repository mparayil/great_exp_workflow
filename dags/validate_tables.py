import os
import sys
from datetime import datetime, timedelta
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + '/include/great_expectations'
sys.path.append(path)
from ge_prod.validate_expectations import validate_batch_data

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
	'owner': 'mparayil',
	'depends_on_past': False,
	'start_date': datetime(2020, 3, 9),
	'email': ["mparayil@agero.com"],
	'email_on_failure': True,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=2),
	'template_searchpath': [f'./include']
}

validate_dag = DAG(dag_id='validate_expectations_daily', default_args=default_args, schedule_interval="0 11 * * *",
                   catchup=False)

starting_validation = DummyOperator(task_id='starting_validation', dag=validate_dag)

provider_network = PythonOperator(task_id='validate_provider_network',
                             python_callable=validate_batch_data,
                             op_kwargs={'table_name': 'provider_network',
                                        'suite_name': 'warnings_dec2019'},
                             dag=validate_dag)

claim_requests = PythonOperator(task_id='validate_claim_requests', python_callable=validate_batch_data,
                           op_kwargs={'table_name': 'claim_requests',
                                      'suite_name': 'warnings_dec2019'}, dag=validate_dag)

surveys = PythonOperator(task_id='validate_surveys', python_callable=validate_batch_data,
                          op_kwargs={'table_name': 'surveys',
                                     'suite_name': 'warnings_2019Q3-Q4'},
                          dag=validate_dag)

complaint_cases = PythonOperator(task_id='validate_complaint_cases', python_callable=validate_batch_data,
                                 op_kwargs={'table_name': 'complaint_cases',
                                            'suite_name': 'warnings_2019Q4'},
                                 dag=validate_dag)


zipcodes = PythonOperator(task_id='validate_zipcodes', python_callable=validate_batch_data,
                                 op_kwargs={'table_name': 'zipcodes', 'suite_name': 'warnings'},
                                 dag=validate_dag)

finish_validation = DummyOperator(task_id='ending_validation', dag=validate_dag)

provider_network.set_upstream(starting_validation)
claim_requests.set_upstream(starting_validation)
surveys.set_upstream(starting_validation)
complaint_cases.set_upstream(starting_validation)
zipcodes.set_upstream(starting_validation)