import json
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

## Sets default arguments for the Airflow pipeline
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(0),
  'email': ['airflow@example.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=15)
}

## Establishes the variables for the dev data prep environment.
## These headers and endpoints are used as part of the export flow call. 
headers_dev = {
  "Content-Type": "application/json",
  "Authorization": Variable.get("trifacta_bearer_dev")
}
dev_flow_id = Variable.get("flow_id")
dev_endpoint = "/v4/flows/" + dev_flow_id + "/package"
zname = f'/home/airflow/gcs/data/flow_{dev_flow_id}_export.zip'

## Establishes the variables for the prod data prep environment.
headers_prod = {
  "Content-Type": "application/json",
  "Authorization": Variable.get("trifacta_bearer_dev")
}
prod_new_dataset_id = Variable.get("new_dataset_id")


DAG_NAME = 'export_flow_move_to_prod3'

## Python task to invoke the HttpHook operator and store the resulting zip file in GCS.
## API documentation: https://api.trifacta.com/dataprep-premium/index.html#operation/getFlowPackage
def download_flow_zip(**context):

    api_hook = HttpHook(
        method='GET',
        http_conn_id='dataprep_api'
    )

    api_response = api_hook.run(
        endpoint = dev_endpoint,
        headers = headers_dev,
    )
	
    flow_zip = open(zname, 'wb')
    flow_zip.write(api_response.content)
    flow_zip.close()

## Python task to compare the results from get_datasource_list with the result from get_flowedges and get_flownodes and identify the connected recipe.
def identify_recipe(**kwargs):

  parsingRecipe_id = json.loads(kwargs['ti'].xcom_pull(key='return_value', task_ids='get_datasource_list'))["data"][0]["parsingRecipe"]["id"]
  flownodes_json = json.loads(kwargs['ti'].xcom_pull(key='return_value', task_ids='get_flownodes'))["flowNodes"]["data"]
  flowedges_json = json.loads(kwargs['ti'].xcom_pull(key='return_value', task_ids='get_flowedges'))["flowEdges"]["data"]
  
  input_node = None
  recipe_id = None

  for item in flownodes_json:
    if item["recipe"]["id"] == parsingRecipe_id:
      input_node = item["id"]
      break

  for item in flowedges_json:
    if item["inputFlowNode"]["id"] == input_node:
      recipe_id = item["outputFlowNode"]["id"]
      break

  kwargs['ti'].xcom_push(key="recipe_id",value=recipe_id)



## Invoke the DAG
with DAG(DAG_NAME, default_args=default_args, catchup=False, schedule_interval=None, user_defined_macros={ 'json': json }) as dag:

## Task calls the API to import the flow stored in GCS to the prod data prep environment
## API documentation: https://api.trifacta.com/dataprep-premium/index.html#operation/importPackage	
  import_flow_task = BashOperator(
	    task_id="import_flow_to_prod",
	    bash_command="curl -X POST https://api.clouddataprep.com/v4/flows/package -H 'authorization: {{ params.PROD_BEARER }}' -H 'content-type: multipart/form-data' -F '{{ params.PROD_PATH }}'",
      params={
        "PROD_BEARER": Variable.get("trifacta_bearer_prod"),
        "PROD_PATH": f"data=@{zname}"
        },
      xcom_push=True,
      dag=dag
	  )

## Task calls the API to retrieve the datasources for a flow, and pushes to Xcom the ID for the first input dataset
## API documentation: https://api.trifacta.com/dataprep-premium/index.html#operation/getFlowInputs
  get_datasource_list = SimpleHttpOperator(
    http_conn_id='dataprep_api',
    method='GET',
    task_id="get_datasource_list",
    endpoint='/v4/flows/{{ json.loads(ti.xcom_pull(task_ids="import_flow_to_prod"))["primaryFlowIds"][0] }}/inputs',
    headers = headers_prod,
    xcom_push=True,
    log_response=True,
    dag=dag
    )

## Task calls the API to retrieve the list of edges for a flow. Flowedges are used to connect imported datasets to recipes.
## API documentation: https://api.trifacta.com/dataprep-premium/index.html#operation/listFlows
## Information about the embedded flowedges call: https://api.trifacta.com/dataprep-premium/index.html#section/Overview/Embedding-Resources
  get_flowedges = SimpleHttpOperator(
    http_conn_id='dataprep_api',
    method='GET',
    task_id="get_flowedges",
    endpoint='/v4/flows/{{ json.loads(ti.xcom_pull(task_ids="import_flow_to_prod"))["primaryFlowIds"][0] }}?embed=flowEdges',
    headers = headers_prod,
    xcom_push=True,
    log_response=True,
    dag=dag
    )

## Task calls the API to retrieve the list of nodes for a flow. Flownodes are used to connect imported datasets to recipes.
## API documentation: https://api.trifacta.com/dataprep-premium/index.html#operation/listFlows
## Information about the embedded flownodes call: https://api.trifacta.com/dataprep-premium/index.html#section/Overview/Embedding-Resources
  get_flownodes = SimpleHttpOperator(
    http_conn_id='dataprep_api',
    method='GET',
    task_id="get_flownodes",
    endpoint='/v4/flows/{{ json.loads(ti.xcom_pull(task_ids="import_flow_to_prod"))["primaryFlowIds"][0] }}?embed=flownodes',
    headers = headers_prod,
    xcom_push=True,
    log_response=True,
    dag=dag
    )

## Task calls the API to replace the input dataset for an identified recipe
## API documentation: https://api.trifacta.com/dataprep-premium/index.html#operation/updateInputDataset
  swap_input_dataset = SimpleHttpOperator(
    http_conn_id='dataprep_api',
    method='PUT',
    task_id="swap_input_dataset",
    endpoint='/v4/wrangledDatasets/{{ ti.xcom_pull(key="recipe_id", task_ids="identify_recipe") }}/primaryInputDataset',
    headers = headers_prod,
    data = json.dumps({"importedDataset": {"id": int(prod_new_dataset_id)}}),
    xcom_push=True,
    log_response=True,
    dag=dag
    )


## Sequence of tasks to execute in the DAG
  [PythonOperator(
		task_id='download_flow_zip_task',
		python_callable = download_flow_zip,
		provide_context = True
		)] \
  >> import_flow_task >> get_datasource_list >> get_flowedges >> get_flownodes >> \
  [PythonOperator(
    task_id='identify_recipe',
    python_callable = identify_recipe,
    provide_context = True
    )] \
  >> swap_input_dataset
