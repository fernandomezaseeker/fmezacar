#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""DAG que ejecuta procedimientos almacenados de bigquery.
Imprime datos en el log de la tarea.
"""

from __future__ import annotations
from airflow import DAG
from pathlib import Path
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator
)

PROJECT_ID = 'cc-data-analytics-prd'
REGION = 'us-central1'
REPOSITORY_ID = 'dataform-code'
WORKSPACE_ID = 'luissalazar'

START_DATE = days_ago(30)

AIRFLOW_TAGS = ['DATAFROM','TEST','BIGQUERY', 'LUIS SALAZAR']

# SCHEDULE_INTERVAL = '5 12 * * *'
SCHEDULE_INTERVAL = None

default_args = {
    'owner': 'CUERVO-IT',
    'email': 'luis.salazar@it-seekers.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 0,
    'depends_on_past': False,
    'pool': 'general'
}

with DAG(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    tags=AIRFLOW_TAGS,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False
    ) as dag:
    # Tareas decorativas
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    # Create Compilation Result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create-compilation-result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                f"workspaces/{WORKSPACE_ID}"
            ),
        },
    )
    # Create Workflow Invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )
    start >> create_compilation_result >> create_workflow_invocation >> end