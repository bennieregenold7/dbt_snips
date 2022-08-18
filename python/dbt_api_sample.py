from ast import AsyncFunctionDef
from nntplib import ArticleInfo
import os
import requests
import json
import csv
import pandas as pd
import datetime

# provide account ID
account_id = 51798

# Save your API key for dbt CLoud to an environment 
# variable named DBT_CLOUD_API_USER_TOKEN 

base_url = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}'


def call_api(url):

    API_KEY = os.getenv('DBT_CLOUD_API_USER_TOKEN')
    HEADERS = {'Authorization': 'Token '+ API_KEY + ''}

    response = requests.get(url, headers= HEADERS)
    response.raise_for_status()
    response.close()
        
    return response


def string_to_date(date_string:str):

    if 'T' in date_string:
        proper_date = datetime.datetime.strptime(date_string[:-7], '%Y-%m-%dT%H:%M:%S.%f')
    else: 
        proper_date = datetime.datetime.strptime(date_string[:-7], '%Y-%m-%d %H:%M:%S.%f')

    return proper_date


def get_run_details():

    run_url = base_url + '/runs?order_by=-id'
    run_results = call_api(run_url)

    for result in run_results['data']:
        run_id = result['id']
        run_status = result['status']
        started_at = string_to_date(result['started_at'])
        finished_at = string_to_date(result['finished_at'])
        execution_time = finished_at - started_at
        print(f'run {run_id} started at {started_at}, ran for {execution_time}, and had a status of {run_status}')


def get_run_artifact(run_id: int, artifact_name: str):

    artifact_url = base_url + f'/runs/{run_id}/artifacts/{artifact_name}'
    artifact_results = call_api(artifact_url)

    return artifact_results

## Once you get the artifact, you can handle it a few ways:

    ## display the json content in a variable:
        ## artifact.json()

    ## write to json file with this:
        ## with open('catalog.json', 'w') as file:
            ## file.write(json.dumps(artifact.json()))

    ## write to html file with this:
        ## with open('index.html', 'w') as file:
            ## file.wrtie(artifact.content.decode())

    


def list_jobs():

    job_url = base_url + '/jobs/'
    job_results = call_api(job_url)

    for job in job_results['data']:
        job_name = job['name']
        job_created_at = string_to_date(job['created_at'])
        print(f'{job_name} was created at {job_created_at}')
