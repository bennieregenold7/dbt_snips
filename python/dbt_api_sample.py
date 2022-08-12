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

    results = response.json()
        
    return results


def get_run_details():

    run_url = base_url + '/runs?order_by=-id'
    run_results = call_api(run_url)

    for result in run_results['data']:
        run_id = result['id']
        run_status = result['status']
        started_at = datetime.datetime.strptime(result['started_at'][:-7], '%Y-%m-%d %H:%M:%S.%f')
        finished_at = datetime.datetime.strptime(result['finished_at'][:-7], '%Y-%m-%d %H:%M:%S.%f')
        execution_time = finished_at - started_at
        print(f'run {run_id} started at {started_at}, ran for {execution_time}, and had a status of {run_status}')




