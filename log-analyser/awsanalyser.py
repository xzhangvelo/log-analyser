#!/usr/bin/python

import boto3
import logging
import sys
from datetime import datetime, timedelta
import time
from pprint import pprint
import re
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

client = boto3.client('logs')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)])


def camel_case_split(str):
    return re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', str)


def get_logs(group_name):
    query = "fields @timestamp, @logStream, @message " \
            "| filter @message like /Started/ " \
            "| parse @message \"Started * in * seconds (JVM running for *)\" as appName, appStartTime, jvmStartTime" \
            "| sort @timestamp desc " \
            "| limit 2000"

    start_query_response = client.start_query(
        logGroupName=group_name,
        startTime=int((datetime.today() - timedelta(days=3)).timestamp()),
        endTime=int((datetime.today() - timedelta(days=2)).timestamp()),
        queryString=query
    )

    query_id = start_query_response['queryId']
    response = None
    while response is None or response['status'] == 'Running':
        print('Waiting for query to complete ...')
        time.sleep(2)
        response = client.get_query_results(
            queryId=query_id
        )

    results = []
    for entry in response['results']:
        result = {}
        for field in entry:
            if field['field'] == 'appName':
                split_app_name = camel_case_split(field['value'].replace('Application', '').replace('Service', ''))
                result['name'] = ' '.join(split_app_name)
            elif field['field'] == 'appStartTime':
                result['appStart'] = field['value']
            elif field['field'] == 'jvmStartTime':
                result['jvmStart'] = field['value']
        if 'name' in result:
            results.append(result)
        else:
            logging.debug(entry)
    return results


def show_graph(env_name, file_name):
    logging.info("Generating graph from {}...".format(file_name))
    data = pd.read_csv(file_name)

    fig = px.scatter(x=data['Service Name'], y=data['Startup time'],
                     labels={'x': 'Service Names', 'y': 'Startup Time (seconds)'},
                     title='Spring Microservices Startup Time Chart (Environment: {})'.format(env_name))
    fig.show()


def print_csv(env_name, data):
    csv_reports = ["Service Name,Startup time,JVM Running Time"]
    for entry in data:
        csv_reports.append('{},{},{}'.format(entry['name'], entry['appStart'], entry['jvmStart']))
    pprint(csv_reports)
    csv_file_name = "{}_{}.csv".format(env_name.replace('.', '_').replace('/', '_'), int(datetime.today().timestamp()))
    output_csv_file(csv_file_name, csv_reports)
    return csv_file_name


def output_csv_file(filename, lines):
    output_file = open(filename, "w")
    for line in lines:
        output_file.write(line)
        output_file.write('\n')
    output_file.close()


def print_pretty(data):
    for entry in data:
        print('{} is started in {} seconds and the JVM is stared in {} seconds'.format(entry['name'],
                                                                                       entry['appStart'],
                                                                                       entry['jvmStart']))


def main(argv):
    env_name = argv[1]
    results = get_logs(env_name)
    file_name = print_csv(env_name, results)
    show_graph(file_name)


main(sys.argv)
