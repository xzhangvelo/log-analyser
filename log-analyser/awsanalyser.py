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
from plotly.subplots import make_subplots
import pandas as pd
import argparse

client = boto3.client('logs')
arg_parser = argparse.ArgumentParser(description='AWS Logs Analyser')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)])


def camel_case_split(str):
    return re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', str)


def get_logs(group_name, start_time, end_time):
    query = "fields @timestamp, @logStream, @message " \
            "| filter @message like /Started/ " \
            "| parse @message \"Started * in * seconds (JVM running for *)\" as appName, appStartTime, jvmStartTime" \
            "| sort @timestamp desc " \
            "| limit 2000"

    start_query_response = client.start_query(
        logGroupName=group_name,
        startTime=int(start_time.timestamp()),
        endTime=int(end_time.timestamp()),
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
    grouped_data_mean = data.groupby('Service Name').mean().reset_index()
    pprint(grouped_data_mean)

    fig = make_subplots(shared_yaxes=True)

    fig.add_trace(
        go.Scatter(x=data['Service Name'], y=data['Startup time'],
                   mode='markers',
                   name='Startup Time (seconds)')
    )

    fig.add_trace(
        go.Scatter(x=grouped_data_mean['Service Name'], y=grouped_data_mean['Startup time'],
                   mode="markers",
                   name="Startup Time Mean (seconds)")
    )

    fig.update_layout(
        title_text='Spring Microservices Startup Time Chart (Environment: {})'.format(env_name)
    )

    fig.update_xaxes(title_text="Microservices names")

    fig.show()


def format_for_file_name(value):
    return value.replace('.', '_').replace('/', '_').replace('-', '_').replace(':', '_')


def output_data_to_csv(env_name, start_time, end_time, data):
    csv_reports = ["Service Name,Startup time,JVM Running Time"]
    for entry in data:
        csv_reports.append('{},{},{}'.format(entry['name'], entry['appStart'], entry['jvmStart']))
    pprint(csv_reports)
    csv_file_name = "{}_from_{}_to_{}.csv".format(format_for_file_name(env_name),
                                                  format_for_file_name(start_time.isoformat()),
                                                  format_for_file_name(end_time.isoformat()))
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
    arg_parser.add_argument("--env", required=True, help="AWS Log Group Name")
    arg_parser.add_argument("--start", required=True, help="Start time (ISO-8601 format)")
    arg_parser.add_argument("--end", required=True, help="End time (ISO-8601 format)")
    args = arg_parser.parse_args()

    env_name = args.env
    start_time = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end_time = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")
    logging.info(
        'Analysing logs for AWS log group {} between {} and {}'.format(env_name,
                                                                       start_time.isoformat(),
                                                                       end_time.isoformat()))
    results = get_logs(env_name, start_time, end_time)
    file_name = output_data_to_csv(env_name, start_time, end_time, results)
    show_graph(env_name, file_name)


main(sys.argv)
