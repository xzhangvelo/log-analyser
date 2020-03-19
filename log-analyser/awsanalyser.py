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


def get_logs(query, group_name, start_time, end_time):
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
        time.sleep(5)
        response = client.get_query_results(
            queryId=query_id
        )
    return response


def get_log_stream_names(group_name, service_name, start_time, end_time):
    log_streams = []
    next_token = None
    while True:
        if next_token is None:
            response = client.describe_log_streams(
                logGroupName=group_name,
                logStreamNamePrefix=service_name,
                limit=50
            )
        else:
            response = client.describe_log_streams(
                logGroupName=group_name,
                logStreamNamePrefix=service_name,
                nextToken=next_token,
                limit=50
            )
        for log_stream in response['logStreams']:
            log_streams.append(log_stream)
        if 'nextToken' in response:
            next_token = response['nextToken']
        else:
            break
    log_stream_names = []
    for log_stream in log_streams:
        last_event_timestamp = datetime.fromtimestamp(int(log_stream['lastEventTimestamp']) / 1000)
        first_event_timestamp = datetime.fromtimestamp(int(log_stream['firstEventTimestamp']) / 1000)
        if first_event_timestamp >= start_time and last_event_timestamp <= end_time:
            print('{} {}'.format(first_event_timestamp, last_event_timestamp))
            log_stream_names.append(log_stream['logStreamName'])
    pprint(log_stream_names)
    pprint(len(log_stream_names))
    return log_stream_names


def get_startup_logs_for_service(group_name, service_name, start_time, end_time):
    log_stream_names = get_log_stream_names(group_name, service_name, start_time, end_time)
    keywords = [
        'The following profiles are active',
        'Flyway Community Edition',
        'HHH000412: Hibernate Core',
        'Producer configuration:',
        'Started ',
        'Ensured that spring events are handled',
        'Initialized JPA',
        'Tomcat initialized',
    ]
    for log_stream_name in log_stream_names:
        query = "fields @timestamp, @logStream, @message " \
                "| filter @logStream like /{}/" \
                "| filter @message like /{}/" \
                "| parse @message \"*  * *] *    : *\" as ts, level, prefixes, class_name, details" \
                "| sort @timestamp desc " \
                "| limit 100".format(log_stream_name, '|'.join(keywords))

        response = get_logs(query, group_name, start_time, end_time)

        messages = []
        for entry in response['results']:
            message = {}
            for field in entry:
                if field['field'] == 'details':
                    message['message'] = field['value']
                elif field['field'] == 'ts':
                    message['timestamp'] = field['value']
            if message:
                messages.append(message)

        for e in messages:
            print(e)
        print('----------------------------------')
        print(len(messages))


def analyse_startup_stages(messages):
    # ts = '2020-03-17 12:20:03.633'
    ts_formatter_str = "%Y-%m-%d %H:%M:%S.%f"
    app_start_ts = None
    app_end_ts = None
    flyway_start_ts = None
    kafka_start_ts = None
    kafka_end_ts = None
    flyway_end_ts = None
    hibernate_start_ts = None
    for message in messages:
        if 'The following profiles are active' in message['message']:
            app_start_ts = datetime.strptime(message['timestamp'], ts_formatter_str)
        if 'Flyway Community Edition' in message['message']:
            flyway_start_ts = datetime.strptime(message['timestamp'], ts_formatter_str)
        if 'HHH000412: Hibernate Core' in message['message']:
            hibernate_start_ts = datetime.strptime(message['timestamp'], ts_formatter_str)
        if 'Schema ' in message['message']:
            flyway_end_ts = datetime.strptime(message['timestamp'], ts_formatter_str)
        if 'Ensured that spring events are handled' in message['message']:
            kafka_end_ts = datetime.strptime(message['timestamp'], ts_formatter_str)
        if 'Producer configuration:' in message['message']:
            kafka_start_ts = datetime.strptime(message['timestamp'], ts_formatter_str)
        if 'Started ' in message['message']:
            app_end_ts = datetime.strptime(message['timestamp'], ts_formatter_str)

    result = {}
    if kafka_end_ts is not None and kafka_start_ts is not None:
        result['kafka'] = kafka_end_ts - kafka_start_ts
    if app_end_ts is not None and app_start_ts is not None:
        result['app'] = app_end_ts - app_start_ts
    if hibernate_start_ts is not None and flyway_start_ts is not None:
        result['flyway'] = hibernate_start_ts - flyway_start_ts

    print(result)
    return result


def get_startup_time_logs(group_name, start_time, end_time):
    query = "fields @timestamp, @logStream, @message " \
            "| filter @message like /Started/ " \
            "| parse @message \"Started * in * seconds (JVM running for *)\" as appName, appStartTime, jvmStartTime" \
            "| sort @timestamp desc " \
            "| limit 2000"

    response = get_logs(query, group_name, start_time, end_time)

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
    data = pd.read_csv(file_name).sort_values(by=['Service Name'])
    grouped_data_mean = data.groupby('Service Name').mean().reset_index().sort_values(by=['Service Name'])
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
    arg_parser.add_argument("-lg", "--log_group", required=True, help="AWS Log Group Name")
    arg_parser.add_argument("-s", "--start", required=True, help="Start time (ISO-8601 format)")
    arg_parser.add_argument("-e", "--end", required=True, help="End time (ISO-8601 format)")
    arg_parser.add_argument("-svc", "--service", required=False, help="Service Name")
    args = arg_parser.parse_args()

    env_name = args.log_group
    start_time = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end_time = datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")
    service_name = args.service

    logging.info(
        'Analysing logs for AWS log group {} between {} and {}'.format(env_name,
                                                                       start_time.isoformat(),
                                                                       end_time.isoformat()))

    if service_name is not None:
        get_startup_logs_for_service(env_name, service_name, start_time, end_time)
    else:
        results = get_startup_time_logs(env_name, start_time, end_time)
        file_name = output_data_to_csv(env_name, start_time, end_time, results)
        show_graph(env_name, file_name)


main(sys.argv)
