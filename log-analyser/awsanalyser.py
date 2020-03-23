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
    logging.info('Query: [{}]'.format(query))
    start_query_response = client.start_query(
        logGroupName=group_name,
        startTime=int(start_time.timestamp()),
        endTime=int(end_time.timestamp()),
        queryString=query
    )

    query_id = start_query_response['queryId']
    response = None
    while response is None or response['status'] == 'Running':
        logging.info('Waiting for query to complete ...')
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
            logging.debug(
                '{} - from {} to {}'.format(log_stream['logStreamName'], first_event_timestamp, last_event_timestamp))
            log_stream_names.append(log_stream['logStreamName'])
    logging.debug('Log Streams: {}'.format(log_stream_names))
    logging.info('{} log streams are found'.format(len(log_stream_names)))
    return log_stream_names


def get_startup_logs_for_service(group_name, service_name, start_time, end_time):
    log_stream_names = get_log_stream_names(group_name, service_name, start_time, end_time)
    keywords = [
        'The following profiles are active',
        'Flyway Community Edition',
        'HHH000412: Hibernate Core',
        'Producer configuration:',
        'JVM running for',
        'Ensured that spring events are handled',
        'Initialized JPA',
        'Tomcat initialized',
        'Creating filter chain',
        'Started o.s.b.w.e.j.JettyEmbeddedWebAppContext'
    ]

    query = "fields @timestamp, @logStream, @message " \
            "| filter @message like /{}/" \
            "| filter @logStream like /{}/" \
            "| parse @message \"*  * *] *: *\" as ts, level, prefixes, class_name, details" \
            "| sort @timestamp desc " \
            "| limit 5000".format('|'.join(keywords), service_name)

    response = get_logs(query, group_name, start_time, end_time)

    logging.info("Retrieved {} log entries from AWS for service {}".format(len(response['results']), service_name))
    messages = []
    for entry in response['results']:
        message = {}
        match = False
        for field in entry:
            if field['field'] == '@logStream':
                ls = field['value']
                if ls in log_stream_names:
                    match = True
                    message['log_stream'] = ls
            if match:
                if field['field'] == 'details':
                    message['message'] = field['value']
                elif field['field'] == 'ts':
                    message['timestamp'] = field['value']
        if 'message' in message:
            messages.append(message)

    logging.info('{} entries are taken into account for analysis.'.format(len(messages)))
    return log_stream_names, messages


def analyse_startup_stages(log_stream_names, messages):
    results = []
    for log_stream_name in log_stream_names:
        ls_messages = []
        for message in messages:
            if message['log_stream'] == log_stream_name:
                ls_messages.append(message)

        logging.info("Analysing detailed timing for log stream {}".format(log_stream_name))

        for m in ls_messages:
            print(m)
        logging.debug("Messages: {}", ls_messages)
        # ts = '2020-03-17 12:20:03.633'
        ts_formatter_str = "%Y-%m-%d %H:%M:%S.%f"
        app_start_ts = None
        app_end_ts = None
        flyway_start_ts = None
        kafka_start_ts = None
        kafka_end_ts = None
        kafka_topics_end_ts = None
        hibernate_start_ts = None
        hibernate_end_ts = None
        tomcat_end_ts = None
        jetty_end_ts = None
        for ls_message in ls_messages:
            if 'The following profiles are active' in ls_message['message']:
                app_start_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Flyway Community Edition' in ls_message['message']:
                flyway_start_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'HHH000412: Hibernate Core' in ls_message['message']:
                hibernate_start_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Ensured that spring events are handled' in ls_message['message']:
                kafka_end_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Producer configuration:' in ls_message['message']:
                kafka_start_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'JVM running for' in ls_message['message']:
                app_end_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Tomcat initialized' in ls_message['message']:
                tomcat_end_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Initialized JPA ' in ls_message['message']:
                hibernate_end_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Creating filter chain' in ls_message['message']:
                kafka_topics_end_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)
            if 'Started o.s.b.w.e.j.JettyEmbeddedWebAppContext' in ls_message['message']:
                jetty_end_ts = datetime.strptime(ls_message['timestamp'], ts_formatter_str)

        result = {}
        if kafka_end_ts is not None and kafka_start_ts is not None:
            result['kafka'] = (kafka_end_ts - kafka_start_ts).seconds
        if app_end_ts is not None and app_start_ts is not None:
            result['app'] = (app_end_ts - app_start_ts).seconds
        if hibernate_start_ts is not None and flyway_start_ts is not None:
            result['flyway'] = (hibernate_start_ts - flyway_start_ts).seconds
        if hibernate_start_ts is not None and hibernate_end_ts is not None:
            result['hibernate'] = (hibernate_end_ts - hibernate_start_ts).seconds
        if tomcat_end_ts is not None and app_start_ts is not None:
            result['tomcat'] = (tomcat_end_ts - app_start_ts).seconds
        if kafka_start_ts is not None and kafka_topics_end_ts is not None:
            result['kafka_topics'] = (kafka_topics_end_ts - kafka_start_ts).seconds
        if kafka_end_ts is not None and kafka_topics_end_ts is not None:
            result['kafka_consumers'] = (kafka_end_ts - kafka_topics_end_ts).seconds
        if jetty_end_ts is not None and app_start_ts is not None:
            result['jetty'] = (jetty_end_ts - app_start_ts).seconds

        if result:
            result['log_stream'] = log_stream_name
            results.append(result)

    return results


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


def show_startup_time_graph(env_name, file_name):
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


def show_startup_time_breakdown_graph(service_name, file_name):
    logging.info("Generating graph from {}...".format(file_name))
    data = pd.read_csv(file_name).sort_values(by=['Name'])

    row_count, column_count = data.shape
    if row_count < 1:
        logging.info("Not enough data for further processing. Total Rows: {}, Total Columns: {}".format(row_count,
                                                                                                        column_count))
        return

    grouped_data_mean = data.groupby('Name').mean().reset_index().sort_values(by=['Name'])
    pprint(grouped_data_mean)

    fig = make_subplots(shared_yaxes=True)

    fig.add_trace(
        go.Scatter(x=data['Name'], y=data['Time in Seconds'],
                   mode='markers',
                   name='Duration')
    )

    fig.add_trace(
        go.Scatter(x=grouped_data_mean['Name'], y=grouped_data_mean['Time in Seconds'],
                   mode="markers",
                   name="Duration Mean")
    )

    fig.update_layout(
        title_text='Startup Time Detailed -- Service: {}'.format(service_name)
    )

    fig.update_xaxes(title_text="Stages")
    fig.update_yaxes(title_text="Time in Seconds")

    fig.show()


def format_for_file_name(value):
    return value.replace('.', '_').replace('/', '_').replace('-', '_').replace(':', '_')


def output_timings_data_to_csv(service_name, start_time, end_time, data, horizontal=False):
    if horizontal:
        csv_reports = ["Log Stream,App,Kafka,Flyway,Hibernate,Tomcat,Kafka Topics,Kafka Consumers,Jetty"]
        for entry in data:
            csv_reports.append('{},{},{},{},{},{},{},{}'.format(
                entry['log_stream'], entry['app'], entry['kafka'], entry['flyway'],
                entry['hibernate'], entry['tomcat'], entry['kafka_topics'], entry['kafka_consumers']))
    else:
        csv_reports = ['Name,Time in Seconds']
        for entry in data:
            if 'app' in entry:
                csv_reports.append('{},{}'.format('Overall', entry['app']))
            if 'kafka' in entry:
                csv_reports.append('{},{}'.format('Kafka', entry['kafka']))
            if 'flyway' in entry:
                csv_reports.append('{},{}'.format('Flyway', entry['flyway']))
            if 'hibernate' in entry:
                csv_reports.append('{},{}'.format('Hibernate', entry['hibernate']))
            if 'tomcat' in entry:
                csv_reports.append('{},{}'.format('Tomcat', entry['tomcat']))
            if 'kafka_topics' in entry:
                csv_reports.append('{},{}'.format('Kafka Topics', entry['kafka_topics']))
            if 'kafka_consumers' in entry:
                csv_reports.append('{},{}'.format('Kafka Consumers', entry['kafka_consumers']))
            if 'jetty' in entry:
                csv_reports.append('{},{}'.format('Jetty', entry['jetty']))

    csv_file_name = "{}_timings_from_{}_to_{}.csv".format(format_for_file_name(service_name),
                                                          format_for_file_name(start_time.isoformat()),
                                                          format_for_file_name(end_time.isoformat()))
    output_csv_file(csv_file_name, csv_reports)
    return csv_file_name


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
    logging.info("{} lines are written to [{}]".format(len(lines), filename))


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
        log_stream_names, messages = get_startup_logs_for_service(env_name, service_name, start_time, end_time)
        timings = analyse_startup_stages(log_stream_names, messages)
        file_name = output_timings_data_to_csv(service_name, start_time, end_time, timings)
        show_startup_time_breakdown_graph(service_name, file_name)
    else:
        results = get_startup_time_logs(env_name, start_time, end_time)
        file_name = output_data_to_csv(env_name, start_time, end_time, results)
        show_startup_time_graph(env_name, file_name)


main(sys.argv)
