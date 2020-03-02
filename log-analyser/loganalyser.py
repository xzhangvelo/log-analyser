#!/usr/bin/python

import logging
import sys
import csv
from pprint import pprint
import re
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)])


def read_hibernate_statistics(filename):
    reports = []

    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            message_lines = row['@message'].splitlines()
            batches = [line for line in message_lines if
                       re.search("^.*nanoseconds spent executing.*JDBC batches;$", line.strip())]
            batches_desc = batches[0].strip()
            batches_desc_split = batches_desc.split(' ')

            statements = [line for line in message_lines if
                          re.search("^.*nanoseconds spent executing.*JDBC statements;$", line.strip())]
            statements_desc = statements[0].strip()
            statements_desc_split = statements_desc.split(' ')

            flushes = [line for line in message_lines if
                       re.search("^.*nanoseconds spent executing.* flushes .*$", line.strip())]
            flushes_desc = flushes[0].strip()
            flushes_desc_split = flushes_desc.split(' ')

            report = {
                'timestamp': row['@timestamp'].strip(),
                'batches': {
                    # 'description': batches_desc,
                    'duration_ms': float("{0:.2f}".format(int(batches_desc_split[0]) / 1000000)),
                    'amount': int(batches_desc_split[4])
                },
                'exec_statements': {
                    # 'description': statements_desc,
                    'duration_ms': float("{0:.2f}".format(int(statements_desc_split[0]) / 1000000)),
                    'amount': int(statements_desc_split[4])
                },
                'exec_flushes': {
                    # 'description': flushes_desc,
                    'duration_ms': float("{0:.2f}".format(int(flushes_desc_split[0]) / 1000000)),
                    'amount': int(flushes_desc_split[4])
                }

            }
            reports.append(report)

    csv_reports = []
    csv_reports.append(
        "timestamp,batch_time_ms,batches,statement_time_ms,statements,flush_time_ms,flushes,total_time_ms")
    for report in reports:
        if report['batches']['duration_ms'] > 200 \
                or report['exec_statements']['duration_ms'] > 200 \
                or report['exec_flushes']['duration_ms'] > 200:
            total_time = report['batches']['duration_ms'] + \
                         report['exec_statements']['duration_ms'] + \
                         report['exec_flushes']['duration_ms']
            csv_reports.append('{},{},{},{},{},{},{},{}'.format(report['timestamp'],
                                                                report['batches']['duration_ms'],
                                                                report['batches']['amount'],
                                                                report['exec_statements']['duration_ms'],
                                                                report['exec_statements']['amount'],
                                                                report['exec_flushes']['duration_ms'],
                                                                report['exec_flushes']['amount'],
                                                                float("{0:.2f}".format(total_time))))

    output_file_name = "result_{}.csv".format(filename.replace('.', '_').replace('/', '_'))
    output_csv_file(output_file_name, csv_reports)
    # output_file = open(output_file_name, "w")
    # for line in csv_reports:
    #     output_file.write(line)
    #     output_file.write('\n')
    # output_file.close()
    # pprint(csv_reports)
    # print(row['@timestamp'], row['@logStream'], row['@message'])


def output_csv_file(filename, lines):
    output_file = open(filename, "w")
    for line in lines:
        output_file.write(line)
        output_file.write('\n')
    output_file.close()


def get_timer_contents(filename, str_filter=None):
    contents = {}
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            method_name = str(row['method'])
            if str_filter is not None:
                if str_filter not in method_name and str_filter not in str(row['parent']) and str_filter not in str(
                        row['testname']):
                    continue
            if contents.get(method_name) is None:
                contents[method_name] = {
                    'parent': str(row['parent']),
                    'total': int(row['total']),
                    'count': int(row['count'])
                }
            else:
                if contents[method_name]['parent'] != str(row['parent']):
                    parent = 'VARIES'
                else:
                    parent = str(row['parent'])
                total = contents[method_name]['total'] + int(row['total'])
                count = contents[method_name]['count'] + int(row['count'])
                contents[method_name] = {
                    'parent': parent,
                    'total': total,
                    'count': count
                }
    return contents


def compare_timers(left, right):
    left_contents = get_timer_contents(left)
    right_contents = get_timer_contents(right)
    diff_in_left_not_in_right_method_calls = {k: left_contents[k] for k in left_contents if k not in right_contents}
    diff_in_right_not_in_left_method_calls = {k: right_contents[k] for k in right_contents if k not in left_contents}
    time_delta = 5000
    diff_values = {k: left_contents[k] for k in left_contents
                   if k in right_contents
                   # and abs(left_contents[k]['total'] - right_contents[k]['total']) > time_delta
                   }
    left_values = {k: left_contents[k] for k in left_contents if k in diff_values}
    right_values = {k: right_contents[k] for k in right_contents if k in diff_values}

    result = {
        'left': left_values,
        'right': right_values,
        'in_left_not_in_right': diff_in_left_not_in_right_method_calls,
        'in_right_not_in_left': diff_in_right_not_in_left_method_calls
    }
    return result


def output_compare_timers_result(left_file_name, right_file_name, result):
    csv_reports = []
    csv_reports.append(
        "method,left_total,left_count,left_mean,right_total,right_count,right_mean")
    for method_name in result['left'].keys():
        csv_reports.append(
            "{},{},{},{},{},{},{}".format(method_name,
                                          result['left'][method_name]['total'],
                                          result['left'][method_name]['count'],
                                          float("{0:.2f}".format(
                                              result['left'][method_name]['total'] / result['left'][method_name][
                                                  'count'])),
                                          result['right'][method_name]['total'],
                                          result['right'][method_name]['count'],
                                          float("{0:.2f}".format(
                                              result['right'][method_name]['total'] / result['right'][method_name][
                                                  'count'])), ))
    for method_name in result['in_left_not_in_right'].keys():
        csv_reports.append(
            "{},{},{},{},{},{},{}".format(method_name,
                                          result['in_left_not_in_right'][method_name]['total'],
                                          result['in_left_not_in_right'][method_name]['count'],
                                          float("{0:.2f}".format(
                                              result['in_left_not_in_right'][method_name]['total'] /
                                              result['in_left_not_in_right'][method_name][
                                                  'count'])),
                                          0,
                                          0,
                                          0))
    for method_name in result['in_right_not_in_left'].keys():
        csv_reports.append(
            "{},{},{},{},{},{},{}".format(method_name,
                                          0,
                                          0,
                                          0,
                                          result['in_right_not_in_left'][method_name]['total'],
                                          result['in_right_not_in_left'][method_name]['count'],
                                          float("{0:.2f}".format(
                                              result['in_right_not_in_left'][method_name]['total'] /
                                              result['in_right_not_in_left'][method_name][
                                                  'count']))
                                          ))
    output_file_name = 'diff_{}_AND_{}.csv'.format(left_file_name[-10:].replace('.', '_'),
                                                   right_file_name[-10:].replace('.', '_'))
    output_csv_file(output_file_name, csv_reports)


def group_method_calls(filename, str_filter=None):
    contents = get_timer_contents(filename, str_filter)
    csv_reports = []
    csv_reports.append("parent,method,total,count,mean")
    for method_name in contents.keys():
        mean = float("{0:.2f}".format(
            contents[method_name]['total'] / contents[method_name][
                'count']))
        # TODO: The below threshold can be configurable
        if contents[method_name]['total'] > 0 and mean > 0:
            csv_reports.append(
                "{},{},{},{},{}".format(contents[method_name]['parent'],
                                        method_name,
                                        contents[method_name]['total'],
                                        contents[method_name]['count'],
                                        mean
                                        ))
    output_file_name = 'group_top_{}_{}.csv'.format(filename[-10:].replace('.', '_'), str_filter.replace('.', '_'))
    output_csv_file(output_file_name, csv_reports)

def str_to_iso_datetime(timestamp):
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

def analyse_keyword(filename, keyword):
    file = open(filename, "r")
    lines = file.readlines()
    keyword_lines = [k for k in lines if keyword in k]
    print("========================{}=========================".format(filename))
    print("There are [{}] out of [{}] lines contain [{}]".format(len(keyword_lines), len(lines), keyword))
    if len(keyword_lines) > 0:
        print("The first line starts with {} ... omitted...".format(keyword_lines[0][:100]))
        print("The last line starts with {} ... omitted...".format(keyword_lines[-1][:100]))

        if keyword_lines[0].startswith("[2020-"):
            start_timestamp = keyword_lines[0][1:25]
            end_timestamp = keyword_lines[-1][1:25]
            print("Start {}, End {}".format(start_timestamp, end_timestamp))
            print("Duration: {} seconds".format((str_to_iso_datetime(end_timestamp)-str_to_iso_datetime(start_timestamp)).seconds))

        time_deltas = []
        for i in range(len(keyword_lines)):
            if i < len(keyword_lines) - 1:
                i_datetime = str_to_iso_datetime(keyword_lines[i][1:25])
                i_plus_datetime = str_to_iso_datetime(keyword_lines[i+1][1:25])
                time_delta = i_plus_datetime - i_datetime
                if time_delta.microseconds >= 0:
                    time_deltas.append({
                        "left": i_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "right": i_plus_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "delta": time_delta.microseconds
                    })
        time_delta_sum = sum(x["delta"] for x in time_deltas)
        max_time_delta = max(x["delta"] for x in time_deltas)
        # maxa = x for x in time_deltas if max_time_delta == x["delta"]
        print("{} time gaps in between the lines have an average of {} microseconds".format(len(time_deltas), int(time_delta_sum/len(time_deltas))))
        print("the longest gap is {} microseconds at {}".format(max_time_delta, [x["left"] for x in time_deltas if x["delta"] == max_time_delta]))

        start_index = lines.index(keyword_lines[0])
        end_index = lines.index(keyword_lines[-1])
        print("There are {} lines in between the first match and the last".format(str(end_index - start_index)))
        # pprint(time_deltas)





def main(argv):
    if argv[1] == 'hibernate':
        read_hibernate_statistics(argv[2])
    elif argv[1] == 'diff':
        result = compare_timers(argv[2], argv[3])
        output_compare_timers_result(argv[2], argv[3], result)
    elif argv[1] == 'group':
        if argv[2] == 'filter':
            group_method_calls(argv[4], argv[3])
        else:
            group_method_calls(argv[2])
    elif argv[1] == 'keyword':
        analyse_keyword(argv[2], argv[3])


main(sys.argv)
