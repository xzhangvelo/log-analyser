#!/usr/bin/python

import sys
from pprint import pprint
import json


def sort_and_print(data):
    sorted_result = sorted(data, key=lambda k: k['action'])
    for e in sorted_result:
        print('[{}][{}] -- {}'.format(e['action'], e['code'], e['location']))


def main(argv):
    filename = argv[1]
    print(filename)
    breaking_changes = []
    non_breaking_changes = []
    with open(filename) as json_file:
        data = json.load(json_file)
        for entry in data.get('breakingDifferences'):
            result_entry = {
                'location': entry['sourceSpecEntityDetails'][0]['location'],
                'action': entry['action'],
                'code': entry['code']
            }
            breaking_changes.append(result_entry)

        for entry in data.get('nonBreakingDifferences'):
            location = None
            if len(entry['sourceSpecEntityDetails']) > 0:
                location = entry['sourceSpecEntityDetails'][0]['location']
            else:
                location = entry['destinationSpecEntityDetails'][0]['location']
            result_entry = {
                'location': location,
                'action': entry['action'],
                'code': entry['code']
            }
            non_breaking_changes.append(result_entry)

    print('--------BREAKING CHANGES----------')
    sort_and_print(breaking_changes)

    print('--------NON-BREAKING CHANGES----------')
    sort_and_print(non_breaking_changes)


main(sys.argv)
