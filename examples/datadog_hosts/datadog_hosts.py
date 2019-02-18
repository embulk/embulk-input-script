#!/usr/bin/env python

# How to use:
# 1. install python
# 2. install embulk
# 3. install datadog client SDK:  $ pip install datadog
# 3. install PyYAML:              $ pip install pyyaml
# 4. install embulk-input-script: $ embulk gem install embulk-input-script
# 5. run:                         $ embulk run config.yml

import datadog
import yaml
import json
import csv
import sys

if sys.argv[1] == "setup":
    # $ python datadog_hosts.py setup config.yml setup.yml
    print("Setup")
    with open(sys.argv[2]) as file:
        config = yaml.load(file)

    setup = {
        'api_key': config['api_key'],
        'app_key': config['app_key'],
        'columns': [
            {'name': 'name', 'type': 'string'},
            {'name': 'up', 'type': 'boolean'},
            {'name': 'host_name', 'type': 'string'},
            {'name': 'metrics_load', 'type': 'double'},
            {'name': 'metrics_iowait', 'type': 'double'},
            {'name': 'metrics_cpu', 'type': 'double'},
            {'name': 'sources', 'type': 'json'},
        ],
    }

    with open(sys.argv[3], 'w') as file:
        yaml.dump(setup, file)

elif sys.argv[1] == "run":
    # $ python datadog_hosts.py run setup.yml output-0.csv 0
    print("Run")

    with open(sys.argv[2]) as file:
        setup = yaml.load(file)

    datadog.initialize(api_key=setup['api_key'], app_key=setup['app_key'])
    results = datadog.api.Hosts.search(q="development")

    with open(sys.argv[3], 'w') as file:
        writer = csv.writer(file)
        for host in results['host_list']:
            writer.writerow([
                host['name'],
                host['up'],
                host['host_name'],
                host['metrics']['load'],
                host['metrics']['iowait'],
                host['metrics']['cpu'],
                json.dumps(host['sources']),
            ])

elif sys.argv[1] == "finish":
    print("Finish")

