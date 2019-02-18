#!/usr/bin/env python

# How to use:
# 1. install python
# 2. install embulk
# 3. install EC2 client SDK:      $ pip install boto3
# 3. install PyYAML:              $ pip install pyyaml
# 4. install embulk-input-script: $ embulk gem install embulk-input-script
# 5. run:                         $ embulk run config.yml

import boto3
import yaml
import json
import csv
import sys

if sys.argv[1] == "setup":
    # $ python aws_ec2_instances.py setup config.yml setup.yml
    print("Setup")

    with open(sys.argv[2]) as file:
        config = yaml.load(file)

    setup = {
        'region_name': config['region_name'],
        'columns': [
            {'name': 'image_id', 'type': 'string'},
            {'name': 'instance_id', 'type': 'string'},
            {'name': 'instance_type', 'type': 'string'},
            {'name': 'private_dns_name', 'type': 'string'},
            {'name': 'private_ip_address', 'type': 'string'},
            {'name': 'public_dns_name', 'type': 'string'},
            {'name': 'state', 'type': 'string'},
            {'name': 'subnet_id', 'type': 'string'},
            {'name': 'vpc_id', 'type': 'string'},
            {'name': 'security_groups', 'type': 'json'},
            {'name': 'tags', 'type': 'json'},
        ],
    }

    with open(sys.argv[3], 'w') as file:
        yaml.dump(setup, file)

elif sys.argv[1] == "run":
    # $ python aws_ec2_instances.py run setup.yml output-0.csv 0
    print("Run")

    with open(sys.argv[2]) as file:
        setup = yaml.load(file)

    client = boto3.client(service_name='ec2', region_name=setup['region_name'])
    instances = client.describe_instances()

    with open(sys.argv[3], 'w') as file:
        writer = csv.writer(file)
        for reservation in instances['Reservations']:
            instance = reservation['Instances'][0]
            print(instance)
            writer.writerow([
                instance['ImageId'],
                instance['InstanceId'],
                instance['InstanceType'],
                instance['PrivateDnsName'],
                instance['PrivateIpAddress'],
                instance['PublicDnsName'],
                instance['State']['Name'],
                instance['SubnetId'],
                instance['VpcId'],
                json.dumps(list(map(lambda sg: sg['GroupId'], instance['SecurityGroups']))),
                json.dumps(dict(map(lambda pair: [pair['Key'], pair['Value']], instance['Tags']))),
            ])

elif sys.argv[1] == "finish":
    print("Finish")

