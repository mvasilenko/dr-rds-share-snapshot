#!/usr/bin/env python3
import argparse
import datetime
import logging
import os
import re
import sys
import time

import boto3

if "AWS_DEFAULT_REGION" not in os.environ:
    print("Please set the environment variable AWS_DEFAULT_REGION")
    sys.exit(1)

from snapshots_tool_utils import *

# Initialize everything
LOGLEVEL = os.getenv('LOG_LEVEL', 'DEBUG').strip()
BACKUP_INTERVAL = int(os.getenv('INTERVAL', '24'))
# for prod RDS matching (every DB expect QA), must be set to
# export PATTERN='^((?!qa).)*$'
# or ((.+)prod.(.+)|speech-api$) - everything with prod in name plus speech-api
PATTERN = os.getenv('PATTERN', 'ALL_INSTANCES')
TAGGEDINSTANCE = os.getenv('TAGGEDINSTANCE', 'FALSE')
EXPECTED_SNAPSHOT_COUNT = int(os.getenv('EXPECTED_SNAPSHOT_COUNT', 4))

TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())

sns_client = boto3.client('sns')
topic_arn = os.environ.get("TOPIC_ARN", "")


def main():

    aws_region = os.getenv('AWS_DEFAULT_REGION')
    client = boto3.client('rds', region_name=aws_region)

    target_aws_account = os.getenv('TARGET_ACCOUNT', '0')
    if not bool(re.match(r"^\d{12}$", target_aws_account)):
        print("TARGET_ACCOUNT environment variable must be set to target AWS account id")
        sys.exit(1)

    # lookup KMS key ARN in env vars, it must be created in backup account and shared with main account
    kms_key_arn = os.getenv('KMS_KEY_ARN', '')
    if not bool(re.match(r"^arn:aws:kms:(.+):\d{12}:(?:key\/[a-f0-9-]+)$", kms_key_arn)):
        print("KMS_KEY_ARN environment variable must be set to KMS key AWS arn, like arn:aws:kms:<region>:<account_id>:key/<key_id>")
        sys.exit(1)

    response = paginate_api_call(client, 'describe_db_instances', 'DBInstances')
    filtered_instances = filter_instances(TAGGEDINSTANCE, PATTERN, response)
    if len(filtered_instances) > 0:
        filtered_instances_identifiers = [instance['DBInstanceIdentifier'] for instance in filtered_instances]
        print("Starting snapshot sharing for DB instances: {}".format(filtered_instances_identifiers))
    else:
        print("DB instances list for sharing is empty, matching pattern env var: PATTERN={}".format(PATTERN))

    ready_snapshot_count = 0
    for db_instance in filtered_instances:
        snapshot = get_latest_automatic_rds_snapshots(client, db_instance['DBInstanceIdentifier'])
        # no snapshots found
        if len(snapshot) == 0:
            continue
        recrypted_copy = recrypt_snapshot_with_new_key(client, snapshot, kms_key_arn)
        wait_for_snapshot_to_be_ready(client, recrypted_copy)
        share_snapshot_with_external_account(client, recrypted_copy, target_aws_account)
        ready_snapshot_count += 1
        # clean up old snapshots recrypted with key from backup account
        while get_manual_recrypted_rds_snapshots_count(client, db_instance['DBInstanceIdentifier']) > 2:
            snapshot = get_oldest_manual_recrypted_rds_snapshots(client, db_instance['DBInstanceIdentifier'])
            print("Deleting oldest recrypted manual snapshot {}".format(snapshot['DBSnapshotIdentifier']))
            client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'])
            time.sleep(1)
    if ready_snapshot_count != EXPECTED_SNAPSHOT_COUNT:
        sns_message = ("COPY_RDS_SNAPSHOT_COUNT_ERROR sharing RDS snapshots completed with error: "
                       "Expected snapshot count={0} not equal to actual snapshot count={1}, exiting"
                       .format(EXPECTED_SNAPSHOT_COUNT, ready_snapshot_count))
        print(sns_message)
        if not debug and topic_arn:
            sns_client.publish(TopicArn=topic_arn, Message=sns_message,
                               Subject="DR: source account take shared RDS snapshot error")
        sys.exit(1)
    print("Snapshot sharing completed successfully, exiting")
    return


def share_snapshot_with_external_account(rds_client, snapshot, target_account):
    # in order to restore a snapshot from another account it needs to be shared
    # with that account first
    print("Modifying snapshot {} to be shared with account {}...".format(snapshot['DBSnapshotArn'], target_account))
    rds_client.modify_db_snapshot_attribute(
        DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'],
        AttributeName='restore',
        ValuesToAdd=[target_account]
    )
    print("  Modified snapshot {}".format(snapshot['DBSnapshotIdentifier']))


def wait_for_snapshot_to_be_ready(rds_client, snapshot):
    # simply check if the specified snapshot is healthy every 10 seconds until it is available
    percent_progress = -1
    while True:
        snapshotcheck = rds_client.describe_db_snapshots(
            DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'])['DBSnapshots'][0]
        if snapshotcheck['Status'] == 'available':
            print("  Snapshot {} complete and available!".format(snapshot['DBSnapshotIdentifier']))
            break
        else:
            # output percentage only when it's changed
            if percent_progress != snapshotcheck['PercentProgress']:
                percent_progress = snapshotcheck['PercentProgress']
                print("Snapshot {} in progress, {}% complete".format(
                    snapshot['DBSnapshotIdentifier'], percent_progress))
            time.sleep(10)


def recrypt_snapshot_with_new_key(rds_client, snapshot, kms_key_arn):
    # create an identifier to use as the name of the manual snapshot copy
    if ':' in snapshot['DBSnapshotIdentifier']:
        target_db_snapshot_id = "{}-recrypted".format(snapshot['DBSnapshotIdentifier'].split(':')[1])
    else:
        target_db_snapshot_id = "{}-recrypted".format(snapshot['DBSnapshotIdentifier'])

    print("Copying automatic snapshot {} to manual snapshot".format(snapshot['DBSnapshotIdentifier']))

    try:
        # copy the snapshot, supplying the new KMS key (which is also shared with
        # the target account)
        copy = rds_client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'],
            TargetDBSnapshotIdentifier=target_db_snapshot_id,
            KmsKeyId=kms_key_arn
        )
        print("  Snapshot {} created".format(snapshot['DBSnapshotIdentifier']))
        return copy['DBSnapshot']
    except rds_client.exceptions.DBSnapshotAlreadyExistsFault:
        # if the snapshot we tried to make already exists, retrieve it
        print("Snapshot already exists, retrieving {}".format(target_db_snapshot_id))

        snapshots = rds_client.describe_db_snapshots(DBSnapshotIdentifier=target_db_snapshot_id)

        return snapshots['DBSnapshots'][0]


def get_latest_automatic_rds_snapshots(rds_client, db_id):
    print("Getting latest (automated) snapshot from rds instance {}...".format(db_id))
    # we can't query for the latest snapshot straight away, so we have to retrieve
    # a full list and go through all of them
    snapshots = rds_client.describe_db_snapshots(
        DBInstanceIdentifier=db_id,
        SnapshotType='automated'
    )
    latest = []
    for snapshot in snapshots['DBSnapshots']:
        if len(latest) == 0:
            latest = snapshot
        if not snapshot.get('SnapshotCreateTime') or not latest.get('SnapshotCreateTime'):
            continue
        if snapshot['SnapshotCreateTime'] > latest['SnapshotCreateTime']:
            latest = snapshot
    # if we have any snapshots at all
    if len(latest) > 0:
        print("  Found snapshot {}".format(latest['DBSnapshotIdentifier']))
    else:
        print("  No snapshots found for instance {}".format(db_id))
    return latest


def get_oldest_manual_recrypted_rds_snapshots(rds_client, db_id):
    print("Getting oldest recrypted (manual) snapshot from rds instance {}...".format(db_id))
    # we can't query for the latest snapshot straight away, so we have to retrieve
    # a full list and go through all of them
    snapshots = rds_client.describe_db_snapshots(
        DBInstanceIdentifier=db_id,
        SnapshotType='manual'
    )

    oldest = 0
    for snapshot in snapshots['DBSnapshots']:
        if not "recrypted" in snapshot['DBSnapshotIdentifier']:
            continue
        if oldest == 0:
            oldest = snapshot
        if snapshot['SnapshotCreateTime'] < oldest['SnapshotCreateTime']:
            oldest = snapshot

    print("  Found snapshot {}".format(oldest['DBSnapshotIdentifier']))
    return oldest


def get_manual_recrypted_rds_snapshots_count(rds_client, db_id):
    print("Getting oldest recrypted (manual) snapshot from rds instance {}...".format(db_id))
    # we can't query for the oldest snapshot straight away, so we have to retrieve
    # a full list and go through all of them
    snapshots = rds_client.describe_db_snapshots(
        DBInstanceIdentifier=db_id,
        SnapshotType='manual'
    )

    recrypted_snapshots_count = 0
    for snapshot in snapshots['DBSnapshots']:
        if not "recrypted" in snapshot['DBSnapshotIdentifier']:
            continue
        recrypted_snapshots_count += 1
    print("  Found {} manual recrypted snapshot(s) for DB instance {}".format(recrypted_snapshots_count, db_id))
    return recrypted_snapshots_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='run in debug mode, do not send sns notifications')
    args = parser.parse_args()
    debug = args.debug

    if not topic_arn and not debug:
        sys.exit("Unable to read TOPIC_ARN environment variable")

    main()
