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
EXPECTED_SNAPSHOT_COUNT = int(os.getenv('EXPECTED_SNAPSHOT_COUNT', 4))
# for prod RDS matching (every DB expect QA), must be set to
# export PATTERN='^((?!qa).)*$'
# or ((.+)prod.(.+)|speech-api$) - everything with prod in name plus speech-api
PATTERN = os.getenv('PATTERN', 'ALL_INSTANCES')
TAGGEDINSTANCE = os.getenv('TAGGEDINSTANCE', 'FALSE')
shared_snapshot_regex = r"arn:aws:rds:(.+):\d{12}:snapshot:"

TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())

sns_client = boto3.client('sns')
topic_arn = os.environ.get("TOPIC_ARN", "")


def main():

    aws_region = os.getenv('AWS_DEFAULT_REGION')
    client = boto3.client('rds', region_name=aws_region)

    # lookup KMS key ARN in env vars, it must be created in backup account and shared with main account
    kms_key_arn = os.getenv('KMS_KEY_ARN', '')
    if not bool(re.match(r"^arn:aws:kms:(.+):\d{12}:(?:key\/[a-f0-9-]+)$", kms_key_arn)):
        print("KMS_KEY_ARN environment variable must be set to KMS key AWS arn, like arn:aws:kms:<region>:<account_id>:key/<key_id>")
        sys.exit(1)

    response = client.describe_db_snapshots(IncludeShared=True)
    if not response.get('DBSnapshots'):
        print("Unable to find snapshots, shared with current account, exiting")
        sys.exit(1)
    snapshots_with_shared = response['DBSnapshots']
    snapshots_owned = {}
    ready_snapshot_count = 0
    for snapshot in snapshots_with_shared:
        if not bool(re.search(r"recrypted$", snapshot['DBSnapshotIdentifier'])) or \
           not bool(re.search(shared_snapshot_regex, snapshot['DBSnapshotIdentifier'])):
            continue
        local_copy = copy_shared_snapshot_to_local(client, snapshot, kms_key_arn)
        wait_for_snapshot_to_be_ready(client, local_copy)
        ready_snapshot_count += 1
        # count owned snapshots count for db instance
        snapshots_owned[snapshot['DBInstanceIdentifier']] = snapshots_owned.get(snapshot['DBInstanceIdentifier'], 0) + 1

    for db_instance in snapshots_owned.keys():
        while get_owned_rds_snapshots_count(client, db_instance) > 2:
            snapshot = get_oldest_manual_recrypted_rds_snapshots(client, db_instance)
            print("Deleting oldest recrypted manual snapshot {}".format(snapshot['DBSnapshotIdentifier']))
            client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'])
            time.sleep(1)
    if ready_snapshot_count != EXPECTED_SNAPSHOT_COUNT:
        sns_message = ("COPY_RDS_SNAPSHOT_COUNT_ERROR Shared RDS snapshot copying completed with error:\n"
                       "Expected snapshot count={1} not equal to actual snapshot count={2}, exiting"
                       .format(EXPECTED_SNAPSHOT_COUNT, ready_snapshot_count))
        print(sns_message)
        if not debug and topic_arn:
            sns_client.publish(TopicArn=topic_arn, Message=sns_message,
                               Subject="DR: dst account copy RDS shared snapshot error")
        sys.exit(1)
    print("Shared RDS snapshot copying completed successfully, exiting")
    return


def wait_for_snapshot_to_be_ready(rds_client, snapshot):
    percent_progress = -1
    # simply check if the specified snapshot is healthy every 10 seconds until it is available
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
                    snapshot['DBSnapshotIdentifier'], snapshotcheck['PercentProgress']))
            time.sleep(10)


def copy_shared_snapshot_to_local(rds_client, shared_snapshot, kms_key_arn):
    # unfortunately it's not possible to restore an RDS instance directly from a
    # snapshot that is shared by another account. This makes a copy local to the
    # account where we want to restore the RDS instance
    target_db_snapshot_id = re.sub(shared_snapshot_regex, "", shared_snapshot['DBSnapshotIdentifier'])
    target_db_snapshot_id = "{}-copy".format(target_db_snapshot_id)

    print("Copying shared snaphot {} to local snapshot {}...".format(
        shared_snapshot['DBSnapshotArn'], target_db_snapshot_id))

    try:
        copy = rds_client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=shared_snapshot['DBSnapshotArn'],
            TargetDBSnapshotIdentifier=target_db_snapshot_id,
            KmsKeyId=kms_key_arn
        )
        print("  Copy created.")
        return copy['DBSnapshot']
    except rds_client.exceptions.DBSnapshotAlreadyExistsFault:
        # if the snapshot we tried to make already exists, retrieve it
        print("Snapshot already exists, retrieving {}...".format(target_db_snapshot_id))

        snapshots = rds_client.describe_db_snapshots(
            DBSnapshotIdentifier=target_db_snapshot_id,
        )
        print("  Retrieved.")
        return snapshots['DBSnapshots'][0]


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


def get_owned_rds_snapshots_count(rds_client, db_id):
    print("Getting oldest (manual) snapshot from rds instance {}...".format(db_id))
    # we can't query for the oldest snapshot straight away, so we have to retrieve
    # a full list and go through all of them
    snapshots = rds_client.describe_db_snapshots(
        DBInstanceIdentifier=db_id,
        SnapshotType='manual',
        IncludeShared=False
    )

    owned_rds_snapshots_count = 0
    for snapshot in snapshots['DBSnapshots']:
        if not "recrypted-copy" in snapshot['DBSnapshotIdentifier']:
            continue
        owned_rds_snapshots_count += 1
    print("  Found {} owned snapshot(s) for DB instance {}".format(owned_rds_snapshots_count, db_id))
    return owned_rds_snapshots_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='run in debug mode, do not send sns notifications')
    args = parser.parse_args()
    debug = args.debug

    if not topic_arn and not debug:
        sys.exit("Unable to read TOPIC_ARN environment variable")

    main()
