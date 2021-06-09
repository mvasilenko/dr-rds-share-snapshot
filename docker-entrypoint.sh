#!/usr/bin/env sh
if [ ! -z "$TARGET_ACCOUNT" ]; then
  python3 src_acc_take_share_rds_snapshot.py
else
  python3 dst_acc_copy_shared_rds_snapshot_to_local.py
fi