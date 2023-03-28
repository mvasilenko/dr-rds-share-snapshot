# AWS Cross-account Disaster Recovery for lost RDS snapshots

How to solve the problem of Disaster Recovery, when something catastrophic was happend and you loose RDS databases AND their snapshots?

Easy, please refer to the diagram below, you need to have two AWS accounts, which are not part of one AWS organization, main/production account and backup/DR account

the code is consists of two python scripts 

- `src_acc_take_share_rds_snapshot.py` running daily in main/production AWS account
- `dst_acc_copy_shared_rds_snapshot.py` running daily in backup/DR AWS account


<img src="diagram.png">

# Deploy

TODO

# Original post

https://stackoverflow.com/questions/59983969/aws-rds-disaster-recovery-using-cross-account/67905878#67905878
