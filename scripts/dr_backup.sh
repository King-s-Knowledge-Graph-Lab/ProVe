mkdir -p /home/ubuntu/mntdisk/dr_backup

DATE=$(date +"%Y%m%d_%H%M%S")

mongodump --out=/home/ubuntu/mntdisk/dr_backup
echo "Disaster Recovery Backup completed at: $DATE"
