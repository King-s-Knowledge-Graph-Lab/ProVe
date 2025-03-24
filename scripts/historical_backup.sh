DATE=$(date +"%Y%m%d")
mkdir -p /home/ubuntu/mntdisk/backups/

mongodump --out="/home/ubuntu/mntdisk/backups/$DATE/"
echo "Historical Backup completed at: $DATE"
