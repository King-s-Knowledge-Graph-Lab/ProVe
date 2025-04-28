mkdir -p /mnt/rds/dr_backup

DATE=$(date +"%d/%m/%Y %H:%M:%S")

mongodump --out=/mnt/rds/dr_backup
body="
Disaster Recovery Backup completed at: $DATE

This is an automated message.
If you don't want to receive this message again, please contact nathan.schneider_gavenski@kcl.ac.uk.

ProVe Tool
https://kclwqt.sites.er.kcl.ac.uk/apidocs/"

echo "$body" | mail -s "Disaster Recovery Backup $DATE" k21158663@kcl.ac.uk odinaldo.rodrigues@kcl.ac.uk albert.merono@kcl.ac.uk
