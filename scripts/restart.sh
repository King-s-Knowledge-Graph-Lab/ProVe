#!/bin/bash

echo "Updating code"
git pull

echo "Moving api code to kclwqt.er.kc.ac.uk folder"
API_FOLDER="/home/ubuntu/RQV/api"
KCL_FOLDER="/var/www/kclwqt.sites.er.kcl.ac.uk"

cp "$API_FOLDER/app.py" "$KCL_FOLDER/app.py"
cp "$API_FOLDER/custom_decorators.py" "$KCL_FOLDER/custom_decorators.py"
cp -r "$API_FOLDER/docs/" "$KCL_FOLDER/docs/"


echo "Restarting services"
sudo systemctl restart apache2
sudo systemctl restart prove_service
