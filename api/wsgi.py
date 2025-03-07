import os
import sys
import site

# Python 3.9 경로로 수정
site.addsitedir('/home/ubuntu/RQV/rqvenv/lib/python3.10/site-packages')

project_home = '/home/ubuntu/RQV'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

os.chdir(project_home)

from app import app as application