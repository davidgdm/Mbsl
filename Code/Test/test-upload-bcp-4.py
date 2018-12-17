import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
import datetime
import time
import urllib
import pyodbc
import time
import pymssql
import subprocess



print('Process has started at:')
print(datetime.datetime.now())

subprocess.run("""bcp Extr_bcp in "/home/dwh/ETL/mbsl/Code/daily.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,""", shell=False, check=True)
print('\n\nProcess has finished at:')
print(datetime.datetime.now())
print('-------------------------------')