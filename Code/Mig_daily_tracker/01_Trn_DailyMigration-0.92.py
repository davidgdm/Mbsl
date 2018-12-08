import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib

#AZURE SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

engineAzure.execute("""
--to create dailyconsolidated table
delete from t_daily_consolidated where snapshot_at=(select snapshot_at from [Extr_Dailysnapshot] group by snapshot_at);
insert into t_daily_consolidated select * from [Extr_Dailysnapshot];
"""
    )