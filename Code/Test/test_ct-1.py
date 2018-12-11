import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pyodbc 

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')

cursor = cnxn_dev.cursor()
#Performing all transformations
querystring ="""
    if object_id('temp_ct') is not null
	drop table temp_ct
	
declare @dt datetime
set @dt=(select getdate());

create table temp_ct
(
[date_dt] datetime NULL
);

insert into temp_ct values (@dt)
    """
cursor.execute(querystring)
cnxn_dev.commit()





