import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib

#AZURE SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

engineAzure.execute("""SELECT TOP 10 * FROM DIM_CAO
    """
    )