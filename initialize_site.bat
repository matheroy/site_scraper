@ECHO OFF
ECHO Initializing the site scrapper project files
ECHO Deleting any csv, note that this will delete all CSV
del roy_mathew_book*.csv
ECHO Reset the log file by deleting it. 
del dbm.log
ECHO Initialize the database
del data/lavaDB.s3db
C:/Python/Python36/python.exe c:/CodeRepository/python/sqllite_db_manage.py
ECHO completed Initialization
