'''Module that sets up and manages the sqlite database'''

import os
import sys
import sqlite3
import pickle
import datetime
import logging


#db = sqlite3.connect(f'{os.getcwd()}\\data\\lavaDB.s3db')
#cursor = db.cursor()

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = f'{os.getcwd()}\\dbm.log', level = logging.DEBUG, format = LOG_FORMAT)
logger = logging.getLogger()
logger.info("DBM Manger Startup")

class DbConnect:

    def __init__(self, env='prod'):
        self.env = env

    def connect(self):
        self.conn = sqlite3.connect(f'{os.getcwd()}\\data\\lavaDB.s3db')
        return self.conn

    def cursor(self):
        return self.conn.cursor()

    def close(self):
        self.conn.close()


##dbm = DbConnect()
##db = dbm.connect()
##cursor = db.cursor()


def open_db_session():
  db = sqlite3.connect(f'{os.getcwd()}\\data\\lavaDB.s3db')

  return db

def create_tables():

  cursor.execute(
      '''CREATE TABLE master_list(id INTEGER PRIMARY KEY NOT NULL,  
      pickled_data BLOB NOT NULL)
      ''')
  db.commit()
  
  cursor.execute(
      '''CREATE TABLE book_list(id INTEGER PRIMARY KEY NOT NULL,  
      pickled_data BLOB NOT NULL, data_file_name TEXT NOT NULL, 
      next_url TEXT, job_run_date TEXT NOT NULL)
      ''')
  db.commit()
  
  create_indexes()
  
  return

def create_indexes():
  
  cursor.execute(
      '''CREATE UNIQUE INDEX id_idx on book_list(id)
      ''')
  db.commit()
  
  return

def drop_tables():
  
  cursor.execute('''DROP TABLE book_list''')
  db.commit()
  
  cursor.execute('''DROP TABLE master_list''')
  db.commit()

  return


def insert_data(page_id, pickled_data, log_name, run_date):
  cursor.execute(
      '''insert into book_list(id, pickled_data, data_file_name, job_run_date) values(?, ?, ?, ?)''', 
      (page_id, pickled_data, log_name, run_date))
  db.commit()

  return

def initialize_db():
  '''initialize the sqlite3 database'''
  
  try:
    drop_tables()
  except Exception as err:
    logging.warning(f'Error DB initiailization: {err}, defaulting to creating new tables')
    pass
  finally:
    create_tables()
  
  return 
  

def update_data(pList):
  cursor.execute(
      '''update pickled_Books set pickled_data = ? where id=?''', (1, pList))
  db.commit()

  return


def test_dbm():

  page_num = 9999
  TODAY = datetime.datetime.today()


  run_date = f'{TODAY.year}-{TODAY.month}-{TODAY.day}'
  TIMESTAMP = f'{TODAY.month}-{TODAY.day}-{TODAY.year}-{TODAY.hour}-{TODAY.minute}'
  output_file = f'roy_mathew_books_{page_num}_of_50_{TIMESTAMP}'

  book_list = ['A Light in the Attic', 'Tipping the Velvet', 'Soumission', 'Sharp Objects', 'Sapiens: A Brief History of Humankind', 'The Requiem Red', 'The Dirty Little Secrets of Getting Your Dream Job', 'The Coming Woman: A Novel Based on the Life of the Infamous Feminist, Victoria Woodhull',
              'The Boys in the Boat: Nine Americans and Their Epic Quest for Gold at the 1936 Berlin Olympics',
              'The Black Maria', 'Starving Hearts (Triangular Trade Trilogy, #1)', "Shakespeare's Sonnets", 'Set Me Free',
              "Scott Pilgrim's Precious Little Life (Scott Pilgrim #1)", 'Rip it Up and Start Again', 'Jack Frost']

  pickled_list = pickle.dumps(book_list)

  
  sel_query = 'select * from book_list'

  try:
    insert_data(page_num, pickled_list, output_file, run_date)
  except:
    db.close()
    raise

  a = cursor.execute(sel_query)
  print(a)
  b = a.fetchall()
  pickled_data = b[0][1]
  print(b)

  if b:
    un_pickle_list = pickle.loads(pickled_data)
    print(un_pickle_list)
  else:
    print(f'No data to unpickle: {b}')

  ##update_data(pickled_list)
  ##print('After Update')
  ##c = cursor.execute(sel_query)
  ##d = c.fetchall()[0][1]
  ##print(d)

  ##un_pickle_list = pickle.loads(d)
  ##print(un_pickle_list)
  
  return


def main():

  initialize_db()
  #test_dbm()

  return


if __name__ == '__main__':

  dbm = DbConnect()
  db = dbm.connect()
  cursor = db.cursor()

  main()
  db.close()
