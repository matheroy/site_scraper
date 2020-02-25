import os
import sys
import csv
import datetime
import time

import lxml
import requests
from bs4 import BeautifulSoup
import logging
import json
import sqllite_db_manage as sqlDbm
import pickle 

#from boto3 import boto

# instantiate the database connection
dbm = sqlDbm.DbConnect()
db = dbm.connect()
cursor = db.cursor()

#set up a basic error or Info log process
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename=f'{os.getcwd()}\\url_open.log',
                level=logging.DEBUG, format=LOG_FORMAT)

logger = logging.getLogger()
logger.info("Screen Scraper App Startup")



def generate_run_number():
    
    logger.info('In generate_run_number')
    sel_query = 'select max(id) from book_list'
    try:
        num = cursor.execute(sel_query).fetchall()[0][0]
    except:
        sqlDbm.initialize_db()
    logger.info(num)        
    if num in [None, 0]:
        return 1
    else:
        return num+1
    
    return


def log(output_file, data, mode_selector='a', encode='utf_16'):
    """
    Write to a log file 
    
    Arguments:
        output_file {[type]} -- [description]
        data {[type]} -- [description]
    
    Keyword Arguments:
        mode_selector {str} -- [description] (default: {'a'})
        encode {str} -- [description] (default: {'utf_16'})
    """    
    
    with open(output_file, mode=mode_selector, encoding=encode) as file:
        file.write(f'{data}\n')
        file.close()
              
    return

def get_url(url):
    """[summary]
    
    Arguments:
        url {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """    
    try:
        print(f'processing url: {url}')
        response = requests.get(url)
        print(response.status_code)
        assert response.status_code == 200
        return response
    except:
        raise
    
    return None
    
def process_data(soup, book_limit=20):
    
    book_count = 0
    ##soup = BeautifulSoup(response.text, 'lxml')
    #soup = BeautifulSoup(response.text, 'html.parser')
    #print(soup)
    book_dict = {}
    products = soup.findAll('article', attrs={'class': 'product_pod'})
    #print(products)
    for product in products:
        book_title = product.h3.a.get('title')
        rating = product.p.get('class')[1]
        price = product.find('div', attrs={"class": "product_price"}).p.text[1:]
        in_stock = product.find(
            'p', attrs={"class": "instock availability"}).text.strip()
        book_count += 1
        print(f'{book_count}: {book_title}, {rating}, {price}, {in_stock}')
        if book_count > book_limit: break
        if book_title not in master_book_list: 
            book_dict[book_title] = [book_title, rating, price, in_stock]
            book_list.append(book_title)
            master_book_list.append(book_title)
            #log(output_file, f'{book_title}, {rating}, {price}, {in_stock}')
        
        if len(book_list) == book_limit:
            save_the_data(master_book_list, master_list=True)
            save_the_data(book_list)
            write_to_log(book_dict)
            
    print(f'{book_list}')
    return

def save_the_data(bk_list, master_list=False):
    """save the processed data to a csv and the database"""
    
    pickled_data = pickle.dumps(bk_list)
    if master_list:
        cursor.execute(
        '''insert into master_list(id, pickled_data) values(?, ?)''',
        (page_num, pickled_data))
    else:    
        cursor.execute(
        '''insert into Book_list(id, pickled_data, data_file_name) values(?, ?, ?)''',
        (page_num, pickled_data, output_file))
    
    db.commit()
    
    return
    
def write_to_log(book_dict):
    '''write the book list to the csv or similar file '''
    
    header = 'Title, Rating, Price, In_stock'
    log(output_file, header)
    for book in book_dict.keys():
        title, rating, price, in_stock = book_dict.get(book)
        log(output_file, f'{title}, {rating}, {price}, {in_stock}')
    
    return

def load_master_list():
    '''load the current master list from the db'''
    logger.info(f"Loading Master List, page# {page_num}")
    sel_query = f'select pickled_data from master_list where id = {page_num-1}'
    
    try:
        pickled_data = cursor.execute(sel_query).fetchall()[0][0]
        
        logging.info(pickled_data)
        mList = pickle.loads(pickled_data)
        logging.info(f'mlist = {mList}')
        return mList
    except:
        #raise
        return []
    

def get_next_page(soup):
     
    #soup = BeautifulSoup(response.text, 'html.parser')
    #print(soup)
    try:
        next_link = soup.findAll('li', attrs={'class': 'next'})[
            0].find('a').get('href').rstrip().split('/')[-1]
        
        if next_link != None or len(next_link)>1:
            next_page = f'{base_url}catalogue/{next_link}'
    except:
        next_page = None
    
    print(f'next link = {next_page}')
    return next_page

def send_to_cloud():
    pass

if __name__ == '__main__':


    base_url = 'http://books.toscrape.com/'
    url_to_find = f'{base_url}'
    
    ##db = sqlite3.connect(f'{os.getcwd()}\\data\\lavaDB')

    page_num = generate_run_number()
    TODAY = datetime.datetime.today()

    TIMESTAMP = f'{TODAY.month}-{TODAY.day}-{TODAY.year}-{TODAY.hour}-{TODAY.minute}'
    output_file = f'roy_mathew_books_{page_num}_of_50_{TIMESTAMP}.csv'
    print(output_file)

    page_to_log_dict = {}
    page_to_log_dict[page_num] = output_file
    book_limit = 20
    print(page_to_log_dict)

    #keeps track of all books that have been found, this is meant to be persistent list
    master_book_list = load_master_list()  # [] keeps track of all books found
    book_list = []  # keeps track of all current books not in the master list

    max_page_search = 1#99
    page_found = 1
    next_page_link = None
    
    while True:    
      
        response = get_url(url_to_find)
        #soup = BeautifulSoup(response.text, 'lxml')
        soup = BeautifulSoup(response.text, 'html.parser')
        process_data(soup)
        logging.info(len(book_list))
        url_to_find =  get_next_page(soup)
        #if (url_to_find == None) or (len(url_to_find) <= 1) or (page_found >= max_page_search):
        if (url_to_find == None) or (len(url_to_find)<=1) or len(book_list) >= book_limit: 
            response.close()
            print('no more links')
            break
        page_found += 1
    pass
