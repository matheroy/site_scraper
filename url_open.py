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
#import boto3

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
    ''' Determine and generate the next page number '''
    
    logger.info('In generate_run_number')
    sel_query = 'select max(id) from book_list'
    try:
        num = cursor.execute(sel_query).fetchall()[0][0]
    except:
        sqlDbm.initialize_db()
    
    if num in [None, 0]:
        num =  1
    else:
        num += 1
    logger.info(f'The generated page number is:{num}')
    return num


def log(output_file, data, mode_selector='a', encode='utf_16'):
    """
    Write to a file. create a csv file
    """    
    
    with open(output_file, mode=mode_selector, encoding=encode) as file:
        file.write(f'{data}\n')
        file.close()
              
    return

def get_url(url):
    """ retrieve a valid url and return valid responses """    
    try:
        print(f'processing url: {url}')
        response = requests.get(url)
        print(response.status_code)
        assert response.status_code == 200
        return response
    except:
        raise
    
    return None
    
def process_data0(soup, book_limit=20):
    '''This function processes each page and determines if the books have already been processed, 
    and if ht enumber of books per log limit has been reached .  If so, then it will output the
    results to a csv file.
    '''
    
    book_count = 0
    book_in_master = False
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
            #book_list = book_dict.keys()
            master_book_list.append(book_title)
            #log(output_file, f'{book_title}, {rating}, {price}, {in_stock}')
        else:
            book_in_master = True
        
    if len(book_list) == book_limit:
        save_the_master_list(master_book_list)
        save_the_book_list(book_list)
        write_to_log(book_dict)
    else:
        # we have less books that need to be written to the & should be stored 
        pass
    if book_list:
        logging.info(
            f'Page#:{page_num}, books found in master:{book_in_master}, new books added: {len(book_list)}')
        logging.info(
            f'Page#:{page_num}, len of book_list:{len(book_list)}, len of master:{len(master_book_list)}')
        print(f'Titles worked{book_list}')
    return


def process_data(soup, book_limit=20):
    '''This function processes each page and determines if the books have already been processed, 
    and if ht enumber of books per log limit has been reached .  If so, then it will output the
    results to a csv file.
    '''

    book_count = 0
    book_in_master = False
    
    book_dict = {}
    products = soup.findAll('article', attrs={'class': 'product_pod'})
    #print(products)
    for product in products:
        book_title = product.h3.a.get('title')
        rating = product.p.get('class')[1]
        price = product.find(
            'div', attrs={"class": "product_price"}).p.text[1:]
        in_stock = product.find(
            'p', attrs={"class": "instock availability"}).text.strip()
        book_count += 1
        print(f'{book_count}: {book_title}, {rating}, {price}, {in_stock}')
        if book_count > book_limit:
            break
        if book_title not in master_book_list:
            book_dict[book_title] = [book_title, rating, price, in_stock]
            book_list.append(book_title)
            #book_list = book_dict.keys()
            master_book_list.append(book_title)
            #log(output_file, f'{book_title}, {rating}, {price}, {in_stock}')
        else:
            book_in_master = True
            logging.info(f'Title already exists in the master list: {book_title}')
            logging.info(f'master list = {master_book_list}')

    if len(book_list) <= book_limit:
        save_the_master_list(master_book_list)
        save_the_book_list(book_list)
        write_to_log(book_dict)
    
    if book_list:
        logging.info(
            f'Page#:{page_num}, books found in master:{book_in_master}, new books added: {len(book_list)}')
        logging.info(
            f'Page#:{page_num}, len of book_list:{len(book_list)}, len of master:{len(master_book_list)}')
        print(f'Titles worked{book_list}')
        
    return True

def save_the_master_list(bk_list):
    """save the processed data to a csv and the database"""
    
    pickled_data = pickle.dumps(bk_list)
    cursor.execute(
    '''insert into master_list(id, pickled_data) values(?, ?)''',
    (page_num, pickled_data))
    
    db.commit()
    
    return


def save_the_book_list(bk_list):
    """save the processed data to a csv and the database"""
    
    pickled_data = pickle.dumps(bk_list)
    file_data = f'{os.getcwd()}\\{output_file}'
    #date = datetime.datetime.today()
    run_date = f'{TODAY.year}-{TODAY.month}-{TODAY.day}'
    cursor.execute(
    '''insert into Book_list(id, pickled_data, data_file_name, job_run_date) 
    values(?, ?, ?, ?)''',
    (page_num, pickled_data, file_data, run_date))
    
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
        mList = pickle.loads(pickled_data)
        #logging.debug(f'mlist = {mList}')
        return mList
    except:
        #raise
        return []
    

def get_next_page(soup):
    '''Get the link for the next page on the site'''
     
    try:
        next_link = soup.findAll('li', attrs={'class': 'next'})[
            0].find('a').get('href').rstrip().split('/')[-1]
        
        if next_link != None or len(next_link)>1:
            next_page = f'{base_url}catalogue/{next_link}'
    except:
        next_page = None
    
    print(f'next link = {next_page}')
    return next_page

def set_next_url(url):
    '''stores the next page for the site'''
    
    cursor.execute(
      '''update book_list set next_url = ? where id=?''', (url, page_num))
    db.commit()
    logging.info(f'next url {url} has been set for page: {page_num}')

    return

def get_next_url():
    '''retrieve the next url to be processed'''
    
    logger.info(f"Loading next url for page# {page_num}")
    sel_query = f'select next_url from book_list where id = {page_num-1}'

    try:
        next_url = cursor.execute(sel_query).fetchall()[0][0]
    except:
        next_url = f'{base_url}'
    
    logging.info(f'next_url = {next_url}')
    return next_url

def send_to_cloud():
    pass

if __name__ == '__main__':

    base_url = 'http://books.toscrape.com/'

    TODAY = datetime.datetime.today()
    TIMESTAMP = f'{TODAY.month}-{TODAY.day}-{TODAY.year}-{TODAY.hour}-{TODAY.minute}'

    page_num = generate_run_number()
    output_file = f'roy_mathew_books_{page_num}_of_50_{TIMESTAMP}.csv'
    print(output_file)
    
    page_to_log_dict = {}
    page_to_log_dict[page_num] = output_file
    book_limit = 20
    print(page_to_log_dict)
    
    
    url_to_find = get_next_url()  # f'{base_url}'

    #keeps track of all books that have been found, this is meant to be persistent list
    master_book_list = load_master_list()  # [] keeps track of all books found
    book_list = []  # keeps track of all current books not in the master list

    max_page_search = 1#99
    page_found = 1
    next_page_link = None
    all_pages_processed = False
    
    while True:    
      
        # This will be used to ensure that the job will terminate if it's already been run successfully
        if (url_to_find == None) or (len(url_to_find) <= 1):
            all_pages_processed = True
            print('no more links')
            break
      
        response = get_url(url_to_find)
        soup = BeautifulSoup(response.text, 'html.parser')
        processed = process_data(soup)
        logging.info(f'len of the book_list afer it\'s been processed: {len(book_list)}')
        url_to_find =  get_next_page(soup)
        set_next_url(url_to_find)
        #if (url_to_find == None) or (len(url_to_find) <= 1) or (page_found >= max_page_search):
        #if (url_to_find == None) or (len(url_to_find)<=1) or len(book_list) >= book_limit:
        #if (url_to_find == None) or (len(url_to_find)<=1): 
        if processed:
            response.close()
            break
        
        page_found += 1
    
    if all_pages_processed:
        send_to_cloud()
