#!/usr/bin/env python
#coding=utf8
'''
Author:Xunzhi Wang
Email:wxzpursue@gmail.com
Usage: 
Change variable 'tieba_name'
python crawl-tieba.py --csv

Requirement: Need beautifulsoup4 library
Function: Save all posts in a baidu tieba in the form of
"title","id","number of replies","url","author","creation time","last replyer",
"last reply time","good or not","span (time between creation and last reply)",
"exist time"
'''
#---------------------------------import---------------------------------------
import argparse
import re
from multiprocessing import Pool
import requests
import bs4
import io
import time
import datetime
#------------------------------------------------------------------------------
tieba_name = 'YOUR_TIEBA_NAME' # CHANGEME
root_url = 'http://tieba.baidu.com'
index_url = root_url + '/f?ie=utf-8&kw='+tieba_name

today = datetime.date.today() #422
reply_year = int(today.strftime("%Y")) 
# CHANGEME if latest reply date is more than a year ago

def get_page_urls():
    response = requests.get(index_url)
    '''
    # write test response
    f = io.open("response", 'w', encoding='utf8')
    f.write(response.text)
    f.close();
    '''
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    '''
    # read test response
    f = io.open("response", 'r', encoding='utf8')
    response = f.read()
    f.close();
    soup = bs4.BeautifulSoup(response, "html.parser")
    '''
    #print(soup.prettify())
    # get the posts count
    posts = int(soup.select('div.th_footer_l span')[0].contents[0])
    # generate urls per 50 posts as default
    urls = []
    for pn in range(0,posts,50):
        urls.append(index_url+'&pn='+str(pn)) 
    return urls
    # . for class

'''
def len_assert(data,name):
    if len(data[name])!=50:
        print(name+str(len(data[name])))
'''    
def get_data(page_url):
    results = []
    #response = requests.get(index_url+'&pn='+str(0))
    response = requests.get(page_url)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    '''
    f = io.open("response", 'r', encoding='utf8')
    response = f.read()
    f.close();
    soup = bs4.BeautifulSoup(response, "html.parser")
    '''
    for li in soup.select('ul#thread_list li.j_thread_list'):
        data = {}        
        data['addr'] = li.select('a[href^=/p]')[0]['href']
        data['title'] = li.select('a[href^=/p]')[0].get_text()
        data['author'] = li.select('span.tb_icon_author')[0].get_text().strip()
        data['create_time'] = li.select('span.is_show_create_time')[0].get_text().strip()
        data['reply_number'] = li.select('span.threadlist_rep_num')[0].get_text().strip()
        # ip author: do not have 'a'
        if len(li.select('span.j_replyer'))>0: 
            data['last_replyer'] =  li.select('span.j_replyer')[0].get_text().strip()
            data['last_reply_time'] = li.select('span.j_reply_data')[0].get_text().strip()
        else:
            # top posts may not have replyer shown
            data['last_replyer'] = ''
            data['last_reply_time'] = ''
        if len(li.select('i.icon-good'))>0:
            data['good'] = 1
        else:
            data['good'] = 0
        results.append(data)
    #soup.select('i.icon-good')?
    #repr() no change of unicode
    return results

def parse_args():
    parser = argparse.ArgumentParser(description='Show tieba statistics.')
    parser.add_argument('--csv', action='store_true', default=False,
                        help='output the data in CSV format.')
    parser.add_argument('--max', metavar='MAX', type=int, help='show the latest MAX entries only.')
    # for paralellism, not implemented
    parser.add_argument('--workers', type=int, default=2,
                       help='number of workers to use, 2 by default.')
    return parser.parse_args()
            
def find_create_date(time):
    global today
    date = time.split('-')
    if len(date)<=1:
        result = today
    else:
        date = [int(i) for i in date]
        if date[0]<=12:    
            # m-d
            result = datetime.date(int(today.strftime("%Y")), date[0], date[1])
        else:
            # y-m,use 1st day
            result = datetime.date(date[0],date[1],1) 
    return result

def find_reply_date(time):
    global today
    global reply_year
    date = time.split('-')
    if len(date)<=1:
        result = datetime.date(reply_year, int(today.strftime("%m")), 
                    int(today.strftime("%d")))
    else:
        date = [int(i) for i in date]
        if date[0]<=12:    
            # m-d
            result = datetime.date(reply_year, date[0], date[1])
        else:
            # y-m,use 1st day, not possible?
            result = datetime.date(date[0],date[1],1) 
    return result

def show_stats(options):
    pool = Pool(options.workers)
    #page_urls = ['http://tieba.baidu.com/f?ie=utf-8&kw='+tieba_name+'&pn=3200']
    page_urls = get_page_urls()
    results = {}
    max = options.max
    last_addr = ''
    print('Analyzing ' + str(len(page_urls)) + ' pages')
    global reply_year
    if options.csv:
        f = io.open("results.csv", 'w', encoding='utf8')
        f.write(u'"title","number","reply_num","address","author","create_time","last_replyer","last_reply_time","good","span","exist_time"\n')
    else:
        print("non csv not implemented")
        return
    pre_last_reply_date = [today, today]
    for i in range(len(page_urls)):
        #results.append(get_data(page_urls[i]))
        print(page_urls[i])
        results = get_data(page_urls[i])
        new_start = 0
        for j in range(len(results)):
            # a naive way to remove duplication because of new posts during crawling
            if results[j]['addr'] == last_addr:
                new_start = j+1
                break            
        for j in range(new_start,len(results),1):    
            if max is not None and max < len(i*50+j):
                if options.csv:
                    f.close()
                return
            # days from create to last reply
            last_reply_date = find_reply_date(results[j]['last_reply_time'])
            # reduce year number if date is increasing
            # baidu bug: some posts are not in order of last reply time
            # 3 choices:
            # 1. assume there must be reply in a Nov-Feb period 
            #if last_reply_date > pre_last_reply_date[0] and int(pre_last_reply_date.strftime("%m"))<3 and int(last_reply_date.strftime("%m"))>10:
            # 2. brute force way: if i*50!=650 and i*50!=2750 and ...
            # 3. perhaps should check more neighbors: assume no 2 consecutive bugs
            if last_reply_date > pre_last_reply_date[0] and last_reply_date > pre_last_reply_date[1]:
                reply_year -= 1
                last_reply_date = find_reply_date(results[j]['last_reply_time'])
                print(last_reply_date,pre_last_reply_date)
                print("reply_year "+str(reply_year))
            pre_last_reply_date[1] = pre_last_reply_date[0]
            pre_last_reply_date[0] = last_reply_date
            create_date = find_create_date(results[j]['create_time'])
            span = last_reply_date - create_date
            exist_time = today - create_date
            #print(last_reply_date.strftime("%Y,%m,%d"),create_date.strftime("%Y,%m,%d"),diff.days)
            if options.csv:
                f.write(u'"{0}",{1},{2},"{3}","{4}","{5}","{6}","{7}",{8},{9},{10}\n'.format(
                    results[j]['title'], i*50+j, results[j]['reply_number'],
                    root_url+results[j]['addr'], results[j]['author'],
                    results[j]['create_time'],results[j]['last_replyer'],
                    str(reply_year)+'-'+results[j]['last_reply_time'],
                    results[j]['good'],span.days,
                    exist_time.days))
            if j==len(results):
                last_addr = results[j]['addr']
        '''
        # one time test
        if options.csv:
            f.close()
        return
        '''   
    if options.csv:
        f.close()
###############################################################################
if __name__ == '__main__':
    show_stats(parse_args())

    
