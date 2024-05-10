from datetime import datetime, timedelta

from airflow.decorators import dag, task

from dotenv import load_dotenv
import os
import json
import requests
from bs4 import BeautifulSoup
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
from elsapy.elsprofile import ElsAuthor
from elsapy.elsdoc import AbsDoc

load_dotenv()

HEADERS = {
            "X-ELS-APIKey"  : os.environ['SCOPUS_API_KEY'],
            "User-Agent"    : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            "Accept"        : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
            }
HEADERS_2 = {
            "X-ELS-APIKey"  : os.environ['SCOPUS_API_KEY'],
            "User-Agent"    : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            "Accept"        : 'application/json'
            }
CLIENT = ElsClient(os.environ['SCOPUS_API_KEY'])

def get_detail_page(uri):
    response = requests.get(uri, headers = HEADERS)
    return response
    
def affiliation_id_serch(schoolname, client,year,count):
    """Search affiliation ID by name
    client - object of the ElsClient class  """
    result = []
    school_srch = ElsSearch(f' AFFIL({schoolname}) AND PUBYEAR = {year}','scopus')
    # print(school_srch._uri)
    res =requests.get(school_srch._uri,
                  headers = HEADERS_2)
    res_json = res.json()
    result = result + res_json['search-results']['entry']
    while len(result) < count:
        next_uri = None
        for e in res_json['search-results']['link']:
            if e['@ref'] == 'next':
                next_uri = e['@href']
                break
        if next_uri is None:
            break
        res =requests.get(next_uri,
                  headers = HEADERS_2)
        res_json = res.json()
        result = result + res_json['search-results']['entry']
    return result

def get_journal_info(soup):
    journal_info = soup.select_one('span#journalInfo')
    if journal_info is None:
        return None
    return journal_info.text.replace('\xa0',' ')

def get_author_list(soup):
    author_list = []
    authors = soup.select('#authorlist span.previewTxt')
    if authors is None:
        return None
    for e in authors:
        author_list.append(e.text)
    return author_list

def get_affiliation_list(soup):
    affiliation_list = []
    affiliations = soup.select('#affiliationlist li')
    if affiliations is None:
        return None
    for e in affiliations:
        affiliation_list.append(e.text)
    return affiliation_list

def get_abstract(soup):
    abstract = soup.select_one('section#abstractSection p')
    if abstract is None:
        return None
    return abstract.text

def get_author_keywords(soup):
    author_keywords = []
    data = soup.select('section#authorKeywords span')
    if data is None:
        return []
    for e in data:
        author_keywords.append(e.text)
    return author_keywords

def get_index_keywords(soup):
    index_keywords = []
    data = soup.select('section#indexedKeywords span')
    if data is None:
        return []
    for e in data:
        index_keywords.append(e.text)
    return index_keywords
    
def get_reference_info(soup):
    ref_dict = {}
    ref_info = soup.select('section#referenceInfo li')
    if ref_info is None:
        return None
    for e in ref_info:
        ref_dict[e.select_one('strong').text.strip()[:-1]] = e.text.split(':')[1].strip()
    return ref_dict

default_args = {
    'owner':'Soei_Data',
    'reties':2,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='scopus_extract_dag',
    default_args = default_args,
    start_date=datetime(2024,5,8),
    schedule=None,
    catchup=False)
def query_scopus():
    @task()
    def get_journal_list(affiliation, year, num):
        journal_list = affiliation_id_serch(affiliation, CLIENT, year, num)
        path = f'/opt/airflow/extract_data/{affiliation}/{year}'
        isExist = os.path.exists(path)
        if not isExist:
            os.makedirs(path)
        with open(f'{path}/{affiliation}_jornal_list_{year}.json','w') as journal_list_file:
            json.dump(journal_list, journal_list_file)
        return journal_list
        
    @task()
    def get_journal_detail(journal_list, affiliation, year):
        count = 0
        bronze_data = []
        exclude = {'@_fa', 'link','prism:url'}
        for journal in journal_list:
            uri = journal['link'][2]['@href']
            data_to_scrape = {}
            for key in journal.keys():
                if key not in exclude:
                    data_to_scrape[key] = journal[key]
            res = get_detail_page(uri)
            soup = BeautifulSoup(res.content, "html.parser")
            data_to_scrape['journalInfo'] = get_journal_info(soup)
            data_to_scrape['authorlist'] = get_author_list(soup)
            data_to_scrape['affiliationlist'] = get_affiliation_list(soup)
            data_to_scrape['abstract'] = get_abstract(soup)
            data_to_scrape['authorKeywords'] = get_author_keywords(soup)
            data_to_scrape['indexedKeywords'] = get_index_keywords(soup)
            data_to_scrape['referenceInfo'] = get_reference_info(soup)

            path = f'/opt/airflow/extract_data/{affiliation}/{year}'
            isExist = os.path.exists(path)
            if not isExist:
                os.makedirs(path)
            with open(f'{path}/{str(year) + "0"*(5 - len(str(count))) + str(count) }.json', 'w') as journal_file:
                json.dump(data_to_scrape, journal_file)
            count += 1
            bronze_data.append(data_to_scrape)
        return bronze_data

    @task()
    def transform_silver_data(bronze_data, affiliation, year):
        author_key = {}
        index_key = {}
        for data in bronze_data:
            if data['authorKeywords'] is not None:
                for key in data['authorKeywords']:
                    if author_key.get(key) is not None:
                        author_key[key] += 1
                    else:
                        author_key[key] = 1
            if data['indexedKeywords'] is not None:
                for key in data['indexedKeywords']:
                    if index_key.get(key) is not None:
                        index_key[key] += 1
                    else:
                        index_key[key] = 1
        index = sorted([(k, index_key[k]) for k in index_key.keys()], key=lambda x:x[1], reverse=True)
        author = sorted([(k, author_key[k]) for k in author_key.keys()], key=lambda x:x[1], reverse=True)
        
        path = f'/opt/airflow/extract_data/{affiliation}/{year}'
        isExist = os.path.exists(path)
        if not isExist:
            os.makedirs(path)
        with open(f'{path}/{affiliation}-{year}-keywords.json', 'w') as transform_file:
            json.dump({
                'authorKeywords': author,
                'indexedKeywords': index
            }, transform_file)
    
    @task()
    def start():
        print("Starting....")
        
    AFFILIATION = "kasetsart"
    YEAR = 2018
    NUM = 500
    start()
    journal_list = get_journal_list(AFFILIATION,YEAR,NUM)
    bronze_data = get_journal_detail(journal_list, AFFILIATION,YEAR)
    transform_silver_data(bronze_data, AFFILIATION, YEAR)
scopus_dag = query_scopus()