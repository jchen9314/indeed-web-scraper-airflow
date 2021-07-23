import datetime as dt
import pandas as pd
import numpy as np
import time
import re
import warnings
warnings.filterwarnings("ignore")

# Web Scraping
import requests 
from bs4 import BeautifulSoup

# text processing
import spacy
# spacy.cli.download("en")
nlp = spacy.load('en_core_web_sm')

# database
import sqlite3

# airflow
from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator


# global variables
HEADER = {'user_agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'}
DELAYS = [7, 4, 6, 2, 10, 19]


def get_job_links(position, location, num_pages=20):
    """
    Given position and location, number of pages, find the job links
    """
    job_list = []

    base_url = "https://ca.indeed.com"
    
    # Scrape 200 jobs 
    for i in range(num_pages):
        time.sleep(np.random.choice(DELAYS))
        print(f'Scraping page {i}...')
        jobs_html = requests.get(base_url + "/jobs?q="\
                                 + position + "&l=" + location\
                                 + "&start="+str(i*10), headers=HEADER)
        job_bs = BeautifulSoup(jobs_html.text)
        # one mosaic-provider-jobcards in each page
        jobcards = job_bs.find('div', {'id':'mosaic-provider-jobcards'})
        if not jobcards:
            continue
        for job_link in jobcards.find_all('a', {'rel':"nofollow"}):            
            job_list.append(base_url + job_link.get('href'))
    
    return job_list

def get_job_desc(job_link):
    """Find job related contents given a job link"""
    time.sleep(np.random.choice(DELAYS))
    job_page = requests.get(job_link, headers=HEADER)
    bs_page = BeautifulSoup(job_page.text)
    
    title = bs_page.find('h1').text
    
    # company, location
    company_info = str(bs_page.find('div', {'class':'jobsearch-JobInfoHeader-subtitle'})).split('<div>')
    # remove html tags
    cleaner = re.compile('<.*?>')
    company = re.sub(cleaner, '', company_info[0])
    location = re.sub(cleaner, '', company_info [1])
    
    # salary
    metadata = str(bs_page.find('div', {'class':'jobsearch-JobMetadataHeader-item'})).split('</span>')
    if len(metadata) > 2:
        salary = re.sub(cleaner, '', metadata[0])
    else:
        salary = None
        
    # job description
    jd = bs_page.find('div', {'class':'jobsearch-JobComponent-description'}).text
    
    return [title, company, location, salary, jd, job_link]

def clean_text(text,  
               irrelevant_pos = ['ADV','PRON','CCONJ','PUNCT','PART','DET','ADP','SPACE','VERB']): 
    """
    Given text, and irrelevant_pos carry out preprocessing of the text 
    and return a preprocessed string. 
    
    Keyword arguments:
    text -- (str) the text to be preprocessed
    irrelevant_pos -- (list) a list of irrelevant pos tags
    
    Returns: (str) the preprocessed text
    """
    # Remove distracting characters
    text = re.sub(r'\s+', ' ', text)
    
    text = re.sub(r'''[\*\~]+''', "", text)
    
    # Customized Stopwords 
    # The job postings contains some French as these are jobs in Canada
    stopwords = [
    "jobs", "apply", "email", "password", "new", "please", "required",
    "opportunities", "skills", "sign", "talent", "follow",  "job", "work",
    "click", "home", "next", "us", "start", "take", "enter", "date", "people",
    "career", "hours", "week", "type", "attach", "resume", "search", "advanced",
    "privacy", "terms", "find", "years", "experience", "find", "title", "keywords",
    "company", "indeed", "help", "centre", "cookies", "post", "(free)", "industry",
    "employment", "ago", "save", "forums", "browse", "employer", "browse", "city",
    "province", "title", "api", "review", "application", "help", "instructions", 
    "applying", "full", "time", "opportunity", "team", "join", "resumes", "employers",
    "rights", "reserved", "create", "north", "america", "toronto", "interview", "cover",
    "letter", "30+", "days", "inc", "trends", 'de', 'le', 'ericsson', 'dans', 'des', 
    'working', 'etc.', 'canada', 'les', 'employees', 'accommodation',
    'et', 'montréal','en','mi','yoga','à','équipe','vancuover','se','se se' 
    ]
    
    text = " ".join([i.lower() for i in text.split() if i.lower() not in stopwords])
    
    doc = nlp(text)
    clean_text = []
    
    for token in doc:
        if (token.is_stop == False # Check if it's not a stopword
            and token.is_alpha # Check if it's an alphanumerics char 
            and token.pos_ not in irrelevant_pos): # Check if it's irrelevant pos
            clean_text.append(str(token))
    return " ".join(clean_text)

def get_scrapping_results(**kwargs):
    job_list = get_job_links(position="data+scientist", location='Canada', num_pages=20)

    results = []

    for job in job_list:
        print(job)
        try:
            result = get_job_desc(job)
            results.append(result)
        except:
            continue

    df = pd.DataFrame(results, columns=['JobTitle', 'Company', 'Location', 'Salary', 'Description', 'JobLink'])
    return df

def get_clean_df(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(key=None, task_ids='get_scrapping_results')

    df['Description'] = df['Description'].apply(clean_text)
    df['Company'] = [re.sub('\d+', '', x).replace('reviews', '') for x in df['Company']]
    return df

def upload_data(**kwargs):

    ti = kwargs['ti']
    df = ti.xcom_pull(key=None, task_ids='get_clean_df')

    hook = SqliteHook()
    conn = hook.get_conn()
    
    df.to_sql('JobPosting', 
              con=conn, 
              if_exists='replace', 
              index=False)
    return True

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2021, 1, 1, 00, 00, 00),
    'concurrency': 1,
    'retries': 1,
    'retry_delay': dt.timedelta(hours=1)
}

with DAG(dag_id='web_scrapper',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 0 * * 0', # weekly run
         ) as dag:
    opr_scrape_web = PythonOperator(task_id='get_scrapping_results',
                                    python_callable=get_scrapping_results,
                                    provide_context=True)

    opr_clean_result = PythonOperator(task_id='get_clean_df',
                                      python_callable=get_clean_df, provide_context=True)
    opr_store_data = PythonOperator(task_id='upload_data',
                                    python_callable=upload_data, provide_context=True)
    opr_email = EmailOperator(
        task_id='send_email',
        to='xxxxxx', # replace it with email address
        subject='Airflow Finished',
        html_content=""" <h3>DONE</h3> """,
        dag=dag
    )

opr_scrape_web >> opr_clean_result >> opr_store_data >> opr_email