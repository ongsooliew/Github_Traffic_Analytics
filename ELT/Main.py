#Pipeline to bring data into Postgres

from github import Github
import os
import pandas as pd
import json
from datetime import datetime
import time
from dateutil.relativedelta import relativedelta
from dateutil import parser
import yaml
from sqlalchemy import create_engine

#Timestamp of elt job
timestamp = int(time.mktime(datetime.now().timetuple()))

#Read in parameters and credentials
with open('../config.yaml', 'r') as f:
    config = yaml.load(f)
    GithubToken= config['Github']['Token']
    username= config['Postgres']['username']
    password= config['Postgres']['password']
    port= int(config['Postgres']['port'])
    database= config['Postgres']['database']
    query_date= config['Parameters']['query_date']
    
    
def make_api_call(timestamp=timestamp, GithubToken=GithubToken):
    #Get GMT Datetimes
    gmt=time.gmtime()
    gmt=pd.to_datetime(time.strftime('%Y-%m-%dT%H:%M:%SZ', gmt))
    
    SixMonth = gmt - relativedelta(months=6)
    OneWeek = gmt - relativedelta(weeks=1)

    #Linkup with GitHub API
    token = os.getenv('GITHUB_TOKEN', GithubToken)
    g = Github(token)
    repo = g.get_organization('apache').get_repo('airflow')
    commits= repo.get_commits()
    
    #Make API call and Save Json File
    lst=[]
    count=0
    for c in commits:
        dt= parser.parse(c.last_modified).date()
        if dt>= SixMonth:
            lst.append(c.commit.raw_data)
        else:
            break
        count+=1
        print(count)
        print(dt)
    
    JsonString= json.dumps(lst)
    with open('../Json Files/{}.json'.format(str(timestamp)), 'w') as f:
        json.dump(JsonString, f)
        
    return str(timestamp), str(datetime.date(SixMonth))


def parse_and_load(timestamp=timestamp, username=username, 
                   password=password, port=port,database=database):
    
    #open Json file
    with open('../Json Files/{}.json'.format(str(timestamp)), 'r') as f:
        data=json.load(f)
        data= json.loads(data)
        
    #Parse and enrich
    df=pd.json_normalize(data, sep='')
    df1= df[['sha', 'authorname', 'authoremail', 'authordate', 'committername', 'committeremail', 'committerdate']]
    df1['authordate']= pd.to_datetime(df1['authordate'])
    df1['committerdate']= pd.to_datetime(df1['committerdate'])
    df1['elt_timestamp']=timestamp
    
    #Connect to postgres
    postgres_string='postgresql://{username}:{password}@localhost:{port}/{database}'.format(
                username=username, 
                password= password,
                port=port,
                database=database)
    engine = create_engine(postgres_string)
    conn = engine.raw_connection()
    
    #Insert records
    df1.to_sql('raw_commit_traffic', engine, if_exists='append',index=False)
    conn.commit() 
    conn.close()
    
def data_cleaning(query_date, username=username, 
                   password=password, port=port,database=database):
    
    #open SQL files
    with open('SQL/drop_T2_materialized_view.sql', 'r') as f:
        drop_view_query= f.read()
        
    with open('SQL/T2_commit_traffic.sql', 'r') as f:
        create_view_query= f.read()
        
    #Connect to postgres
    postgres_string='postgresql://{username}:{password}@localhost:{port}/{database}'.format(
                username=username, 
                password= password,
                port=port,
                database=database)
    engine = create_engine(postgres_string)
    conn = engine.raw_connection()
    
    #Refresh materialized view
    engine.execute(drop_view_query)
    engine.execute(create_view_query.format(query_date))
    
    conn.commit() 
    conn.close()
    
    
    
if __name__ == "__main__": 
    
    timestamp_str, SixMonth_str = make_api_call() 
    parse_and_load(timestamp= timestamp_str)

    if query_date != '': #Use the query date provided by user, if any
        data_cleaning(query_date)
    else:
        data_cleaning(SixMonth_str) #By default, the earliest date of the data would be 6months from today
