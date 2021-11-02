#pipeline to generate analytics outcome
import pandas as pd
import yaml
from sqlalchemy import create_engine
import seaborn as sns


with open('../config.yaml', 'r') as f:
    config = yaml.load(f)

    username= config['Postgres']['username']
    password= config['Postgres']['password']
    port= int(config['Postgres']['port'])
    database= config['Postgres']['database']        


def top5_committers(username=username, password=password, 
                    port=port,database=database):
    
    #open SQL files
    with open('SQL/top_5_committers.sql', 'r') as f:
        top_committers_query= f.read()
        
    #Connect to postgres
    postgres_string='postgresql://{username}:{password}@localhost:{port}/{database}'.format(
                username=username, 
                password= password,
                port=port,
                database=database)
    engine = create_engine(postgres_string)
    conn = engine.raw_connection()
        
    df= pd.read_sql(top_committers_query, engine)
    df.to_excel('../Results/Question1.xlsx', index=False)
    conn.close()
    

def longest_streak(username=username, password=password, 
                    port=port,database=database):
    
    #open SQL files
    with open('SQL/longest_streak.sql', 'r') as f:
        longest_streak_query= f.read()
        
    #Connect to postgres
    postgres_string='postgresql://{username}:{password}@localhost:{port}/{database}'.format(
                username=username, 
                password= password,
                port=port,
                database=database)
    engine = create_engine(postgres_string)
    conn = engine.raw_connection()
        
    df= pd.read_sql(longest_streak_query, engine)
    
    #Count streak
    result_df= pd.DataFrame()
    previous_dt=0
    for group_name,group_df in df.groupby(['committername', 'committeremail']): #Combination of name and email to identify individuals
        streak_lst=[]
        for index, row in group_df.reset_index().iterrows(): 
            if index == 0: #First record always start off with a streak of 1
                streak_count=1
            elif (row['dt'] - previous_dt).days == 1: #+1 streak, if developer commit the next day
                streak_count+=1
            else:
                streak_count=1 #otherwise, streak goes back to 1 again
            
            streak_lst.append(streak_count)
            previous_dt= row['dt']
            
        group_df['streak']= streak_lst
        result_df=result_df.append(group_df)

    #identify developer with the longest streak
    grouped_df= result_df.groupby(['committername', 'committeremail']).agg({'streak':max})
    output= grouped_df[grouped_df['streak']== grouped_df['streak'].max()].reset_index()
    
    output.to_excel('../Results/Question2.xlsx', index=False)
    conn.close()
    
def generate_heatmap(username=username, password=password, 
                    port=port,database=database):
    
    #open SQL files
    with open('SQL/heatmap.sql', 'r') as f:
        heatmap_query= f.read()
        
    #Connect to postgres
    postgres_string='postgresql://{username}:{password}@localhost:{port}/{database}'.format(
                username=username, 
                password= password,
                port=port,
                database=database)
    engine = create_engine(postgres_string)
    conn = engine.raw_connection()
        
    df= pd.read_sql(heatmap_query, engine)
    
    #Transform dataframe for charting
    df.set_index('weekday', inplace=True)
    pivot= pd.get_dummies(df['category'])
    heatmap_df= pivot.groupby('weekday').sum()
    heatmap_df=heatmap_df.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
    
    #Plot heatmap with Seaborn
    hm=sns.heatmap(heatmap_df)
    fig = hm.get_figure()
    fig.savefig('../Results/Question3.jpg') 

    conn.close()
    
    
if __name__ == "__main__": 
    
    top5_committers()
    longest_streak()
    generate_heatmap()