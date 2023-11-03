import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine
from sqlalchemy.types import *
    
def Extract_Data(source):
    return pd.read_csv(source)

def transform_data(data):

    data_trf = data.copy()
    data_trf["Sex"] = data_trf["Sex upon Outcome"].replace("Unknown", np.nan)
    data_trf['ID'] = data_trf['Animal ID']
    data_trf['outcome'] = data_trf['Outcome Type']
    data_trf['Animal'] = data_trf['Animal Type']
    data_trf['Dt'] = data_trf['DateTime']
    data_trf['recorded_name'] = data_trf['Name']
    cols_to_drop = ['Outcome Type','Animal Type','DateTime','Animal ID','Name']
    data_trf.drop(cols_to_drop,axis=1,inplace=True)

    df_outcome = data_trf['outcome'].drop_duplicates().reset_index()
    df_outcome['outcome_id'] = df_outcome.index + 1
    df_outcome.drop('index',axis=1,inplace=True)
    data_trf = data_trf.merge(df_outcome)
    df_animal = data_trf['Animal'].drop_duplicates().reset_index()
    df_animal['Animal_id'] = df_animal.index + 1
    df_animal.drop('index',axis=1,inplace=True)
    data_trf = data_trf.merge(df_animal)
    df_breed = data_trf['Breed'].drop_duplicates().reset_index()
    df_breed['Breed_id'] = df_breed.index + 1
    df_breed.drop('index',axis=1,inplace=True)
    data_trf = data_trf.merge(df_breed)
    df_date = data_trf['Dt'].drop_duplicates().reset_index()
    df_date['date_id'] = df_date.index + 1
    df_date.drop('index',axis=1,inplace=True)
    data_trf[["Month", "Year"]] = data_trf["Dt"].str.split(" ", n=1, expand=True)
    data_trf[["Month", "Year"]] = data_trf[["Month", "Year"]].drop_duplicates()

    data_trf = data_trf.merge(df_date)

    cols = ['recorded_name','date_id','outcome_id','Animal_id','Sex','Breed_id','Color','ID']

    data_trf = data_trf[cols]
    
    return data_trf, df_animal,df_date,df_outcome,df_breed

def load_data(data):
    
    db_url = "postgresql+psycopg2://srs:sawhneyy@db:5432/shelter"
    engine = create_engine(db_url)
    data_trf, df_animal,df_date,df_outcome,df_breed = data
    data_trf.to_sql(name= 'ADOPTION',con=engine, if_exists='append',index=False)
    df_animal.to_sql(name='ANIMAL',con=engine, if_exists='append',index=False)
    df_outcome.to_sql(name='OUTCOME',con=engine, if_exists='append',index=False)
    df_breed.to_sql(name='BREED',con=engine, if_exists='append',index=False)
    df_date.to_sql(name='DATE',con=engine, if_exists='append',index=False)
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    args = parser.parse_args()
    print("Beginning...")
    df = Extract_Data(args.source)
    new_df = transform_data(df)
    load_data(new_df)
    print("Lets go!!!")