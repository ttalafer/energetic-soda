#IMPORTS
import sys
import pandas as pd
import uuid
import os

from datetime import datetime
from sqlalchemy import create_engine,Engine
from dotenv import load_dotenv,find_dotenv

#INITIALIZATION
def get_required_env(arg:str)->str:
    result: str = os.getenv(arg)
    if result is None:
        log(f"{arg} is not set")
        exit(1)
    return result
def log(message) -> None:
    print(f"{datetime.now()}: {message}", flush=True)


load_dotenv(find_dotenv())

PG_URL: str = get_required_env('PG_URL')
SS_URL: str = get_required_env('SS_URL')
DB: str = get_required_env('DB') # use 'SS' for MSSQL connection and 'PG' for PostgreSQL

path = './files/'
spread_sheet = 'La Liga.xlsx'
csv = 'investments.csv'

engine:Engine = (create_engine(PG_URL) if DB == 'PG' else create_engine(SS_URL)) if (PG_URL or SS_URL) else None

#PROCESSING
def load_league_to_lake():
    sheets = pd.ExcelFile(path+spread_sheet,engine='openpyxl').sheet_names
    id_file = str(uuid.uuid4())
    data = []

    for sheet in sheets:
        #print(f"sheet_name:{sheet}")
        df = pd.read_excel(path+spread_sheet,sheet_name=sheet,engine='openpyxl',dtype='str')
        df['season'] = sheet
        df['Team 1'] = df['Team 1'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        df['Team 2'] = df['Team 2'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        data.extend([x for x in df.to_json(orient='records',date_format='iso',lines=True).split('\n') if x != ''])

    final = pd.DataFrame()
    final['document'] = data
    final['id_file'] = id_file
    final['model'] = 'LEAGUE'

    try:
        final.to_sql('lake',engine, schema='doc',method='multi',if_exists='append',index=False,chunksize=500)
        log(f"League file has finished loading with id_file: {id_file}")
    except(Exception) as e:
        log(f"set_profile error: {str(e)}")

def load_invest_to_lake():
    df = pd.read_csv(path+csv,dtype='str')
    id_file = str(uuid.uuid4())
    data = []
    data.extend([x for x in df.to_json(orient='records',date_format='iso',lines=True).split('\n') if x != ''])
    
    final = pd.DataFrame()
    final['document'] = data
    final['id_file'] = id_file
    final['model'] = 'INVEST'
    
    try:
        final.to_sql('lake',engine, schema='doc',method='multi',if_exists='append',index=False,chunksize=500)
        log(f"League file has finished loading with id_file: {id_file}")
    except(Exception) as e:
        log(f"set_profile error: {str(e)}")

def trigger_data_processing():
    with engine.connect() as cnx:
        call = ('CALL' if DB == 'PG' else 'EXEC')
        cnx.execute(f'{call} process_data()')
            
def get_season_data(season:str):
    season = season.strip()
    log(season)
    if not season: 
        log(f'No season was provided')
        return
    file_name = path+f'season{season}.csv'
    res = pd.read_sql_query(f"SELECT * FROM get_season('{season}')",engine,dtype='str')
    print(res)
    res.to_csv(file_name,index=False)
    log(f'Results were also saved at {file_name}')

def run_load():
    load_league_to_lake()
    load_invest_to_lake()
    trigger_data_processing()

if __name__ == "__main__":
    if (len(sys.argv) > 2):
        globals()[sys.argv[1]](sys.argv[2])
    else:
        globals()[sys.argv[1]]()