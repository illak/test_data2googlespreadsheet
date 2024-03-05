import mysql.connector as connection
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from sqlalchemy.sql import text

import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
from google.oauth2 import service_account

load_dotenv()

HOST = os.getenv('HOST_DB')
PORT = os.getenv('PORT_DB')
DB = os.getenv('DB')
USER = os.getenv('USER_DB')
PASS = os.getenv('PASS_DB')


def getDFfromDB(query_db):

    try:

        url = f"mysql+mysqlconnector://{USER}:{PASS}@{HOST}:{PORT}/{DB}"

        engine = sqlalchemy.create_engine(url)

        with engine.connect() as conn:

            query = conn.execute(text(query_db))
            df = pd.DataFrame(query.fetchall())

        return df

    except Exception as e:
        print(str(e))


def getDataFromQuery():
    sql_directivos = '''
            SELECT dir.nombre, dir.apellido,
             if(dir.mail_2 <> "", CONCAT_WS(",",mail_1,mail_2),mail_1) as email,
             dir.nivel_es, LEFT(dir.grupo_destinatario,3) as fuente

            FROM campanias_grupo_directivos as dir

            WHERE mail_1 <> "";
         '''

    df_directivos = getDFfromDB(sql_directivos)

    print(df_directivos)

    return df_directivos


#change this by your sheet ID
SAMPLE_SPREADSHEET_ID_input = os.getenv('SPREADSHEET_ID')

#change the range if needed
SAMPLE_RANGE_NAME = 'A1:AA8000'

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    #print(SCOPES)
    
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            #flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            #cred = flow.run_local_server()
             cred = service_account.Credentials.from_service_account_file(client_secret_file)


        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        #return service
    except Exception as e:
        print(e)
        #return None


# change 'my_json_file.json' by your downloaded JSON file.
Create_Service('my_json_file.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])
    
def Export_Data_To_Sheets():


    testDf = getDataFromQuery()

    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=SAMPLE_RANGE_NAME,
        body=dict(
            majorDimension='ROWS',
            values=testDf.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

Export_Data_To_Sheets()