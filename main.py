import mysql.connector as connection
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from sqlalchemy.sql import text

import pickle
from googleapiclient.discovery import build
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


def getDataFromQuery(query):
    
    df = getDFfromDB(query.replace('"', ''))

    #print(df['tipo_carrera'])

    return df


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
    
def Export_Data_To_Sheets(query, id_destino, sheet_name, col_fechas, range):

    testDf = getDataFromQuery(query)
    testDf.fillna('', inplace=True)

    print(col_fechas)

    for col in col_fechas:
        testDf[col] = pd.to_datetime(testDf[col], format='%Y-%m-%d')
        #testDf[col] = testDf[col].dt.strftime('%Y/%m/%d')
        testDf[col] = testDf[col].dt.strftime('%Y-%m-%d')

    #testDf['tipo_carrera'] = testDf['tipo_carrera'].astype(str)


    #print(testDf.dtypes)

    for column in testDf.columns:
        for value in testDf[column]:
            if isinstance(value, set):
                print(f"Found a set-like object in column '{column}'     ->   '{value}'")

    # Limpiamos hoja
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=id_destino, 
                                                          range=sheet_name,
                                                          body=body ).execute( )

    response_date = service.spreadsheets().values().update(
        spreadsheetId=id_destino,
        valueInputOption='RAW',
        range=sheet_name + "!" + range,
        body=dict(
            majorDimension='ROWS',
            values=testDf.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')


def getQueriesInfo():

    queries = []
    
    result = (service.spreadsheets()
                      .values()
                      .get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range="A2:E")
                      .execute())

    rows = result.get("values", [])

    print(f"{len(rows)} rows retrieved")
    for row in rows:
        queries.append({'consulta': row[1], 'destino': row[2], 'hoja': row[3], 
                        'col_fechas': [x.strip() for x in row[4].split(',')]})

    #print(queries)
    return queries

if __name__ == "__main__":
    queries = getQueriesInfo()
    
    for i, query in enumerate(queries):
        print(f"Procesando QUERY: {i} ...")
        Export_Data_To_Sheets(query['consulta'], query['destino'], query['hoja'], query['col_fechas'], 'A:Z')
        pass