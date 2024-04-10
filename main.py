import mysql.connector as connection
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
from sqlalchemy.sql import text
from urllib.parse import quote_plus

import pickle
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import service_account

load_dotenv()
PORT_DB = os.getenv('PORT_DB')
HOST_DB = os.getenv('HOST_DB')
DB = os.getenv('DB')
USER_DB = os.getenv('USER_DB')
PASS_DB = os.getenv('PASS_DB')
PORT_DB1 = os.getenv('PORT_DB1')
HOST_DB1 = os.getenv('HOST_DB1')
DB1 = os.getenv('DB1')
USER_DB1 = os.getenv('USER_DB1')
PASS_DB1 = os.getenv('PASS_DB1')

#change this by your sheet ID
SAMPLE_SPREADSHEET_ID_input = os.getenv('SPREADSHEET_ID')

#change the range if needed
SAMPLE_RANGE_NAME = 'A1:AA8000'

def selectDBCred(dbidx):
    cred = {}

    if (dbidx == 0):
        cred["PORT"] = PORT_DB
        cred["HOST"] = HOST_DB
        cred["DB"] = DB
        cred["USER"] = USER_DB
        cred["PASS"] = PASS_DB
        
    elif (dbidx == 1):
        cred["PORT"] = PORT_DB1
        cred["HOST"] = HOST_DB1
        cred["DB"] = DB1
        cred["USER"] = USER_DB1
        cred["PASS"] = PASS_DB1

    return cred


def getDFfromDB(query_db, dbidx):

    try:
        cred = selectDBCred(dbidx)

        encoded_pass = quote_plus(cred['PASS'])

        url = f"mysql+mysqlconnector://{cred['USER']}:{encoded_pass}@{cred['HOST']}:{cred['PORT']}/{cred['DB']}"

        engine = sqlalchemy.create_engine(url)

        with engine.connect() as conn:

            query = conn.execute(text(query_db))
            df = pd.DataFrame(query.fetchall())

        return df

    except Exception as e:
        print(str(e))


def getDataFromQuery(query, bdidx):
    
    df = getDFfromDB(query.replace('"', ''), bdidx)

    #print(df['tipo_carrera'])

    return df

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
    
def Export_Data_To_Sheets(query, id_destino, sheet_name, col_fechas, range, bdidx):

    testDf = getDataFromQuery(query, bdidx)
    testDf.fillna('', inplace=True)

    #print(col_fechas)

    for col in col_fechas:
        # No puede haber nombres de columnas vacios
        if col != '':
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
    print('Hoja actualizada correctamente!')


def getQueriesInfo():

    queries = []
    
    result = (service.spreadsheets()
                      .values()
                      .get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input, range="A2:F")
                      .execute())

    rows = result.get("values", [])

    print(f"{len(rows)} rows retrieved")
    for row in rows:
        queries.append({'consulta': row[1], 'destino': row[2], 'hoja': row[3], 
                        'col_fechas': [x.strip() for x in row[4].split(',')],
                        'bd': int(row[5])})

    #print(queries)
    return queries

if __name__ == "__main__":
    queries = getQueriesInfo()
    
    for i, query in enumerate(queries):
        print(f"Procesando QUERY: {i} ...")
        Export_Data_To_Sheets(query['consulta'], query['destino'], query['hoja'], 
                              query['col_fechas'], 'A:Z', query['bd'])
    
    print("Todo OK!!!")