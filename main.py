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
    
    df = getDFfromDB(query)

    print(df)

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

QUERY_FACT = """SELECT temp1.*,
  inscriptos.inscriptos as prematriculados, aceptados.matriculados_aceptados as inscriptos, ap_mi.aprobados_mi, temp.inician_cursada_seminario,
  temp.cursantes_t3,
  temp.cursando_hoy,
  IF(egr.egresados_intermedio IS NULL,0,egr.egresados_intermedio) as egresados_intermedio,
  IF(egr.egresados is NULL,0,egr.egresados) as egresados,
  COALESCE(anomalas.inician_cursada_stf,ap_mi.aprobados_mi,ap_mi2.aprobados_mi2,temp.inician_cursada_seminario) as matricula_inicial,
  IF(tiene_egresados.egresados is NOT NULL,1,0) as tiene_egresados,
  IF(tiene_egresados_inter.egresados_inter is NOT NULL,1,0) as tiene_egresados_inter,
  IF(aceptados.matriculados_aceptados > 0 OR temp.cursantes_t3 > 0 OR inscriptos.inscriptos > 0,'Si','No') as vigencia_prop,
  activos.activos_RAI, 
  IF(apsem.aprobados_sem IS NULL, 0, apsem.aprobados_sem) as aprobados_sem
  FROM
      (SELECT DISTINCT ca.idcarrera, co.idcohorte, 
       		  ca.idcarreralineaformativa_itemcatalogo as id_linea_formativa,
       		  ca.idgrupocarrerapropuesta_itemcatalogo as id_propuesta, 
       		  ca.idciclo_itemcatalogo as id_ciclo_sem, 
       		  co.anio as cohorte_anio, 
       		  anios.anio, anios.idsede,
              case
              when ca.siglas_identificatorias like "TRAYECTO_PED%" then 5
              when ca.tipo = "Seminario" then 1
              when ca.tipo = "Trayecto" then 2
              when ca.tipo = "Profesorado" then 3
              else 4 end as id_tipo
         FROM cohortes as co
         LEFT JOIN carreras as ca on ca.idcarrera=co.idcarrera
          LEFT JOIN (
             SELECT DISTINCT cm.idcarrera, cm.idcohorte, YEAR(em.fechainicio) as anio, se.idsede
             FROM carreras_matriculacion as cm
             LEFT JOIN carreras_cursadas as cc on cc.idmatriculacion=cm.idmatriculacion
             LEFT JOIN edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
             LEFT JOIN sedes as se on se.idsede=cm.idsede
              WHERE em.fechainicio IS NOT NULL
             union (
                SELECT DISTINCT cm.idcarrera, cm.idcohorte, YEAR(cmt.fechafinalizacion) as anio, se.idsede
                 FROM carreras_matriculacion as cm
                 LEFT JOIN carrerasmatriculacion_titulos as cmt on cmt.idmatriculacion=cm.idmatriculacion
                 LEFT JOIN sedes as se on se.idsede=cm.idsede
                 WHERE cmt.fechafinalizacion IS NOT NULL
                )
               union (
                SELECT DISTINCT cm.idcarrera, cm.idcohorte, co.anio, se.idsede
                 FROM carreras_matriculacion as cm
                 LEFT JOIN cohortes as co on co.idcohorte=cm.idcohorte
                 LEFT JOIN sedes as se on se.idsede=cm.idsede
                 WHERE co.anio IS NOT NULL
                )
         ) as anios on anios.idcarrera=co.idcarrera and anios.idcohorte=co.idcohorte
         where anios.anio is not null) as temp1
   LEFT JOIN(
   SELECT
      ca.idcarrera, co.idcohorte,
        YEAR(em.fechainicio) as anio, se.idsede, se.nombre as ifda, dep.nombre as departamento,
      COUNT(DISTINCT IF(YEAR(em.fechainicio)=co.anio and 
                           ca.tipo='Seminario' and 
                           cc.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional'), cm.idmatriculacion, NULL)) as inician_cursada_seminario,
      COUNT(DISTINCT IF(m.siglas_identificatorias!='MI' and 
                           cc.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional') and MI.status_mi=1, cm.idmatriculacion, NULL)) as cursantes_t3,
      COUNT(DISTINCT IF(cc.estado in ('Cursando','Cursando condicional') and CURDATE()>=em.fechainicio, cm.idmatriculacion, NULL)) as cursando_hoy,
      tiene_mi.idcarrera as tiene_mi,
      se.sigla as ifda_siglas
  FROM
     carreras_matriculacion as cm
     LEFT JOIN carreras_cursadas as cc on cc.idmatriculacion=cm.idmatriculacion
     LEFT JOIN carreras as ca on ca.idcarrera=cm.idcarrera
     LEFT JOIN cohortes as co on co.idcohorte=cm.idcohorte
     LEFT JOIN edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
     LEFT JOIN materias as m on m.idmateria=em.idmateria
     LEFT JOIN aprobados_mi as apmi on apmi.idmatriculacion=cm.idmatriculacion
     LEFT JOIN sedes as se on se.idsede=cm.idsede
     LEFT JOIN localidades as loc on loc.idlocalidad=se.idlocalidad
     LEFT JOIN departamentos as dep on dep.iddepartamento=loc.iddepartamento
     LEFT JOIN v_cm_inactivos_tipo_resumen as inact on inact.idmatriculacion=cm.idmatriculacion
     
     LEFT JOIN (
         SELECT cc.idmatriculacion, 1 as status_mi
         FROM carreras_cursadas as cc
         LEFT JOIN carreras_matriculacion as cm on cm.idmatriculacion=cc.idmatriculacion
         LEFT JOIN edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
         INNER JOIN planestudios as pe on pe.idcarrera=cm.idcarrera
         WHERE cc.estado in ('Aprobado','Equivalencia') and cm.idcarrera not in (97,79) and (pe.introductorio='Si' OR pe.orden=1)
         GROUP BY cc.idmatriculacion
              UNION
         SELECT cm.idmatriculacion, 1 as status_mi
         FROM carreras_cursadas as cc
         LEFT JOIN carreras_matriculacion as cm on cm.idmatriculacion=cc.idmatriculacion
         LEFT JOIN carreras as ca on ca.idcarrera=cm.idcarrera
         WHERE ca.tipo='Seminario' and cc.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional','Equivalencia')
         GROUP BY cc.idmatriculacion
              UNION
         SELECT cm.idmatriculacion, 1 as status_mi
         FROM carreras_cursadas as cc
         LEFT JOIN carreras_matriculacion as cm on cm.idmatriculacion=cc.idmatriculacion
         LEFT JOIN edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
         LEFT JOIN materias as m on m.idmateria=em.idmateria
         WHERE cm.idcarrera in (97,79) and m.siglas_identificatorias in ('SEMINARIO_TF','SEMINARIOTF_PYC') AND
                cc.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional','Equivalencia')
        GROUP BY cc.idmatriculacion) as MI on MI.idmatriculacion=cm.idmatriculacion

     
    LEFT JOIN(SELECT ca.idcarrera
          FROM carreras as ca
          LEFT JOIN planestudios as pl on pl.idcarrera=ca.idcarrera
          LEFT JOIN materias as m on m.idmateria=pl.idmateria
          WHERE m.siglas_identificatorias='MI'
          GROUP BY ca.idcarrera) as tiene_mi on tiene_mi.idcarrera=ca.idcarrera
  WHERE cc.estado is not NULL AND (cm.estado IN ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional','Matriculado','Matriculado condicional') OR inact.tipo_inactivo='NO A' )
     GROUP BY
       ca.idcarrera, co.idcohorte, se.idsede, YEAR(em.fechainicio)) as temp on temp.idcarrera=temp1.idcarrera and temp.idcohorte=temp1.idcohorte and temp.anio=temp1.anio and temp.idsede=temp1.idsede
  LEFT JOIN (
         SELECT ca.idcarrera, co.idcohorte, YEAR(cmt.fechafinalizacion) as anio, se.idsede, 
      			COUNT(DISTINCT IF(ct.es_intermedio='Si',cmt.idmatriculacion,NULL)) as egresados_intermedio,
                COUNT(DISTINCT IF(ct.es_intermedio='No',cmt.idmatriculacion,NULL)) as egresados
         FROM carreras_matriculacion as cm
         LEFT JOIN carreras as ca on ca.idcarrera=cm.idcarrera
         LEFT JOIN cohortes as co on co.idcohorte=cm.idcohorte
         LEFT JOIN carrerasmatriculacion_titulos as cmt on cmt.idmatriculacion=cm.idmatriculacion
         LEFT JOIN carreras_titulos as ct on ct.idtitulo=cmt.idtitulo
         LEFT JOIN sedes as se on se.idsede=cm.idsede
         where cmt.fechafinalizacion is not NULL and ca.tipo<>"Seminario"
         GROUP BY ca.idcarrera, co.idcohorte, se.idsede, YEAR(cmt.fechafinalizacion)
         ) as egr on egr.idcarrera=temp1.idcarrera and egr.idcohorte=temp1.idcohorte and egr.anio=temp1.anio and egr.idsede=temp1.idsede
  LEFT JOIN (
       select cm.idcarrera, co.anio, co.idcohorte, cm.idsede, count(distinct cm.idmatriculacion) as inscriptos
       from carreras_matriculacion as cm
       left join cohortes as co on co.idcohorte=cm.idcohorte
       group by cm.idcarrera, cm.idcohorte, cm.idsede
       ) as inscriptos on inscriptos.idcarrera=temp1.idcarrera and inscriptos.idcohorte=temp1.idcohorte and inscriptos.idsede=temp1.idsede and inscriptos.anio=temp1.anio
  LEFT JOIN (
       select cm.idcarrera, co.anio, co.idcohorte, cm.idsede, count(distinct cm.idmatriculacion) as matriculados_aceptados
       from carreras_matriculacion as cm
       left join cohortes as co on co.idcohorte=cm.idcohorte
       LEFT JOIN v_cm_inactivos_tipo_resumen as inact on inact.idmatriculacion=cm.idmatriculacion
       where cm.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional','Matriculado','Matriculado condicional') OR inact.tipo_inactivo='NO A' 
       group by cm.idcarrera, cm.idcohorte, cm.idsede
       ) as aceptados on aceptados.idcarrera=temp1.idcarrera and aceptados.idcohorte=temp1.idcohorte and aceptados.idsede=temp1.idsede and aceptados.anio=temp1.anio
  LEFT JOIN (
       select cm.idcarrera, YEAR(em.fechainicio) as anio, co.idcohorte, cm.idsede, count(distinct cm.idmatriculacion) as aprobados_mi
       from carreras_matriculacion as cm
       left join cohortes as co on co.idcohorte=cm.idcohorte
       left join carreras_cursadas as cc on cc.idmatriculacion=cm.idmatriculacion
       left join planestudios as pe on pe.idmateria=cc.idmateria
       left join edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
       LEFT JOIN materias as m on m.idmateria=em.idmateria
       LEFT JOIN v_cm_inactivos_tipo_resumen as inac on inac.idmatriculacion=cm.idmatriculacion
       where (cm.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Matriculado','Cursando condicional','Matriculado condicional') OR inac.tipo_inactivo='NO A' ) 
          and (pe.introductorio='Si' OR m.siglas_identificatorias='MI') and cc.estado in ('Aprobado','Equivalencia')
       group by cm.idcarrera, cm.idcohorte, cm.idsede, YEAR(em.fechainicio)
       ) as ap_mi on ap_mi.idcarrera=temp1.idcarrera and ap_mi.idcohorte=temp1.idcohorte and ap_mi.idsede=temp1.idsede and ap_mi.anio=temp1.anio
  LEFT JOIN (
       select cm.idcarrera, YEAR(em.fechainicio) as anio, co.idcohorte, cm.idsede, count(distinct cm.idmatriculacion) as aprobados_mi2
       from carreras_matriculacion as cm
       left join cohortes as co on co.idcohorte=cm.idcohorte
       left join carreras as ca on ca.idcarrera=cm.idcarrera
       left join carreras_cursadas as cc on cc.idmatriculacion=cm.idmatriculacion
       left join edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
       LEFT JOIN planestudios as pe on pe.idmateria=em.idmateria and pe.idcarrera=ca.idcarrera
       LEFT JOIN v_cm_inactivos_tipo_resumen as inac on inac.idmatriculacion=cm.idmatriculacion
       where ca.modulounico='No' and pe.orden=1 and pe.anio=1
       and (cm.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional','Matriculado','Matriculado condicional') OR inac.tipo_inactivo='NO A' )
       and cc.estado in ('Aprobado','Equivalencia')
       group by cm.idcarrera, cm.idcohorte, cm.idsede, YEAR(em.fechainicio)
       ) as ap_mi2 on ap_mi2.idcarrera=temp1.idcarrera and ap_mi2.idcohorte=temp1.idcohorte and ap_mi2.idsede=temp1.idsede and ap_mi2.anio=temp1.anio
  LEFT JOIN (
         SELECT ca.idcarrera, co.idcohorte, COUNT(DISTINCT cmt.idmatriculacion) as egresados
         FROM carreras_matriculacion as cm
         LEFT JOIN carreras as ca on ca.idcarrera=cm.idcarrera
         LEFT JOIN cohortes as co on co.idcohorte=cm.idcohorte
         LEFT JOIN carrerasmatriculacion_titulos as cmt on cmt.idmatriculacion=cm.idmatriculacion
         LEFT JOIN carreras_titulos as ct on ct.idtitulo=cmt.idtitulo
         WHERE cmt.idmatriculacion is NOT NULL and ct.es_intermedio='No'
         GROUP BY ca.idcarrera, co.idcohorte) as tiene_egresados on tiene_egresados.idcarrera=temp1.idcarrera and tiene_egresados.idcohorte=temp1.idcohorte
  LEFT JOIN (
         SELECT ca.idcarrera, co.idcohorte, COUNT(DISTINCT cmt.idmatriculacion) as egresados_inter
         FROM carreras_matriculacion as cm
         LEFT JOIN carreras as ca on ca.idcarrera=cm.idcarrera
         LEFT JOIN cohortes as co on co.idcohorte=cm.idcohorte
         LEFT JOIN carrerasmatriculacion_titulos as cmt on cmt.idmatriculacion=cm.idmatriculacion
         LEFT JOIN carreras_titulos as ct on ct.idtitulo=cmt.idtitulo
         WHERE cmt.idmatriculacion is NOT NULL and ct.es_intermedio='Si'
         GROUP BY ca.idcarrera, co.idcohorte) as tiene_egresados_inter on tiene_egresados_inter.idcarrera=temp1.idcarrera and tiene_egresados_inter.idcohorte=temp1.idcohorte
  LEFT JOIN (
         SELECT cm.idcarrera, cm.idcohorte, cm.idsede, YEAR(em.fechainicio) as anio, COUNT(DISTINCT cm.idmatriculacion) as inician_cursada_stf
          FROM carreras_matriculacion as cm
          LEFT JOIN carreras_cursadas as cc on cc.idmatriculacion=cm.idmatriculacion
          LEFT JOIN edicionesmodulos as em on em.idedicionmodulo=cc.idedicionmodulo
          LEFT JOIN materias as m on m.idmateria=em.idmateria
          LEFT JOIN v_cm_inactivos_tipo_resumen as inac on inac.idmatriculacion=cm.idmatriculacion
          WHERE cm.idcarrera in (97,79,180) AND (cm.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional') OR inac.tipo_inactivo='NO A' )
            AND m.siglas_identificatorias in ('SEMINARIO_TF','SEMINARIOTF_PYC','SEMINARIOTF_ENSEMATE') AND cc.estado in ('Aprobado','Reprobado','Abandonó','Cursando','Cursando condicional','Equivalencia')
          GROUP BY cm.idcarrera, cm.idcohorte, cm.idsede,  YEAR(em.fechainicio)
  ) as anomalas on anomalas.idcarrera=temp1.idcarrera and anomalas.idcohorte=temp1.idcohorte and anomalas.idsede=temp1.idsede and anomalas.anio=temp1.anio
  LEFT JOIN (
         SELECT cm.idcarrera, cm.idcohorte, cm.idsede, YEAR(CURDATE()) as anio, COUNT(DISTINCT cm.idmatriculacion) as activos_RAI
          FROM carreras_matriculacion as cm
          WHERE cm.estado in ('Cursando','Cursando condicional')
          GROUP BY cm.idcarrera, cm.idcohorte, cm.idsede
  ) as activos on activos.idcarrera=temp1.idcarrera and activos.idcohorte=temp1.idcohorte and activos.idsede=temp1.idsede and activos.anio=temp1.anio
 LEFT JOIN (
        SELECT cm.idcarrera, cm.idcohorte, co.anio, cm.idsede, COUNT(DISTINCT cm.idmatriculacion) as aprobados_sem
        FROM carreras_matriculacion as cm
        LEFT JOIN carreras as ca on ca.idcarrera=cm.idcarrera
        LEFT JOIN cohortes as co on co.idcohorte=cm.idcohorte
        WHERE ca.tipo='Seminario' and cm.estado='Aprobado'
        GROUP BY cm.idcarrera, cm.idcohorte, cm.idsede
  ) as apsem on apsem.idcarrera=temp1.idcarrera and apsem.idcohorte=temp1.idcohorte and apsem.idsede=temp1.idsede and apsem.anio=temp1.anio
  WHERE temp1.idcarrera IS NOT NULL and temp1.idcarrera not in (34,105,132,168) and temp1.idcohorte not in (113,127,121);"""

QUERY_DIM_CARRERA = """
    SELECT ca.idcarrera, ca.siglas_identificatorias as carrera_siglas, ca.nombre as carrera
    FROM carreras as ca;"""

QUERY_DIM_LF = """
    SELECT ci.iditem as id_linea_formativa, ci.item linea_formativa, ci.sigla_identificatoria linea_formativa_sigla
    FROM catalogos_items as ci
    WHERE ci.idcatalogo=2;"""

QUERY_DIM_TIPO = """
    SELECT 
		DISTINCT
        case
			  when ca.siglas_identificatorias like "TRAYECTO_PED%" then 5
              when ca.tipo = "Seminario" then 1
              when ca.tipo = "Trayecto" then 2
              when ca.tipo = "Profesorado" then 3
       		  when ca.siglas_identificatorias like "TRAYECTO_PED%" then 5
              else 4 end as id_tipo,
        case
        	  when ca.siglas_identificatorias like "TRAYECTO_PED%" then "Trayecto pedagógico"
              when ca.tipo = "Seminario" then "Curso"
              when ca.tipo = "Trayecto" then "Postítulo"
              when ca.tipo = "Profesorado" then "Profesorado"
              else "Formación académica" end as tipo
    FROM carreras as ca;"""

QUERY_DIM_IFDA = """
    SELECT se.idsede, se.sigla as ifda_siglas, se.nombre as ifda
    FROM sedes as se;"""

QUERY_DIM_COHORTE = """
    SELECT co.idcohorte, co.nombre as cohorte, co.anio as cohorte_anio, co.fechainiciocursada, co.fechainicioinscripcion, 
        co.fechafininscripcion, co.fechafinteorica
    FROM cohortes as co;"""

QUERY_DIM_CICLO = """
    SELECT ci.iditem as id_ciclo_sem, ci.item as ciclo_sem
    FROM catalogos_items as ci
    WHERE ci.idcatalogo=3;"""
    
def Export_Data_To_Sheets(query, sheet_name, range):

    testDf = getDataFromQuery(query)
    testDf.fillna('', inplace=True)

    if(sheet_name == "dim_cohorte"):
        # Debemos cambiar el formato de las fechas
        testDf["fechainiciocursada"] = pd.to_datetime(testDf["fechainiciocursada"], format='%Y-%m-%d')
        testDf["fechainiciocursada"] = testDf["fechainiciocursada"].dt.strftime('%Y/%m/%d')

        testDf["fechainicioinscripcion"] = pd.to_datetime(testDf["fechainicioinscripcion"], format='%Y-%m-%d')
        testDf["fechainicioinscripcion"] = testDf["fechainicioinscripcion"].dt.strftime('%Y/%m/%d')

        testDf["fechafininscripcion"] = pd.to_datetime(testDf["fechafininscripcion"], format='%Y-%m-%d')
        testDf["fechafininscripcion"] = testDf["fechafininscripcion"].dt.strftime('%Y/%m/%d')

        testDf["fechafinteorica"] = pd.to_datetime(testDf["fechafinteorica"], format='%Y-%m-%d')
        testDf["fechafinteorica"] = testDf["fechafinteorica"].dt.strftime('%Y/%m/%d')

        testDf.fillna('', inplace=True)

    # Limpiamos hoja
    body = {}
    resultClear = service.spreadsheets( ).values( ).clear( spreadsheetId=SAMPLE_SPREADSHEET_ID_input, 
                                                          range=sheet_name,
                                                          body=body ).execute( )

    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=sheet_name + "!" + range,
        body=dict(
            majorDimension='ROWS',
            values=testDf.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

Export_Data_To_Sheets(QUERY_FACT, "fact_metricas", "A:Z")
Export_Data_To_Sheets(QUERY_DIM_CARRERA, "dim_carrera", "A:Z")
Export_Data_To_Sheets(QUERY_DIM_TIPO, "dim_tipo", "A:Z")
Export_Data_To_Sheets(QUERY_DIM_LF, "dim_linea_formativa", "A:Z")
Export_Data_To_Sheets(QUERY_DIM_IFDA, "dim_ifda", "A:Z")
Export_Data_To_Sheets(QUERY_DIM_COHORTE, "dim_cohorte", "A:Z")
Export_Data_To_Sheets(QUERY_DIM_CICLO, "dim_ciclo", "A:Z")