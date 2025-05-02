import requests
import traceback
from datetime import datetime, timedelta
import pytz
from config.api import get_global_token
from database.db import get_host_dss_query, get_port_dss_query, get_enpoint_access_record_dss_query

def fetch_access_control_records_page(
    page: str,               # (string) Número de página (requerido)
    pageSize: str,           # (string) Cantidad de registros por página (requerido)
    startTime: str,          # (string) Tiempo de inicio en segundos (requerido)
    endTime: str,            # (string) Tiempo de fin en segundos (requerido)
    token
) -> dict:
    """
    Función para obtener registros de control de acceso por página.
    
    Parámetros:
      - page (str): Número de página (requerido).
      - pageSize (str): Cantidad de registros por página (requerido).
      - startTime (str): Tiempo de inicio en segundos (requerido).
      - endTime (str): Tiempo de fin en segundos (requerido).
      - token (str, opcional): Token de autenticación.
    
    Retorna:
      - Un diccionario con la respuesta del servidor o un mensaje de error.
    """
    
    # Si no se pasa un token, se obtiene uno (asegúrate de tener implementada la función get_token)
    if not token:
        token = get_global_token()
    
    # URL del endpoint
    url = f"https://{get_host_dss_query()[0]}:{get_port_dss_query()[0]}{get_enpoint_access_record_dss_query()[0]}"
    
    # Definir los encabezados requeridos
    headers = {
        "X-Subject-Token": token,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept-Language": "en"
    }
    
    # Construir el payload según la documentación
    payload = {
        "page": page,
        "pageSize": pageSize,
        "startTime": startTime,
        "endTime": endTime
    }
    
    try:
        # print("URL:", url)
        # print("Encabezados:", headers)
        # print("Payload:", payload)
        response = requests.post(url, json=payload, headers=headers, verify=False)
        # print("Status Code:", response.status_code)
        
        if response.status_code == 200:
            try:
                data = response.json()

                # Normalizar pageData
                if data.get("data") is None:
                    data["data"] = {}
                if data["data"].get("pageData") is None:
                    data["data"]["pageData"] = []

                # Ahora pageData siempre es iterable
                for record in data["data"]["pageData"]:
                    alarm_time_seconds = record.get("alarmTime")
                    if alarm_time_seconds:
                        utc_time = datetime.utcfromtimestamp(int(alarm_time_seconds)).replace(tzinfo=pytz.utc)
                        ecuador_time = utc_time - timedelta(hours=5)
                        record["alarmTime"] = ecuador_time.strftime("%Y-%m-%d %H:%M:%S")

                return data

            except Exception as e:
                print("Error al procesar los datos:", e)
                traceback.print_exc()
                return {"error": "No se pudo procesar la respuesta"}
        else:
            print("Error en la solicitud:", response.status_code, response.text)
            return {"error": f"Error {response.status_code}: {response.text}"}
    
    except Exception as e:
        print("Excepción durante la solicitud:", e)
        traceback.print_exc()
        return {"error": str(e)}

# /obms/api/v1.1/alarm-host/alarm-out/channel/list?deviceCode={deviceCode} 
# /obms/api/v1.1/alarm-host/alarm-signal/channel/list?deviceCode={deviceCode}
# /brms/api/v1.1/alarm/record/entrance-block-roster/detail?alarmCode={alarmCode}
# /obms/api/v1.1/acs/person/summary/page?page={page}&pageSize={pageSize}&orgCode={orgCode}&keyword={keyword}&faceComparisonGroupId={faceComparisonGroupId}&faceIssueResult={faceIssueResult}&deviceCode={deviceCode}&channelId={channelId}&accessTypes={accessTypes}&inMultiCardGroups={inMultiCardGroups}&inMultiCardGroupId={inMultiCardGroupId}&containChild={containChild}
