import uvicorn
import urllib3
from fastapi import FastAPI
from config.api import token_manager
from config.task import start_scheduler, get_data_for_a_week

app = FastAPI(
    title='API Dahua para obtener los registros de asistencias',
    description='Conexion con el servidor de Dahua',
    version='1.0.0'
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@app.get('/', description='Endpoint default')
def default_endpoint():
    try:
        info = [
            { "message" : "Dahua Record Assistance API en ejecución ..." },
            { "status" : "ok"}
        ]
        return info
    except Exception as e:
        info = [
            {"message": {str(e)}},
            { "status" : "error"}
        ]
        return info
    
@app.post('/obtener-registros-7-dias', description='Endpoint que obtiene las asistencias de todo el personal en un rango de 7 días')
def get_records_7_days():
    try:
        resultado = get_data_for_a_week()
        return resultado
    except Exception as e:
        return { "error" : str(e) }

@app.on_event("startup")
async def startup_event():
    # Iniciar el scheduler cuando se levanta la API
    start_scheduler()

if __name__ == "__main__":
    token_manager.start()
    uvicorn.run("app:app", host="127.0.0.1", port=9994) #, log_level="debug"