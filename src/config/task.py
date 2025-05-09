import sched
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import HTTPException

from services.dahua import fetch_access_control_records_page, get_global_token
from services.migrate_db import migrate_db_iclock, migrate_db_acc_manager_log, migrate_db_data_sj
from database.db import get_time_validate_iclock_data, get_time_validate_acc_monitor_log_data, log_to_db, get_record_limit_iclock, get_record_limit_acc_monitor_log_query
from utils.mail import send_mail

# Crear un objeto scheduler
scheduler = sched.scheduler(time.time, time.sleep)

# Intervalos independientes (en segundos)
ICLOCK_INTERVAL = 0
ACC_MONITOR_INTERVAL = 0

def run_iclock():
    """
    Ejecuta iclock() y se reprograma automáticamente.
    """
    try:
        iclock()
    except Exception as e:
        print(f"[run_iclock] error: {e}")
    finally:
        scheduler.enter(ICLOCK_INTERVAL, 1, run_iclock)

def run_acc_monitor_log():
    """
    Ejecuta acc_monitor_log() y se reprograma automáticamente.
    """
    try:
        acc_monitor_log()
    except Exception as e:
        print(f"[run_acc_monitor_log] error: {e}")
    finally:
        scheduler.enter(ACC_MONITOR_INTERVAL, 1, run_acc_monitor_log)

def get_data_from_dss():
    try:
        today = datetime.now()
        minutes = int(get_time_validate_iclock_data()[0])
        pageSize_ = get_record_limit_iclock()[0]
        startTime = int((today + timedelta(minutes=minutes)).timestamp())
        endTime = int((today + timedelta(minutes=10)).timestamp())
        result = fetch_access_control_records_page(
            page='1',
            pageSize=pageSize_,
            startTime=startTime,
            endTime=endTime,
            token=get_global_token()
        )
        return result
    except Exception as e:
        message = f'Error al obtener los datos de DSS: {e}'
        print(message)
        send_mail(message)
        log_to_db(2, 1, 'ERROR', message, 'get_data_from_dss', 404)
        return None

def get_data_iclock():
    try:
        today = datetime.now()
        hours = int(get_time_validate_iclock_data()[0])
        startTime = int((today - timedelta(hours=hours)).timestamp())
        endTime = int((today + timedelta(hours=1)).timestamp())
        record_limit = get_record_limit_iclock()[0]
        resultado = fetch_access_control_records_page(
            page='1',
            pageSize=f'{record_limit}',
            startTime=startTime,
            endTime=endTime,
            token=get_global_token()
        )
        return resultado
    except Exception as e:
        message = f'Error al obtener las asistencias ICLOCK: {e}'
        print(message)
        send_mail(message)
        log_to_db(2, 1, 'ERROR', message, 'get_data_iclock', 404)
        return None

def get_data_acc_monitor_log():
    try:
        today = datetime.now()
        hours = int(get_time_validate_acc_monitor_log_data()[0])
        startTime = int((today - timedelta(hours=hours)).timestamp())
        endTime = int((today + timedelta(hours=1)).timestamp())
        record_limit = get_record_limit_acc_monitor_log_query()[0]
        resultado = fetch_access_control_records_page(
            page='1',
            pageSize=f'{record_limit}',
            startTime=startTime,
            endTime=endTime,
            token=get_global_token()
        )
        return resultado
    except Exception as e:
        message = f'Error al obtener las asistencias ACC_MONITOR_LOG: {e}'
        print(message)
        send_mail(message)
        log_to_db(1, 1, 'ERROR', message, 'get_data_acc_monitor_log', 404)
        return None

def get_data_for_a_week():
    try:
        today = datetime.now()
        startTime = int((today - timedelta(days=7)).timestamp())
        endTime = int((today + timedelta(hours=1)).timestamp())
        resultado = fetch_access_control_records_page(
            page='1',
            pageSize='7000',
            startTime=startTime,
            endTime=endTime,
            token=get_global_token()
        )
        return resultado
    except Exception as e:
        message = f'Error al obtener las asistencias semanal: {e}'
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'get_data_for_a_week', 404)
        return None

def get_record_7_days():
    data = get_data_for_a_week()
    if data:
        tamanho = len(data["data"]["pageData"])
        return tamanho, data
    return None

def iclock():
    """
    Obtiene datos ICLOCK y migra en paralelo hacia SJ y tabla principal.
    """
    data = get_data_iclock()
    if not data:
        print("No hay datos de iclock")
        return

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(migrate_db_iclock, data),
            executor.submit(migrate_db_data_sj, data),
        ]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                message = f"Error en migración iclock: {e}"
                print(message)
                log_to_db(2, 1, 'ERROR', message, 'iclock', 500)
                send_mail(message)

def acc_monitor_log():
    """
    Obtiene y migra los logs de control de acceso.
    """
    data = get_data_acc_monitor_log()
    if not data:
        print("No hay datos de acc_monitor_log")
        return

    try:
        migrate_db_acc_manager_log(data)
    except Exception as e:
        message = f"Error en migración acc_monitor_log: {e}"
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'acc_monitor_log', 500)
        send_mail(message)

def start_scheduler():
    """
    Inicia el scheduler programando ambas tareas en t=0.
    """
    scheduler.enter(0, 1, run_iclock)
    scheduler.enter(0, 1, run_acc_monitor_log)
    threading.Thread(target=scheduler.run, daemon=True).start()