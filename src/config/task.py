import sched
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from config.api import get_global_token
from services.dahua import fetch_access_control_records_page
from services.migrate_db import (
    migrate_db_iclock,
    migrate_db_acc_manager_log,
    migrate_db_data_sj,
)
from database.db import (
    get_time_validate_iclock_data,
    get_time_validate_acc_monitor_log_data,
    get_time_validate_acc_monitor_log_sj_data,
    get_record_limit_iclock,
    get_record_limit_acc_monitor_log_query,
    get_record_limit_acc_monitor_log_sj,
    log_to_db,
)
from utils.mail import send_mail

# Define a generic task descriptor
class Task:
    def __init__(self, name, time_validate_fn, record_limit_fn, migrate_fns, interval, error_context):
        self.name = name
        self.time_validate_fn = time_validate_fn
        self.record_limit_fn = record_limit_fn
        self.migrate_fns = migrate_fns if isinstance(migrate_fns, list) else [migrate_fns]
        self.interval = max(1, interval)  # ensure at least 1 second
        self.error_context = error_context  # tuple(log_type, label)
        self.last_ts = int((datetime.now() - timedelta(hours=int(self.time_validate_fn()[0]))).timestamp())

    def fetch_data(self):
        log_type, label = self.error_context
        try:
            now = datetime.now()
            end_ts = int((now + timedelta(hours=5)).timestamp())
            page_size = str(self.record_limit_fn()[0])

            data = fetch_access_control_records_page(
                page="1",
                pageSize=page_size,
                startTime=str(self.last_ts),
                endTime=str(end_ts),
                token=get_global_token()
            )

            if data and data.get("data") is not None:
                if data["data"].get("pageData") is None:
                    data["data"]["pageData"] = []

            # actualizar last_ts solo si hubo datos nuevos
            page_data = data["data"].get("pageData", [])
            if page_data:
                timestamps = [
                    int(datetime.strptime(record["time"], "%Y-%m-%d %H:%M:%S").timestamp())
                    for record in page_data
                    if "time" in record
                ]
                if timestamps:
                    # overlap de 10 segundos para evitar pérdida
                    self.last_ts = max(timestamps) - 10

            return data

        except Exception as e:
            msg = f"[{self.name}] Error fetching data: {e}"
            print(msg)
            send_mail(msg)
            log_to_db(log_type, 1, "ERROR", msg, f"fetch_{self.name}", 500)
            return None

    def run_forever(self):
        while True:
            print(f"[{self.name}] Running at {datetime.now().isoformat()}")
            data = self.fetch_data()
            if not data or not data.get("data", {}).get("pageData"):
                print(f"[{self.name}] No data to process")
            else:
                with ThreadPoolExecutor(max_workers=len(self.migrate_fns)) as executor:
                    futures = [executor.submit(fn, data) for fn in self.migrate_fns]
                    for f in futures:
                        try:
                            f.result()
                        except Exception as e:
                            log_type, _ = self.error_context
                            msg = f"[{self.name}] Migration error: {e}"
                            print(msg)
                            send_mail(msg)
                            log_to_db(log_type, 1, "ERROR", msg, f"migrate_{self.name}", 500)
            time.sleep(self.interval)

# Instantiate tasks dynamically
tasks = [
    Task(
        name="iclock",
        time_validate_fn=get_time_validate_iclock_data,
        record_limit_fn=get_record_limit_iclock,
        migrate_fns=[migrate_db_iclock],
        interval=1,
        error_context=(2, "ICLOCK"),
    ),
    Task(
        name="acc_monitor",
        time_validate_fn=get_time_validate_acc_monitor_log_data,
        record_limit_fn=get_record_limit_acc_monitor_log_query,
        migrate_fns=[migrate_db_acc_manager_log],
        interval=1,
        error_context=(1, "ACC_MONITOR_LOG"),
    ),
    Task(
        name="acc_monitor_sj",
        time_validate_fn=get_time_validate_acc_monitor_log_sj_data,
        record_limit_fn=get_record_limit_acc_monitor_log_sj,
        migrate_fns=[migrate_db_data_sj],
        interval=1,
        error_context=(1, "ACC_MONITOR_LOG_SJ"),
    ),
]

def get_data_for_a_week():
    try:
        today = datetime.now()
        startTime = int((today - timedelta(days=7)).timestamp())
        # print(f'Tiempo de inicio: {startTime}')
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
        message = f'Error al obtener las asistencias: {e}'
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'get_data_for_a_week', 404)
        return None

def start_scheduler():
    for task in tasks:
        threading.Thread(target=task.run_forever, daemon=True).start()