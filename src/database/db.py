import os
import pyodbc
# from utils.mail import sendmail
from dotenv import load_dotenv
from datetime import datetime, timedelta
from fastapi import HTTPException

load_dotenv()

# Diccionario de configuraciones para múltiples bases de datos
DB_CONFIG = {
    1: {"name": f"{os.getenv('DB_NAME_1')}"},
    2: {"name": f"{os.getenv('DB_NAME_2')}"},
}

def create_db_connection(db_id: int):
    """Crea una conexión nueva a la base de datos especificada."""
    if db_id not in DB_CONFIG:
        raise ValueError(f"ID de base de datos no reconocido: {db_id}")
    
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={DB_CONFIG[db_id]['name']};"
            f"UID={os.getenv('DB_USERNAME')};"
            f"PWD={os.getenv('DB_PASSWORD')};",
            timeout=5
        )
        # print(f"[INFO] Conexión establecida con {DB_CONFIG[db_id]['name']}")
        return conn
    except Exception as e:
        print(f"[ERROR] Error al conectar con la base de datos {DB_CONFIG[db_id]['name']}: {e}")
        return None


def get_db_connection(db_id: int):
    """Conexión reutilizable en endpoints de FastAPI (no persistente entre peticiones)."""
    conn = create_db_connection(db_id)
    if conn is None:
        raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail="Error validando conexión: " + str(e))
    
    return conn

# Función para ejecutar SELECT   
def execute_select_query(db_id, query):
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows[0] if rows else None
    except Exception as e:
        message = f'Error al ejecutar SELECT en {DB_CONFIG[db_id]["name"]}: {e}'
        print(message)
        # log_to_db(db_id, 1, 'ERROR', message, 'execute_select_query', 400)
        return None
    
def execute_select_tuple_query(db_id, query, tuple: tuple):
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, tuple)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    except Exception as e:
        message = f'Error al ejecutar SELECT con tupla en {DB_CONFIG[db_id]["name"]}: {e}'
        print(message)
        # log_to_db(db_id, 1, 'ERROR', message, 'execute_select_tuple_query', 400)
        return None
    
def execute_select_multiples_rows_query(db_id, query):
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            print(f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}')
            return []
        
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        # Si rows no es una lista (por ejemplo, un único registro), lo envolvemos en una lista.
        if rows and not isinstance(rows, list):
            rows = [rows]
        return rows
    except Exception as e:
        message = f'Error al ejecutar SELECT en {DB_CONFIG[db_id]["name"]}: {e}'
        print(message)
        # log_to_db(db_id, 1, 'ERROR', message, 'execute_select_multiples_rows_query', 400)
        return []

# 🔹 Función para ejecutar INSERT
def execute_insert_query(db_id, query, params):
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return {'message': 'INSERT ejecutado correctamente'}
    except Exception as e:
        message = f'Error al ejecutar INSERT en {DB_CONFIG[db_id]["name"]}: {e}'
        print(message)
        # log_to_db(db_id, 1, 'ERROR', message, 'execute_insert_query', 409)
        return None
    finally:
        if conn:
            conn.close()

# Función para ejecutar UPDATE
def execute_update_query(db_id, query, params: tuple):
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        return {'message': 'UPDATE ejecutado correctamente'}
    except Exception as e:
        message = f'Error al ejecutar UPDATE en {DB_CONFIG[db_id]["name"]}: {e}'
        print(message)
        # log_to_db(db_id, 1, 'ERROR', message, 'execute_update_query', 400)
        return None

# 🔹 Función para registrar logs en la base de datos
def log_to_db(db_id, id_group, log_level, message, endpoint, status_code):
    log_time = datetime.now() - timedelta(hours=6)
    query = """INSERT INTO Logs_Info (id_group, log_time, log_level, message, endpoint, status_code) VALUES (?, ?, ?, ?, ?, ?)"""
    return execute_insert_query(db_id, query, (id_group, log_time, log_level, message, endpoint, status_code))

# 🔹 Función para validar logs en acc_monitor_log
def validate_log_acc_monitor_log(db_id, time, pin):
    query = """SELECT COUNT(*) FROM acc_monitor_log WHERE time = ? AND pin = ?"""
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, (time, pin))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Exception as e:
        print(f'Error al validar log en {DB_CONFIG[db_id]["name"]}: {e}')
        return None
    
# 🔹 Función para validar logs en iclock transaction
def validate_log_iclock(db_id, time, pin):
    query = """SELECT COUNT(*) FROM iclock_transaction WHERE punch_time = ? AND emp_code = ?""" # '2025-02-25T07:02:27.000'
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, (time, pin))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Exception as e:
        print(f'Error al validar iclock en {DB_CONFIG[db_id]["name"]}: {e}')
        return None
    
def validate_log_iclock_sj(db_id, time, pin):
    query = """SELECT COUNT(*) FROM iclock_transaction_sj WHERE punch_time = ? AND emp_code = ?"""
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, (time, pin))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Exception as e:
        print(f'Error al validar iclock en {DB_CONFIG[db_id]["name"]}: {e}')
        return None
    
def validate_log_acc_monitor_log_sj(db_id, time, pin):
    query = """SELECT COUNT(*) FROM acc_monitor_log_sj WHERE time = ? AND pin = ?"""
    try:
        conn = get_db_connection(db_id)
        if conn is None:
            return {'error': f'Error al conectar con la base de datos {DB_CONFIG[db_id]["name"]}'}
        
        cursor = conn.cursor()
        cursor.execute(query, (time, pin))
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
    except Exception as e:
        print(f'Error al validar log en {DB_CONFIG[db_id]["name"]}: {e}')
        return None

# Querys para obtener las credenciales de la base de datos DSS
host_dss_query = """SELECT prm_valor
                    FROM Parametros_Sistema
                        WHERE id_grupo = 1 AND prm_descripcion = 'host'"""
                        
port_dss_query = """SELECT prm_valor
                    FROM Parametros_Sistema
                    WHERE id_grupo = 1 AND prm_descripcion = 'port'"""
                    
user_dss_query = """SELECT prm_valor
                    FROM Parametros_Sistema
                    WHERE id_grupo = 1 AND prm_descripcion = 'user'"""
                    
password_dss_query = """SELECT prm_valor
                    FROM Parametros_Sistema
                    WHERE id_grupo = 1 AND prm_descripcion = 'password'"""
                    
enpoint_access_record_dss_query = """SELECT prm_valor
                                    FROM Parametros_Sistema
                                    WHERE id_grupo = 6 AND prm_descripcion = 'enpoint_access_record'"""

temp_dss_query = """SELECT prm_valor
                    FROM Parametros_Sistema
                    WHERE id_grupo = 1 AND prm_descripcion = 'temp'"""

# Querys para insertar y validar informacion en la base de datos
validate_log_query = """SELECT COUNT(*)
                            FROM acc_monitor_log
                            WHERE time =? AND pin =?"""                        

insert_iclock_query = """INSERT INTO [dbo].[iclock_transaction]
    (
        [emp_code], 
        [punch_time], 
        [punch_state], 
        [verify_type], 
        [work_code], 
        [terminal_sn],
        [terminal_alias],
        [area_alias],
        [source],
        [purpose],
        [crc],
        [upload_time],
        [emp_id],
        [terminal_id],
        [is_mask],
        [temperature],
        [FechaBio]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_acc_monitor_log_query = """INSERT INTO [dbo].[acc_monitor_log] 
(
    [status], 
    [time], 
    [pin], 
    [device_id], 
    [device_name], 
    [verified], 
    [state], 
    [event_type], 
    [event_point_type], 
    [event_point_id], 
    [event_point_name]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

insert_acc_monitor_log_sj_query = """INSERT INTO [dbo].[acc_monitor_log_sj] 
(
    [status], 
    [time], 
    [pin], 
    [device_id], 
    [device_name], 
    [verified], 
    [state], 
    [event_type], 
    [event_point_type], 
    [event_point_id], 
    [event_point_name]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Querys para obtener la información de Correo Electronico
server_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'server'"""

port_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'port'"""

endpoint_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'mail_endpoint'"""

user_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'user_login_mail'"""

password_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'user_password_mail'"""
                
# Horas para validar y migrar la data para las tablas iclock y acc_monitor_log
time_validate_iclock_data_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 14 AND prm_descripcion = 'num_hours_iclock'"""

time_validate_acc_monitor_log_data_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 14 AND prm_descripcion = 'num_hours_acc_monitor_log'"""

time_validate_acc_monitor_log_sj_data_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 14 AND prm_descripcion = 'num_hours_sj_acc_monitor_log'"""

def get_time_validate_iclock_data():
    return execute_select_query(1, time_validate_iclock_data_query)

def get_time_validate_acc_monitor_log_data():
    return execute_select_query(1, time_validate_acc_monitor_log_data_query)

def get_time_validate_acc_monitor_log_sj_data():
    return execute_select_query(1, time_validate_acc_monitor_log_sj_data_query)

# Correo Electronico
def get_port_mail():
    return execute_select_query(1, port_mail_query)

def get_server_mail():
    return execute_select_query(1, server_mail_query)

def get_user_mail():
    return execute_select_query(1, user_mail_query)

def get_pass_mail():
    return execute_select_query(1, password_mail_query)

def get_user_endpoint():
    return execute_select_query(1, endpoint_mail_query)

# Función para obtener los parametros DSS
def get_host_dss_query():
    return execute_select_query(1, host_dss_query)

def get_port_dss_query():
    return execute_select_query(1, port_dss_query)

def get_user_dss_query():
    return execute_select_query(1, user_dss_query)

def get_password_dss_query():
    return execute_select_query(1, password_dss_query)

def get_enpoint_access_record_dss_query():
    return execute_select_query(1, enpoint_access_record_dss_query)

def get_temp_dss_query():
    return execute_select_query(1, temp_dss_query)

# Función para registrar logs en la base de datos
def insert_iclock(data: tuple):
    return execute_insert_query(2, insert_iclock_query, data)

def insert_acc_monitor_log(data: tuple):
    return execute_insert_query(1, insert_acc_monitor_log_query, data)

def insert_iclock_sj(data: tuple):
    return execute_insert_query(3, insert_iclock_query, data)

def insert_acc_monitor_log_sj(data: tuple):
    return execute_insert_query(1, insert_acc_monitor_log_sj_query, data)

# Validar el tiempo de ultimo registro del usuario

time_max_time_iclock_query = """SELECT MAX([punch_time]) FROM [dbo].[iclock_transaction] WHERE [emp_code] = ?"""

time_max_time_acc_monitor_log_query = """SELECT MAX([time]) FROM [dbo].[acc_monitor_log] WHERE [pin] = ?"""

time_max_time_iclock_sj_query = """SELECT MAX([punch_time]) FROM [dbo].[iclock_transaction_sj] WHERE [emp_code] = ?"""

time_max_time_acc_monitor_log_sj_query = """SELECT MAX([time]) FROM [dbo].[acc_monitor_log_sj] WHERE [pin] = ?"""

def get_last_log_time_iclock(person_id):
    return execute_select_tuple_query(2, time_max_time_iclock_query, (person_id,))

def get_last_log_time_acc_monitor_log(person_id):
    return execute_select_tuple_query(1, time_max_time_acc_monitor_log_query, (person_id,))

def get_last_log_time_iclock_sj(person_id):
    return execute_select_tuple_query(2, time_max_time_iclock_sj_query, (person_id, ))

def get_last_log_time_acc_monitor_log_sj(person_id):
    return execute_select_tuple_query(1, time_max_time_acc_monitor_log_sj_query, (person_id,))

# Validar serial_number Biometricos
sn_alias_query = """SELECT sn, alias FROM iclock_terminal"""

def get_sn_db():
    """Obtiene listas separadas de SN y alias desde la base de datos."""
    results = execute_select_multiples_rows_query(2, sn_alias_query)
    if not results:
        return [], []
    
    # Construir la lista completa de SN y alias
    sn_list = [row[0] for row in results]      # Lista con todos los SN
    alias_list = [row[1] for row in results]     # Lista con todos los alias
    return sn_list, alias_list

# Validar emp_id
emp_id_query = """SELECT id FROM personnel_employee WHERE emp_code = ? GROUP BY id"""

def get_emp_id_db(emp_code):
    return execute_select_tuple_query(2, emp_id_query, emp_code)

# Activar opcion de enviar correo
send_mail_status_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'send_mail'"""

def get_send_mail_status():
    return execute_select_query(1, send_mail_status_query)

# Validacion de empleados

get_employee_by_id_query = """SELECT emp_code, first_name FROM personnel_employee WHERE emp_code = ?"""

update_employee_by_id_query = """UPDATE personnel_employee
                            SET first_name = ?
                            WHERE emp_code = ?"""

insert_personnel_employee = """INSERT INTO [dbo].[personnel_employee](
	[create_time],
    [change_time],
    [status],
    [emp_code],
    [first_name],
    [photo],
    [self_password],
    [dev_privilege],
    [acc_group],
    [acc_timezone],
    [enroll_sn],
    [update_time],
    [hire_date],
    [verify_mode],
    [is_admin],
    [enable_att],
    [enable_overtime],
    [enable_holiday],
    [deleted],
    [reserved],
    [del_tag],
    [app_status],
    [app_role],
    [is_active],
    [department_id],
    [enable_payroll]
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

def get_employee_by_id(emp_code):
    return execute_select_tuple_query(2, get_employee_by_id_query, emp_code)

def get_update_employee(firstName, emp_code):
    return execute_update_query(2, update_employee_by_id_query, (firstName, emp_code))

# Rango de tiempo para enviar el correo electronico establacido desde las 6am hasta las 8pm

time_start_range_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'start_time_send_mail_range'"""

time_end_range_mail_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 8 AND prm_descripcion = 'end_time_send_mail_range'"""

def get_time_start_range_mail():
    return execute_select_query(1, time_start_range_mail_query)

def get_time_end_range_mail():
    return execute_select_query(1, time_end_range_mail_query)

# Numero de registros que se obtendran de DSS en la función get_data_iclock y get_data_acc_monitor_log

record_limit_iclock_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 14 AND prm_descripcion = 'record_limit_dss'"""

def get_record_limit_iclock():
    return execute_select_query(1, record_limit_iclock_query)

record_limit_acc_monitor_log_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 14 AND prm_descripcion = 'record_limit_acc_monitor_log'"""

def get_record_limit_acc_monitor_log_query():
    return execute_select_query(1, record_limit_acc_monitor_log_query)

record_limit_acc_monitor_log_sj_query = """SELECT prm_valor
                FROM dbo.Parametros_Sistema
                WHERE id_grupo = 14 AND prm_descripcion = 'record_limit_sj_acc_monitor_log'"""

def get_record_limit_acc_monitor_log_sj():
    return execute_select_query(1, record_limit_acc_monitor_log_sj_query)