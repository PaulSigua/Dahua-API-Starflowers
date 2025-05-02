import json
import os
from fastapi import HTTPException
from datetime import datetime, timedelta
from database.db import (validate_log_acc_monitor_log, validate_log_iclock, insert_acc_monitor_log, get_last_log_time_acc_monitor_log, 
                       get_last_log_time_iclock, insert_iclock, get_sn_db, get_emp_id_db, log_to_db, validate_log_acc_monitor_log_sj, 
                       get_last_log_time_acc_monitor_log_sj, insert_acc_monitor_log_sj, get_employee_by_id, get_update_employee)

def get_verify_type(pointName):
    try:
        if pointName in ['B_Sistemas_Tics_Door1', 'B_Talento_Humano_Door1',
                                        'B_Casilleros_Mujeres_Door1', 'B_Casilleros_Hombres_Door1',
                                        'B_Comedor_MH_Door1', 'B_Comedor_A4_Door1', 'B_Casilleros_A4_Door1']:
            return 16
        else:
            return 1
    except Exception as e:
        print(f'Error al obtener el tipo de verificación: {e}')
        return None

def get_terminal_sn(pointName):
    try:
        if pointName == 'B_Sistemas_Tics_Door1':
            return 'CL3S212060030'
        elif pointName == 'B_Talento_Humano_Door1':
            return 'CL3S212060030'
        elif pointName == 'B_Casilleros_Mujeres_Door1':
            return 'ECL3S211760086'
        elif pointName == 'B_Ventas_Door1':
            return 'AEWD201860278'
        elif pointName == 'B_Casilleros_Hombres_Door1':
            return 'CL3S211760214'
        elif pointName == 'B_Comedor_MH_Door1':
            return 'CL3S212060271'
        elif pointName == 'B_Comedor_A4_Door1':
            return 'A43S212060270'
        elif pointName == 'B_Casilleros_A4_Door1':
            return 'CL3S212060270'
        elif pointName == 'B_Contabilidad_Door1':
            return 'CL3S212060259'
        else:
            return None
    except Exception as e:
        print(f'ERROR {e}, no se encontro el serial de la terminal para el punto: {pointName}')

def get_terminal_alias(pointName):
    try:
        if pointName == 'B_Sistemas_Tics_Door1':
            return 'ZK_Administracion'
        elif pointName == 'B_Talento_Humano_Door1':
            return 'ZK_Administracion'
        elif pointName == 'B_Casilleros_Mujeres_Door1':
            return 'ZK_Mujeres'
        elif pointName == 'B_Ventas_Door1':
            return 'ZK_Cuenca'
        elif pointName == 'B_Casilleros_Hombres_Door1':
            return 'ZK_Hombres'
        elif pointName == 'B_Comedor_MH_Door1':
            return 'ZK_Comedor'
        elif pointName == 'B_Comedor_A4_Door1':
            return 'ZK_Comedor_A4'
        elif pointName == 'B_Casilleros_A4_Door1':
            return 'ZK_San_Juan'
        elif pointName == 'B_Contabilidad_Door1':
            return 'ZK_Cuenca_Ventas'
        else:
            return None
    except Exception as e:
        print(f'ERROR, {e} no se pudo encontrar el terminal_id para el punto {pointName}')


def get_area_alias(pointName):
    try:
        if pointName == 'B_Sistemas_Tics_Door1':
            return 'ADMINISTRACION'
        elif pointName == 'B_Talento_Humano_Door1':
            return 'ADMINISTRACION'
        elif pointName == 'B_Casilleros_Mujeres_Door1':
            return 'AGR'
        elif pointName == 'B_Ventas_Door1':
            return 'ADMINISTRACION'
        elif pointName == 'B_Casilleros_Hombres_Door1':
            return 'AGR'
        elif pointName == 'B_Comedor_MH_Door1':
            return 'GENERAL'
        elif pointName == 'B_Comedor_A4_Door1':
            return 'GENERAL'
        elif pointName == 'B_Casilleros_A4_Door1':
            return 'AGR'
        elif pointName == 'B_Contabilidad_Door1':
            return 'ADMINISTRACION'
        else:
            return 'GENERAL'
        # Obtenemos los datos del empleado
    except Exception as e:
        print(f'Error al obtener los datos del empleado: {e}')
        return None

def guardar_emp_sin_id(emp_id, emp_name, filename="empleados_sin_id.json"):
    """
    Guarda en un archivo JSON el emp_id y el nombre del empleado,
    evitando duplicados y validando que ambos campos sean válidos.
    """
    if emp_id is None or emp_name is None:
        print("Datos insuficientes: no se guardará el registro.")
        return

    registros = []
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            try:
                registros = json.load(f)
            except json.JSONDecodeError:
                registros = []

    if any(str(reg.get('emp_id')) == str(emp_id) for reg in registros):
        return  # Evitamos duplicados

    registros.append({'emp_id': emp_id, 'firstName': emp_name})
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(registros, f, ensure_ascii=False, indent=4)

def get_emp_id(entry):
    try:
        personId = entry.get('personId')
        personName = entry.get('firstName')
        # print(f'ID: {personId} - NOMBRE: {personName}')
        rows = get_emp_id_db(personId)
        for row in rows:
            if personId == '2287':
                return 7777
            elif row[0] is not None:
                return row[0]
            else:
                print(f'El empleado: {personId} no tiene ID')
                guardar_emp_sin_id(personId, personName)
                return 0
        print(f'No se encontraron registros para el empleado: {personId}')
        guardar_emp_sin_id(personId, personName)
        return None
    except Exception as e:
        print(f'Error al obtener el emp_id: {e}')
        guardar_emp_sin_id(personId, personName)
        return None

def get_terminal_id(pointName):
    try:
        
        if pointName == 'B_Sistemas_Tics_Door1':
            return 2
        elif pointName == 'B_Talento_Humano_Door1':
            return 2
        elif pointName == 'B_Casilleros_Mujeres_Door1':
            return 4
        elif pointName == 'B_Ventas_Door1':
            return 5
        elif pointName == 'B_Casilleros_Hombres_Door1':
            return 6
        elif pointName == 'B_Comedor_MH_Door1':
            return 9
        elif pointName == 'B_Comedor_A4_Door1':
            return 17
        elif pointName == 'B_Casilleros_A4_Door1':
            return 11
        elif pointName == 'B_Contabilidad_Door1':
            return 14
        else:
            return 0
    except Exception as e:
        print(f'Error al obtener el alias del terminal: {e}')
        return None

def get_upload_time():
    try:
        upload_time = datetime.now()
        ajuste_time = (upload_time - timedelta(hours=6))
        return ajuste_time
    except Exception as e:
        print(f'Error al obtener la hora de carga: {e}')
        return None
    
def validate_employee(personId):
    try:
        # Obtenemos los datos del empleado
        employees = get_emp_id_db(personId)
        if employees and not isinstance(employees, list):
            employees = [employees]
        return employees
    except Exception as e: 
        print("Error al validar el empleado.")
        return False

def validate_department_employee(pointName):
    if pointName:
        if pointName == 'B_Sistemas_Tics_Door1':
            return 2
        elif pointName == 'B_Talento_Humano_Door1':
            return 2
        elif pointName == 'B_Casilleros_Mujeres_Door1':
            return 4
        elif pointName == 'B_Ventas_Door1':
            return 2
        elif pointName == 'B_Casilleros_Hombres_Door1':
            return 4
        elif pointName == 'B_Casilleros_A4_Door1':
            return 4
        elif pointName == 'B_Contabilidad_Door1':
            return 2 
        else:
            return 3

def get_emp_code_and_name(personId):
    try:
        emp_list = get_employee_by_id(personId)
        # print(f'Empleado obtenido: {emp_list}')
        if emp_list and emp_list[0][0] == personId:
            # Obtener la tupla
            emp_code, emp_name = emp_list[0]
            # print(f'El empleado es correcto: {emp_code} - {emp_name}')
            return emp_code, emp_name
        else:
            print("El empleado no coincide o no se encontró.")
            return None, None
    except Exception as e:
        print(f'Error al obtener el emp_code y el nombre: {e}')
        return None, None

def migrate_db_iclock(data):
    try:
        print('')
        # Verificamos si 'pageData' está presente en los datos
        if "data" in data and "pageData" in data["data"]:
            for i, entry in enumerate(data["data"]["pageData"]):
                print(f'Migrando datos iclock {i+1}/{len(data["data"]["pageData"])}')

                personId = entry.get("personId")
                pointName = entry.get('pointName')
                firstName = entry.get('firstName')

                # Convertir alarmTime a objeto datetime y luego formatearlo
                alarm_time_str = entry.get("alarmTime")
                try:
                    alarm_time_obj = datetime.strptime(alarm_time_str, "%Y-%m-%d %H:%M:%S")
                    fecha_bio_val = datetime.strptime(alarm_time_str, "%Y-%m-%d %H:%M:%S").date()
                except Exception as e:
                    print(f'Error al convertir alarmTime: {alarm_time_str}, {e}')
                    continue

                time_val = alarm_time_obj
                punch_state = '0'
                verify_type = get_verify_type(pointName)
                work_code = ''
                terminal_sn = get_terminal_sn(pointName)
                terminal_alias = get_terminal_alias(pointName)
                area_alias = get_area_alias(pointName)
                source = 1
                purpose = 9
                crc = 'BADAAACAAADAAABACAJA'
                upload_time = get_upload_time()
                emp_id = None
                # if emp_id is None:
                #     print(f'Error: emp_id es None para el empleado {personId}. Registro omitido.')
                #     continue  # O asignar: emp_id = 0
                terminal_id = get_terminal_id(pointName)
                is_mask = 255
                temperature = 255.0
                Fecha_Bio = fecha_bio_val
                
                list_data_iclock_transaction = [
                    personId,
                    time_val,
                    punch_state,
                    int(verify_type),
                    work_code,
                    terminal_sn,
                    terminal_alias,
                    area_alias,
                    int(source),
                    int(purpose),
                    crc,
                    upload_time,
                    emp_id,
                    int(terminal_id),
                    int(is_mask),
                    int(temperature),
                    Fecha_Bio
                ]

                try:
                    empcode_, firstName_ = get_emp_code_and_name(personId)
                    if empcode_ == personId:
                        # print(f'El empleado es correcto: {firstName_}')
                        get_update_employee(firstName, personId)

                except Exception as e:
                    print(f'Ocurrio un error al obtener el empleado {e}')
                    continue

                try:
                    # Validar si ya existe un registro con el mismo time y pin en acc_monitor_log_dev
                    if not validate_log_iclock(2, alarm_time_obj, list_data_iclock_transaction[0]):
                        last_log_time = get_last_log_time_iclock(personId)
                        # print(f'Fecha del colaborador {personId} obtenido para last_log_time y validacion: {last_log_time}')
                        if last_log_time:
                            # Extraer el valor del primer registro y la primera columna
                            if isinstance(last_log_time, list) and len(last_log_time) > 0:
                                last_log_time_value = last_log_time[0][0]
                            else:
                                last_log_time_value = last_log_time[0]  # En caso de que no sea una lista de tuplas

                            # Si el valor es una cadena, conviértelo a datetime
                            if isinstance(last_log_time_value, str):
                                last_log_time_value = datetime.strptime(last_log_time_value, "%Y-%m-%d %H:%M:%S")
                            
                            # print(f'Fecha del colaborador {personId} procesado: {last_log_time_value}')
                            if last_log_time_value is not None:
                                time_diff = upload_time - last_log_time_value
                                if time_diff.total_seconds() < 1800:
                                    print(f'Registro rechazado clock para pin: {personId}. Ya existe un registro en los últimos 30 minutos.')
                                    continue
                                else:
                                    # Manejar el caso en que no se encontró un last_log_time válido.
                                    print(f'No se encontró un último log válido para el empleado {personId}.')

                        insert_iclock(list_data_iclock_transaction)
                        print('Log insertado correctamente.')
                    else:
                        print(f'Registro iclock duplicado encontrado para time: {list_data_iclock_transaction[1]} y pin: {list_data_iclock_transaction[0]}')
                except Exception as e:
                    print(f'Error al insertar iclock, {e}')

            return True
        else:
            print("No se encontraron datos en 'pageData'.")
            return False

    except HTTPException as http_ex:
        message = f'Error en la petición HTTP, {http_ex}'
        print(message)
        log_to_db(2, 1, 'ERROR', message, 'migrate_db', http_ex.status_code)
        return None

    except Exception as e:
        message = f'Error al migrar datos a la base de datos, {e}'
        print(message)
        log_to_db(2, 1, 'ERROR', message, 'migrate_db', 409)
        return None

def migrate_db_acc_manager_log(data):
    try:
        print('')
        # Verificamos si 'pageData' está presente en los datos
        if "data" in data and "pageData" in data["data"]:
            for i, entry in enumerate(data["data"]["pageData"]):
                print(f'Migrando datos acc_monitor_log {i+1}/{len(data["data"]["pageData"])}')

                personId = entry.get("personId")
                pointName = entry.get('pointName')

                # Convertir alarmTime a objeto datetime y luego formatearlo
                alarm_time_str = entry.get("alarmTime")
                try:
                    alarm_time_obj = datetime.strptime(alarm_time_str, "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f'Error al convertir alarmTime: {alarm_time_str}, {e}')
                    continue

                status = 200
                device_id = 15
                time_val = alarm_time_obj
                device_name = "ZK_Comedor_98"
                verified = 200
                state = 2
                event_type = 300
                event_point_type = 0
                event_point_id = 1
                event_point_name = "ZK_Comedor_98-1"

                list_data = [
                    int(status),
                    time_val,
                    personId,  # pin
                    int(device_id),
                    device_name,
                    int(verified),
                    int(state),
                    int(event_type),
                    int(event_point_type),
                    int(event_point_id),
                    event_point_name
                ]

                # Validar si ya existe un registro con el mismo time y pin en acc_monitor_log_dev
                if not validate_log_acc_monitor_log(1, alarm_time_obj, list_data[2]):
                    try:
                        if pointName == "B_Comedor_MH_Door1" and personId is not None:
                            last_log_time = get_last_log_time_acc_monitor_log(personId)
                            if last_log_time:
                                # Extraer el valor del primer registro y la primera columna
                                if isinstance(last_log_time, list) and len(last_log_time) > 0:
                                    last_log_time_value = last_log_time[0][0]
                                else:
                                    last_log_time_value = last_log_time[0]  # En caso de que no sea una lista de tuplas

                                # Si el valor es una cadena, conviértelo a datetime
                                if isinstance(last_log_time_value, str):
                                    last_log_time_value = datetime.strptime(last_log_time_value, "%Y-%m-%d %H:%M:%S")

                                # print(f'Fecha del colaborador {personId} procesado: {last_log_time_value}')
                                date_now = get_upload_time()
                                if last_log_time_value is not None:
                                    time_diff = date_now - last_log_time_value
                                    if time_diff.total_seconds() < 1800:
                                        print(f'Registro rechazado acc manager log para pin: {personId}. Ya existe un registro en los últimos 30 minutos.')
                                        continue
                                    else:
                                        # Manejar el caso en que no se encontró un last_log_time válido.
                                        print(f'No se encontró un último log válido para el empleado {personId}.')
                            
                            # Insertar el log en la base de datos
                            insert_acc_monitor_log(list_data)
                            # print('Datos migrados correctamente.')
                        else:
                            print(f'Dispositivo {pointName} no válido.')
                            continue
                    except Exception as e:
                        print(f'Error al insertar log de monitorización, {e}')
                else:
                    print(f'Registro log monitor duplicado encontrado para time: {list_data[1]} y pin: {list_data[2]}')

            return True
        else:
            print("No se encontraron datos en 'pageData'.")
            return False

    except HTTPException as http_ex:
        message = f'Error en la petición HTTP, {http_ex}'
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'migrate_db', http_ex.status_code)
        return None

    except Exception as e:
        message = f'Error al migrar datos a la base de datos, {e}'
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'migrate_db', 409)
        return None

def migrate_db_data_sj(data):
    try:
        print('')
        # Verificamos si 'pageData' está presente en los datos
        if "data" in data and "pageData" in data["data"]:
            for i, entry in enumerate(data["data"]["pageData"]):
                print(f'Migrando datos sj {i+1}/{len(data["data"]["pageData"])}')

                personId = entry.get("personId")
                pointName = entry.get('pointName')

                # Convertir alarmTime a objeto datetime y luego formatearlo
                alarm_time_str = entry.get("alarmTime")
                try:
                    alarm_time_obj = datetime.strptime(alarm_time_str, "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f'Error al convertir alarmTime: {alarm_time_str}, {e}')
                    continue

                status = 200
                device_id = 16
                time_val = alarm_time_obj
                device_name = "ZK_Comedor_99"
                verified = 200
                state = 2
                event_type = 300
                event_point_type = 0
                event_point_id = 1
                event_point_name = "ZK_Comedor_99-1"

                list_data = [
                    int(status),
                    time_val,
                    personId,  # pin
                    int(device_id),
                    device_name,
                    int(verified),
                    int(state),
                    int(event_type),
                    int(event_point_type),
                    int(event_point_id),
                    event_point_name
                ]

                # Validar si ya existe un registro con el mismo time y pin en acc_monitor_log_dev
                if not validate_log_acc_monitor_log_sj(1, alarm_time_obj, list_data[2]):
                    try:
                        if pointName == "B_Comedor_A4_Door1" and personId is not None:
                            last_log_time = get_last_log_time_acc_monitor_log_sj(personId)
                            if last_log_time:
                                # Extraer el valor del primer registro y la primera columna
                                if isinstance(last_log_time, list) and len(last_log_time) > 0:
                                    last_log_time_value = last_log_time[0][0]
                                else:
                                    last_log_time_value = last_log_time[0]  # En caso de que no sea una lista de tuplas

                                # Si el valor es una cadena, conviértelo a datetime
                                if isinstance(last_log_time_value, str):
                                    last_log_time_value = datetime.strptime(last_log_time_value, "%Y-%m-%d %H:%M:%S")

                                # print(f'Fecha del colaborador {personId} procesado: {last_log_time_value}')
                                date_now = get_upload_time()
                                if last_log_time_value is not None:
                                    time_diff = date_now - last_log_time_value
                                    if time_diff.total_seconds() < 1800:
                                        print(f'Registro rechazado acc manager log para pin: {personId}. Ya existe un registro en los últimos 30 minutos.')
                                        continue
                                    else:
                                        # Manejar el caso en que no se encontró un last_log_time válido.
                                        print(f'No se encontró un último log válido para el empleado {personId}.')
                            
                            # Insertar el log en la base de datos
                            insert_acc_monitor_log_sj(list_data)
                            # print('Datos migrados correctamente.')
                        else:
                            print(f'Dispositivo {pointName} no válido.')
                            continue
                    except Exception as e:
                        print(f'Error al insertar log de monitorización, {e}')
                else:
                    print(f'Registro log monitor duplicado encontrado para time: {list_data[1]} y pin: {list_data[2]}')

            return True
        else:
            print("No se encontraron datos en 'pageData'.")
            return False

    except HTTPException as http_ex:
        message = f'Error en la petición HTTP, {http_ex}'
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'migrate_db', http_ex.status_code)
        return None

    except Exception as e:
        message = f'Error al migrar datos a la base de datos, {e}'
        print(message)
        log_to_db(1, 1, 'ERROR', message, 'migrate_db', 409)
        return None
