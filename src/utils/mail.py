import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from database.db import get_server_mail, get_port_mail, get_user_mail, get_pass_mail, get_user_endpoint, log_to_db, get_send_mail_status, get_time_start_range_mail, get_time_end_range_mail
from datetime import datetime, timedelta, time

def get_start_end_time_to_send_mail():
    now_time = datetime.now()
    offset = timedelta(hours=6)
    hour_start = get_time_start_range_mail()[0]
    end_start = get_time_end_range_mail()[0]
    # Combina una fecha arbitraria (por ejemplo, hoy) con el tiempo definido y suma el offset
    start_time = (datetime.combine(datetime.today(), time(int(hour_start), 0)) + offset).time()
    end_time = (datetime.combine(datetime.today(), time(int(end_start), 0)) + offset).time()

    return start_time, end_time, now_time

def status_send_mail():
    result = get_send_mail_status()
    if result is None:
        print("No se obtuvo un valor de la base de datos 'status'. Se desactiva el envío de correos.")
        return False
    status = result[0]
    # print(f'Valor obtenido de la base de datos "status": {status}')
    boolean = True
    if status == 'E':
        boolean = True
    else:
        boolean = False
    return boolean

def send_mail(message):

    start_time, end_time, now_time = get_start_end_time_to_send_mail()

    if now_time.time() < start_time or now_time.time() > end_time:
        print("El horario actual no se encuentra dentro del rango de envío de correos.")
        return

    boolean = status_send_mail()

    if boolean == True:
        try:
            # Obtener datos de configuración y extraer el primer elemento de la tupla
            smtp_server = str(get_server_mail()[0])
            smtp_port = int(get_port_mail()[0])
            user_mail = str(get_user_mail()[0])
            pass_mail = str(get_pass_mail()[0])
            endpoint_mail = str(get_user_endpoint()[0])
            
            # Validar datos básicos
            if not smtp_server or not smtp_port:
                raise ValueError("El servidor SMTP o el puerto no están configurados correctamente.")
            
            # print(f"Conectando a {smtp_server}:{smtp_port}...")
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            server.ehlo()

            # Autenticación
            server.login(user_mail, pass_mail)

            # Crear el mensaje multipart (alternativo)
            msg = MIMEMultipart("alternative")
            msg['Subject'] = "API Dahua - Notificación"
            msg['From'] = endpoint_mail
            msg['To'] = endpoint_mail

            # Crear la versión de texto plano
            text = f"Mensaje:\n{message}"
            
            # Crear la versión HTML con estilos
            html = f"""\
            <html>
            <head>
                <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    background-color: #f4f4f4;
                }}
                .container {{
                    margin: 20px auto;
                    padding: 20px;
                    max-width: 600px;
                    background-color: #fff;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    box-shadow: 0 2px 3px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #344888;
                }}
                p {{
                    margin: 15px 0;
                }}
                </style>
            </head>
            <body>
                <div class="container">
                <h1>Notificación de la API Dahua</h1>
                <p>{message}</p>
                </div>
            </body>
            </html>
            """

            # Convertir las cadenas a objetos MIMEText
            part1 = MIMEText(text, "plain")
            part2 = MIMEText(html, "html")

            # Adjuntar ambas partes al mensaje
            msg.attach(part1)
            msg.attach(part2)

            # Enviar el correo
            server.sendmail(user_mail, endpoint_mail, msg.as_string())
            server.quit()
            print("Correo enviado exitosamente.")
        except smtplib.SMTPException as smtp_error:
            print(f"Error con el servidor SMTP: {smtp_error}")
        except ValueError as value_error:
            print(f"Error en los datos proporcionados: {value_error}")
        except Exception as e:
            message_error = f"Error con la funcion enviar correo, {e}"
            print(message_error)
            log_to_db(2, 1, "ERROR", message_error, endpoint='mail', status_code=404)
        finally:
            if server:
                server.quit()
    else:
        print("Envío de correos desactivado.")
        log_to_db(2, 2, "INFO", "Envío de correos desactivado", endpoint='mail', status_code=200)