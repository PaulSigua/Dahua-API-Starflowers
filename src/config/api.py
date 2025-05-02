import hashlib
import requests
import sched
import time
import threading
from database.db import get_host_dss_query, get_port_dss_query, get_user_dss_query, get_password_dss_query, get_temp_dss_query, log_to_db
from utils.mail import send_mail
from fastapi import HTTPException

class TokenManager:
    def __init__(self):
        self.token = None
        self.lock = threading.Lock()
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.keepalive_interval = 22      # Segundos
        self.update_token_interval = 1080 # 18 minutos

    def _first_authentication(self):
        """Realiza la autenticación inicial para obtener realm y randomKey."""
        try:
            response = requests.post(
                url=f'https://{get_host_dss_query()[0]}:{get_port_dss_query()[0]}/brms/api/v1.0/accounts/authorize',
                json={'userName': get_user_dss_query()[0], "clientType": "WINPC_V2"},
                verify=False
            )
            if not response.text.strip():
                print("Error en first_authentication: respuesta vacía")
                return {}
            return response.json()
        except Exception as e:
            print("Error en first_authentication:", e)
            return {}

    def _get_signature(self, realm, randomKey):
        """Genera la firma MD5 para la autenticación."""
        try:
            temp1 = hashlib.md5(str(get_password_dss_query()[0]).encode('utf-8')).hexdigest()
            temp2 = hashlib.md5(f'{get_user_dss_query()[0]}{temp1}'.encode('utf-8')).hexdigest()
            temp3 = hashlib.md5(temp2.encode('utf-8')).hexdigest()
            temp4 = hashlib.md5(f'{get_user_dss_query()[0]}:{realm}:{temp3}'.encode('utf-8')).hexdigest()
            return hashlib.md5(f'{temp4}:{randomKey}'.encode('utf-8')).hexdigest()
        except Exception as e:
            print("Error generando firma:", e)
            return None

    def _second_authentication(self, signature, randomKey):
        """Realiza la segunda fase de autenticación para obtener el token."""
        first_auth_resp = self._first_authentication()
        data = {
            "userName": get_user_dss_query()[0],
            "signature": signature,
            "randomKey": randomKey,
            "publicKey": first_auth_resp.get("publickey", ""),
            "encryptType": "MD5",
            "ipAddress": "",
            "clientType": "WINPC_V2",
            "userType": "0"
        }
        try:
            resp = requests.post(
                url=f'https://{get_host_dss_query()[0]}:{get_port_dss_query()[0]}/brms/api/v1.0/accounts/authorize',
                json=data,
                verify=False
            )
            if not resp.text.strip():
                print("Error en second_authentication: respuesta vacía")
                return None
            return resp.json()
        except Exception as e:
            print("Error en second_authentication:", e)
            return None

    def get_token(self):
        """Obtiene el token; si ya existe, se verifica su validez."""
        with self.lock:
            if self.token:
                headers = {'X-Subject-Token': self.token}
                try:
                    response = requests.put(
                        url=f'https://{get_host_dss_query()[0]}:{get_port_dss_query()[0]}/brms/api/v1.0/accounts/keepalive',
                        headers=headers,
                        data="{}",
                        verify=False
                    )
                    if response.status_code != 200:
                        self.token = None
                except Exception as e:
                    print("Error al verificar token:", e)
                    self.token = None

            if not self.token:
                first_auth_resp = self._first_authentication()
                if "realm" not in first_auth_resp or "randomKey" not in first_auth_resp:
                    print("Error: No se pudo obtener realm o randomKey.")
                    return None
                realm = first_auth_resp["realm"]
                randomKey = first_auth_resp["randomKey"]
                signature = self._get_signature(realm, randomKey)
                if not signature:
                    print("Error: No se pudo generar la firma.")
                    return None
                auth_response = self._second_authentication(signature, randomKey)
                if not auth_response:
                    print("Error: Autenticación fallida.")
                    return None
                self.token = auth_response.get("accessToken") or auth_response.get("token")
                if not self.token:
                    print("Error: No se recibió token de acceso.")
                    return None
            return self.token

    def keepalive(self):
        """Envía un keepalive para mantener la sesión activa."""
        if not self.token:
            return
        try:
            headers = {'X-Subject-Token': self.token}
            response = requests.put(
                url=f'https://{get_host_dss_query()[0]}:{get_port_dss_query()[0]}/brms/api/v1.0/accounts/keepalive',
                headers=headers,
                data="{}",
                verify=False
            )
            if response.status_code != 200:
                print("Keepalive fallido:", response.status_code)
        except Exception as e:
            print("Error en keepalive:", e)

    def update_token(self):
        """Renueva el token de acceso."""
        with self.lock:
            if not self.token:
                return None
            try:
                signature = self._get_signature_for_update_token(self.token)
                headers = {'X-Subject-Token': self.token}
                data = {"signature": signature}
                response = requests.post(
                    url=f'https://{get_host_dss_query()[0]}:{get_port_dss_query()[0]}/brms/api/v1.0/accounts/updateToken',
                    headers=headers,
                    json=data,
                    verify=False
                )
                if response.status_code == 200:
                    updated_token = response.json().get("data", {}).get("token")
                    if updated_token:
                        self.token = updated_token
                        return updated_token
                    else:
                        print("Error: No se recibió nuevo token.")
                        return None
                else:
                    print("Error al actualizar token:", response.status_code)
                    return None
            except Exception as e:
                print("Error en update_token:", e)
                return None

    def _get_signature_for_update_token(self, old_token):
        """Genera la firma necesaria para actualizar el token."""
        try:
            temp = f"{get_temp_dss_query()[0]}"
            return hashlib.md5(f'{temp}:{old_token}'.encode('utf-8')).hexdigest()
        except Exception as e:
            print("Error generando firma para actualizar el token:", e)
            return None

    def start(self):
        """Inicia las tareas periódicas de keepalive y actualización del token."""
        def periodic_tasks():
            self.keepalive()
            self.update_token()
            self.scheduler.enter(self.keepalive_interval, 1, periodic_tasks)
            self.scheduler.enter(self.update_token_interval, 1, periodic_tasks)
        threading.Thread(target=self.scheduler.run, daemon=True).start()
        self.scheduler.enter(0, 1, periodic_tasks)

# Instancia global del TokenManager
token_manager = TokenManager()

def get_global_token():
    try:
        return token_manager.get_token()
    except Exception as e:
        message = "Error obteniendo token global:", e
        send_mail(message)
        log_to_db(2, 1, 'ERROR', message, 'get_global_token', 500)
        return None
