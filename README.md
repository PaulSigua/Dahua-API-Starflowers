# Dahua Record Assistance

![Logo del Proyecto](https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png)

## Tabla de Contenidos
- [Descripción](#descripción)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)

## Descripción
Este repositorio contiene lo necesario para instalar y desplegar la API en cualquier entorno de Python, facilitando la integración de las funcionalidades de Dahua.

## Instalación

### Requerimientos
- Python 3.12 o superior
- pip
- Git

### Pasos

1. **Clonar el repositorio:**  
   Ejecuta el siguiente comando en tu terminal:

    `git clone https://github.com/SiguaPaul/Dahua-API.git`

2. **Acceder al directorio del repositorio:**

    `cd Dahua-API`

3. **Crear y activar un entorno virtual:**

    Crear el entorno virtual:

    `python -m venv myenv`

4. **Activar el entorno:**

    Linux/macOS:

    `source myenv/bin/activate`

    Windows:

    `myenv\Scripts\activate.bat`

5. **Instalar las dependencias:**

    `pip install -r requirements.txt`

### Configuración
Crea un archivo .env dentro de la carpeta config del proyecto y agrega las líneas indicadas en la documentación correspondiente.

### Uso
    
Para ejecutar la API, utiliza el siguiente comando:
    
`python src/app.py`