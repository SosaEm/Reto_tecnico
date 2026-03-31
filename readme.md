Codeable Challenge
Descripción del Proyecto

Este proyecto implementa una solución de Ingeniería de Datos end-to-end para Codeable. El sistema procesa transacciones en tiempo real, realiza limpieza de datos, detecta patrones de fraude y almacena la información en un Data Warehouse (MySQL).

Arquitectura del Sistema

El pipeline sigue una arquitectura funcional de 3 capas:

    Data Lake (Bronce): Almacenamiento de archivos CSV crudos generados cada minuto.

    Procesamiento (Plata): Script en Python (main.py) que limpia, valida y aplica reglas de negocio para detección de fraude.

    Data Warehouse (Oro): Almacenamiento en MySQL usando un modelo de estrella para optimizar consultas analíticas.

Tecnologías Utilizadas

    Lenguaje: Python 3.x

    Librerías: Pandas (Procesamiento), SQLAlchemy (ORM/Carga), MySQL-Connector.

    Base de Datos: MySQL 8.0

    Entorno: Virtualenv / Pip

Configuración y Ejecución
1. Requisitos Previos

    Tener instalado MySQL Server.

    Crear la base de datos ejecutando el script SQL:
    Bash

    mysql -u tu_usuario -p < schema.sql

2. Instalación
Bash

# Clonar el repositorio
gh repo clone SosaEm/Reto_tecnico
cd <Reto_Tecnico>

# Instalar dependencias
pip install -r requirements.txt

3. Ejecución

Para iniciar el pipeline generador y procesador:
Bash

python main.py

Decisiones Técnicas y Metodología
1. Limpieza de Datos (clean_data)

    Se implementó un manejo robusto de outliers mediante el método de Rango Intercuartílico (IQR).

    Estandarización de formatos de fecha y códigos de país para asegurar la integridad referencial.

    Tratamiento de nulos en montos usando la mediana para evitar sesgos en reportes financieros.

2. Detección de Fraude (detect_suspicious_transactions)

Se aplicaron 3 reglas de negocio clave:

    Velocity Checks: Identificación de usuarios con más de 3 intentos fallidos en un mismo batch.

    Alertas de Monto: Transacciones por encima del percentil 99.

    Anomalías Temporales: Flaggeo de transacciones nocturnas (2 AM - 5 AM) para auditoría.

3. Modelo Dimensional (Fase 3)

Se optó por un Modelo en Estrella (Star Schema) compuesto por:

    fact_transactions: Tabla central de hechos con métricas de negocio.

    dim_users, dim_merchants, dim_payment_methods: Dimensiones descriptivas.

    dim_time: Dimensión temporal generada para facilitar análisis por franjas horarias y días de la semana.

Trabajo Pendiente / Próximos Pasos (Escalabilidad)

Debido a la restricción de tiempo (24h) y limitacion de conocimientos robustos, quedaron como propuestas de mejora:

    Idempotencia: Implementar una capa de Staging en MySQL para manejar duplicados con UPSERT.

    Orquestación: Migrar el loop de time.sleep() a Apache Airflow para mayor observabilidad.

    Contenedores: Dockerizar la aplicación completa para despliegue inmediato.

    Streaming: Implementar Kafka (Fase 4) para alertas de fraude con latencia de milisegundos.

Nota: El pipeline de carga a MySQL fue validado localmente. En caso de persistir tablas vacías durante la revisión, se adjuntan los archivos CSV en ./processed que contienen la salida exacta del proceso de transformación

Autor

    Emiliano Sosa - www.linkedin.com/in/emiliano-m-sosa - https://github.com/SosaEm