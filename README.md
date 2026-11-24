# 🌦️ Weather Ledger: Modern Data Lakehouse Pipeline

[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5.2-red.svg)](https://iceberg.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

![image](https://github.com/user-attachments/assets/b6f825ff-4633-4c34-a8ea-8d3c861ed307)
**Un pipeline de datos escalable y con garantías ACID, que ingesta, procesa y almacena datos meteorológicos utilizando la Arquitectura Medallón.**

## 📖 Sobre el Proyecto

**Weather Ledger** es un proyecto moderno de ingeniería de datos diseñado para extraer datos meteorológicos en tiempo real, procesarlos y almacenarlos en un formato confiable y consultable.

Construido desde cero para cerrar la brecha entre los requisitos del negocio y la ejecución técnica, este proyecto demuestra las mejores prácticas en la construcción de un **Data Lakehouse**. Utiliza **Apache Iceberg** para resolver los desafíos tradicionales de los data lakes (como la evolución del esquema y las transacciones ACID), mientras emplea **Apache Spark** para el procesamiento distribuido.

Los flujos de datos meteorológicos deben lidiar con algo más que registros limpios y perfectamente ordenados. Una misma observación puede llegar más de una vez, una estación puede corregir una lectura y una observación puede llegar después de su fecha original.

Este proyecto crea un <MARK>**Lakehouse**</MARK> meteorológico de extremo a extremo utilizando <mark>**Apache Iceberg**</mark> para demostrar cómo se pueden manejar esos cambios, al tiempo que se mantiene la precisión de los resultados analíticos y se ponen a disposición para su inspección los estados anteriores de las tablas.

El canal de datos fluye de **Bronce** → **Plata** → **Oro**:

* **Capa de Bronce**: conserva los eventos meteorológicos entrantes, incluyendo las entregas repetidas y las correcciones.

* **Capa de Plata**: mantiene una observación actual para cada estación y cada marca de tiempo de observación.

* **Capa de Oro**: genera métricas meteorológicas diarias para análisis y paneles de control.

📋 <mark>**Nota sobre el conjunto de datos**</mark>: todas las mediciones meteorológicas son datos sintéticos generados localmente para este proyecto. No son lecturas en vivo de estaciones meteorológicas reales.

___________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🧭 Navegación Rápida






__________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura

El pipeline sigue la **Arquitectura de Medalla** (Bronce, Plata, Oro), implementando actualmente la ingesta fundamental de la capa Bronce.

```mermaid
%%{init: {'theme': 'default', 'themeVariables': { 'fontFamily': 'Segoe UI, Arial, sans-serif', 'lineColor': '#555555'}}}%%
flowchart TD
    %% ESTILOS DE NODOS (Paleta profesional)
    classDef dataSource fill:#F0F4F8,stroke:#4A90E2,stroke-width:2px,color:#333333,rx:5px,ry:5px;
    classDef bronze fill:#FFF3E0,stroke:#F5A623,stroke-width:3px,color:#333333,rx:5px,ry:5px;
    classDef silver fill:#E8F5E9,stroke:#50E3C2,stroke-width:3px,color:#333333,rx:5px,ry:5px;
    classDef gold fill:#FFFDE7,stroke:#F8E71C,stroke-width:3px,color:#333333,rx:5px,ry:5px;
    classDef presentation fill:#F5F5F5,stroke:#9B9B9B,stroke-width:2px,color:#333333,rx:5px,ry:5px;
    classDef timeTravel fill:#F3E5F5,stroke:#9013FE,stroke-width:2px,stroke-dasharray: 5 5,color:#333333,rx:5px,ry:5px;

    %% NODOS
    A(["<b>Synthetic Weather Data</b><br/><font size='2'>Generator</font>"]):::dataSource
    
    B[("<b>Landing CSV Files</b><br/><font size='2'>Baseline + Change Batch</font>")]:::dataSource

    C[("<b>🥉 BRONZE ICEBERG TABLE</b><br/><br/><font size='2'><i>Append-only event history</i></font>")]:::bronze

    D[("<b>🥈 SILVER ICEBERG TABLE</b><br/><br/><font size='2'><i>Deduplicate + MERGE INTO</i></font>")]:::silver

    E[("<b>🥇 GOLD ICEBERG TABLE</b><br/><br/><font size='2'><i>Daily station metrics</i></font>")]:::gold

    F[("<b>Daily Summary CSV</b>")]:::presentation

    G{{"<b>Power BI Dashboard</b>"}}:::presentation

    H[("<b>Earlier Silver State</b><br/><font size='2'>Snapshots + Time Travel</font>")]:::timeTravel

    %% CONEXIONES
    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G

    %% Ramificación del Time Travel (Línea punteada lateral)
    D -.-> H

    %% Estilos de línea
    linkStyle default stroke:#555555,stroke-width:2px;
    linkStyle 6 stroke:#9013FE,stroke-width:2px,stroke-dasharray: 5 5;
```

___________________________________________________________________________________________________________________________________________________________________________________________________________________

## 💡El problema que resuelve

Imagina que una estación meteorológica envía una observación y, más tarde, llega una versión corregida. Otra observación se envía dos veces. Mientras tanto, una lectura de un día anterior llega con retraso.

Un proceso confiable debería poder:

| Desafío | Lo que el proceso debe hacer |
|---------|------------------------------|
| Envíos duplicados | Conservar los eventos recibidos en **Bronze** y mantener una sola fila actual en **Silver** |
| Observaciones corregidas | Actualizar la observación actual cuando llegue una revisión más reciente |
| Datos que llegan tarde | Agregar la observación a la estación y fecha históricas correctas y actualizar las métricas diarias |
| Inspección histórica | Consultar una instantánea anterior de Iceberg para ver el estado de la tabla antes de un cambio |
| Análisis confiables | Recalcular los resúmenes diarios a partir del estado actual en Silver |

📌 El objetivo no es simplemente mover un archivo CSV a través de tres carpetas. Es demostrar cómo un formato de tabla como Apache Iceberg puede respaldar un flujo de trabajo de <mark>**lakehouse**</mark> en el que los datos cambian con el tiempo.

___________________________________________________________________________________________________________________________________________________________________________________________________________________

## ![image](https://github.com/user-attachments/assets/ccd89b2c-2ed5-4358-a64e-f3d5a5cf0f37) Características de Apache Iceberg

Este proyecto se centra en un conjunto práctico de capacidades de Iceberg y muestra cómo se integran en un flujo de datos.

| Característica | Cómo se utiliza en este proyecto |
|----------------|----------------------------------|
| Tablas y catálogo de Iceberg | Almacenar Bronze, Silver y Gold como tablas de Iceberg a través del catálogo de Iceberg de Spark |
| Añadir confirmaciones | Agregar eventos entrantes a la tabla Bronze sin perder las entregas repetidas |
| MERGE INTO / upserts | Actualiza las observaciones corregidas e inserta las observaciones recién llegadas en Silver |
| Historial de instantáneas | Revisa las instantáneas de tablas confirmadas a través de las tablas de metadatos de Iceberg |
| Tablas de metadatos | Consulta .snapshots para revisar los ID de las instantáneas, las operaciones y el historial de confirmaciones |
| Viaje en el tiempo | Consulta una instantánea guardada de Silver para recuperar el valor anterior a la corrección |
| Actualización de Gold | Vuelve a calcular los análisis diarios a partir de la tabla Silver actualizada y confirma el estado actualizado de Gold |

___________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧰 Pila tecnológica

| Tecnología | Función |
|------------|---------|
| Apache Iceberg 1.9.2 | Formato de tabla, confirmaciones, historial de instantáneas, consultas de metadatos y **viaje en el tiempo** | 
| Apache Spark 3.5.1 | Motor de ejecución distribuida |
| PySpark | Ingesta de datos, validación, deduplicación, fusiones y agregación |
| Biblioteca estándar de Python | Generar entrada CSV sintética determinista |
| Spark SQL | Inspeccionar las tablas y verificar el comportamiento del flujo de trabajo |
| Docker / Docker Compose | Ejecutar el entorno local de Spark |
| Power BI Desktop | Visualizar el resumen Gold exportado |

___________________________________________________________________________________________________________________________________________________________________________________________________________________

## 🗃️ Tablas Iceberg

| Capa | Tabla | Responsabilidad |
|------|-------|-----------------|
| 🟤 Bronce | <mark>local.weather_demo.bronze_events</mark> | Historial de eventos meteorológicos recibidos, solo de adición |
| ⚪ Plata | <mark>local.weather_demo.silver_observations</mark> | Una observación actual por clave (**station_id, observed_at**) |
| 🟡 Oro | <mark>local.weather_demo.gold_daily_summary</mark> | Un registro analítico por estación y fecha del calendario |


* **Ubicación del almacén dentro del entorno de Spark:** /opt/iceberg/warehouse/weatherledger_project

___________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📂 Estructura del Proyecto

weather_ledger/
├── apps/
|   └── weatherledger_project/
│       ├── generate_demo_data.py          # Creates the baseline and change CSV files
|       ├── bronze_ingest.py               # Appends incoming events to Bronze
|       ├── silver_upsert.py               # Validates, deduplicates, and MERGEs into Silver
|       ├── gold_analytics.py              # Builds daily analytics and exports the CSV
|       └── sql/
|           └── inspection_queries.sql     # Validation, history, time travel, and Gold queries
├── data/                                  # Volcados de datos crudos locales (si aplica)
├── iceberg-warehouse/                     # Volumen montado para el almacenamiento de tablas Iceberg
├── jars/                                  # Dependencias de Spark (ej. runtime de Iceberg)
├── docker-compose.yml                     # Configuración del clúster Spark (Master/Worker)
└── README.md                              # ¡Estás aquí!
    
___________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Cómo Ejecutarlo Localmente

<mark>**Importante:**</mark> este repositorio está diseñado para la configuración local de Docker/Spark incluida. Inicia los contenedores y verifica las rutas montadas y el archivo JAR del tiempo de ejecución de Iceberg utilizando la infraestructura y los archivos de referencia de comandos proporcionados antes de enviar las solicitudes.

1. **Requisitos Previos**

- Docker Desktop con Docker Compose.

- Los archivos de infraestructura del proyecto y los contenedores de Spark necesarios en ejecución

- Apache Spark 3.5.1

- El tiempo de ejecución de Apache Iceberg 1.9.2 disponible dentro de los contenedores de Spark

- Los directorios montados de la aplicación y de datos utilizados por los comandos incluidos

La ruta configurada del archivo JAR del tiempo de ejecución de Iceberg en el contenedor es:

            /opt/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

Los comandos de Spark configuran las extensiones de Iceberg para Spark SQL, un catálogo de Spark respaldado por Hadoop llamado «local», el almacén del proyecto y la zona horaria de la sesión en UTC. Mantenga estos ajustes consistentes en las tres aplicaciones.

**2. Configuración**

Clona el repositorio y navega a la carpeta raíz:

bash:

    git clone https://github.com/AllGoHer/weather_ledger.git
    cd weather_ledger


**3. Iniciar el Clúster de Spark**

Construye e inicia los contenedores Docker (Spark Master y Worker):

bash:

        docker compose up -d --build


Verifica que el clúster esté funcionando visitando:

- Interfaz de Spark Master: http://localhost:8080

- Interfaz de Spark Worker: http://localhost:8081


**4. Orden de ejecución recomendado**

1. Inicia la infraestructura utilizando la configuración de Docker/Spark proporcionada.

2. Genera los archivos de entrada sintéticos con generate_demo_data.py.

3. Carga el lote de referencia ejecutando bronze_ingest.py.

4. Crea el estado actual inicial ejecutando silver_upsert.py.

5. Crea el resumen diario inicial ejecutando gold_analytics.py.

6. Usa inspection_queries.sql para revisar la singularidad de Bronce y Plata, así como el resumen actual. Guarda el ID de la instantánea actual de Plata antes de procesar los cambios.

7. Incorpora el lote de cambios ejecutando bronze_ingest.py nuevamente.

8. Aplica la corrección y el evento atrasado ejecutando silver_upsert.py nuevamente; revisa la resolución de entregas duplicadas.

9. Consulte la instantánea de Silver guardada antes del cambio utilizando el viaje en el tiempo de Iceberg.

10. Actualice la tabla Gold ejecutando gold_analytics.py nuevamente y compare las métricas diarias afectadas.

Utilice la referencia de comandos proporcionada para conocer las invocaciones exactas de spark-submit en PowerShell y las rutas de los archivos montados. Ejecutar bronze_ingest.py, silver_upsert.py y gold_analytics.py fuera de orden no reproducirá los puntos de control previstos.


**5. Cargar el archivo Jar**

Powershell:

            Invoke-WebRequest `
              -Uri "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar" `
              -OutFile ".\jars\iceberg-spark-runtime-3.5_2.12-1.9.2.jar"


**6. Ejecutar la aplicación**

Powershell:

            cd apps
            
            docker exec iceberg-spark-master /opt/spark/bin/spark-submit `
              --master spark://spark-master:7077 `
              --jars /opt/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar `
              --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
              --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog `
              --conf spark.sql.catalog.local.type=hadoop `
              --conf spark.sql.catalog.local.warehouse=/opt/iceberg/warehouse/weatherledger_project `
              --conf spark.sql.defaultCatalog=local `
              --conf spark.sql.session.timeZone=UTC `
              /opt/spark-apps/weatherledger_project/<<app_name>>.py


**7. Abrir el trabajador de Spark**

en otra terminal y en la raíz del proyecto.

Powershell:

            docker exec -it iceberg-spark-worker bash


**8. Abrir sesión Spark**

Para hacer consultas y verificaciones.

Powershell:

/opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --jars /opt/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/opt/iceberg/warehouse/weatherledger_project

___________________________________________________________________________________________________________________________________________________________________________________________________________________

## ✅ Resultados de la validación

Se verificaron los siguientes resultados en la ejecución local completada utilizando datos sintéticos para 2024.

**Recuento de filas**

| Punto de control | Filas esperadas / observadas |
|------------------|------------------------------|
| Eventos de referencia (Bronce) | 70 272 |
| Observaciones actuales (Plata) después de la referencia | 70 272 |
| Resumen diario (Oro) después de la referencia | 2 928 |
| Bronce después del lote de cambios de cuatro eventos | 70 276 |
| Plata después de aplicar el lote de cambios | 70 273 |
| Oro después de la actualización | 2 928 |

¿Por qué Plata termina con 70 273 filas en lugar de 70 276? Bronce conserva cada evento entrante. Plata mantiene la fila actual por clave de negocio: la corrección actualiza una clave existente, las entregas duplicadas se agrupan en una sola clave actual y la observación tardía introduce una nueva clave.

El nivel Oro se mantiene en 2,928 filas porque tiene una fila para cada una de las 8 estaciones × 366 fechas del calendario en el año bisiesto 2024. La lectura que llega tarde modifica un agregado existente de estación/día en lugar de crear uno nuevo de estación/día.


**Escenarios comprobados en la ejecución**

| Escenario | Antes | Después | Lo que demuestra |
|-----------|-------|---------|------------------|
| Corrección ST-001 | 26,52 °C, revisión 1 | 29,02 °C, revisión 2 | Silver actualiza el registro actual cuando llega una revisión más reciente |
| Entregas duplicadas ST-002 | Registro de referencia más dos entregas adicionales en Bronze | Una fila actual en Silver para esa clave | Bronze conserva el historial de entregas; Silver resuelve los duplicados |
| ST-003: observación tardía | 24 observaciones por estación/día | 25 observaciones | Un evento tardío se incorpora al agregado diario histórico correcto |
| Viaje en el tiempo de Silver | Valor corregido actual | Una instantánea anterior devuelve 26,52 °C | Se puede consultar un estado anterior de la tabla que ya se haya confirmado |

✅ Estos resultados validan los escenarios demostrados. No constituyen una afirmación de que el proyecto esté implementado como un servicio de producción ni conectado a un proveedor de información meteorológica en tiempo real.



