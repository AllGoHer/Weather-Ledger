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

📌 El objetivo no es simplemente mover un archivo CSV a través de tres carpetas. Es demostrar cómo un formato de tabla como Apache Iceberg puede respaldar un flujo de trabajo de <mark>lakehouse</mark> en el que los datos cambian con el tiempo.




    
  
