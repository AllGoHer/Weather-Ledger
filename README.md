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

%%{init: {'theme': 'dark', 'themeVariables': { 'fontFamily': 'monospace', 'lineColor': '#00ffff'}}}%%
flowchart TD
    %% Estilos Neón (Cyberpunk)
    classDef neonBlue fill:#000000,stroke:#00ffff,stroke-width:2px,color:#00ffff,box-shadow: 0 0 10px #00ffff;
    classDef neonOrange fill:#000000,stroke:#ff9900,stroke-width:2px,color:#ff9900,box-shadow: 0 0 10px #ff9900;
    classDef neonGreen fill:#000000,stroke:#00ff00,stroke-width:2px,color:#00ff00,box-shadow: 0 0 10px #00ff00;
    classDef neonYellow fill:#000000,stroke:#ffff00,stroke-width:2px,color:#ffff00,box-shadow: 0 0 10px #ffff00;
    classDef neonPurple fill:#000000,stroke:#ff00ff,stroke-width:2px,color:#ff00ff,box-shadow: 0 0 10px #ff00ff;
    classDef neonRed fill:#000000,stroke:#ff0000,stroke-width:2px,color:#ff0000,box-shadow: 0 0 10px #ff0000;
    
    %% Estilos de línea de flujo
    linkStyle default stroke:#00ffff,stroke-width:2px,fill:none,filter: drop-shadow(0 0 5px #00ffff);

    %% --- ENTORNO / INGESTA ---
    subgraph Ingestion ["⚡ INGESTA"]
        A(["🤖 Synthetic Weather Data\nGenerator"]):::neonBlue
        B[("📁 Landing CSV Files\nBaseline + Change Batch")]:::neonBlue
    end

    %% --- ENTORNO / CAPAS MEDALLION ---
    subgraph Medallion ["🧱 ARQUITECTURA MEDALLION (Apache Iceberg)"]
        C[("🥉 Bronze Iceberg Table\nAppend-only event history")]:::neonOrange
        D[("🥈 Silver Iceberg Table\nDeduplicate + MERGE INTO")]:::neonGreen
        E[("🥇 Gold Iceberg Table\nDaily station metrics")]:::neonYellow
    end

    %% --- ENTORNO / PRESENTACIÓN ---
    subgraph Presentation ["📊 PRESENTACIÓN"]
        F[("📄 Daily Summary CSV")]:::neonYellow
        G{{"📈 Power BI\nDashboard"}}:::neonRed
    end

    %% --- ENTORNO / AUDITORÍA ---
    subgraph Audit ["⏱️ AUDITORÍA & VERSIONADO"]
        H[("🕰️ Earlier Silver\nTable State")]:::neonPurple
    end

    %% --- CONEXIONES PRINCIPALES ---
    A --> B
    B -->|"Ingesta Batch"| C
    C -->|"Transformación"| D
    D -->|"Agregación"| E
    E -->|"Export"| F
    F -->|"Carga de Datos"| G

    %% --- CONEXIÓN DE TIME TRAVEL (Punteada) ---
    D -.->|"Snapshots + time travel"| H
    
    %% Estilos de conexión específicos (opcional)
    linkStyle 4 stroke:#ffff00,stroke-width:2px,stroke-dasharray: 5 5,filter: drop-shadow(0 0 5px #ffff00);





___________________________________________________________________________________________________________________________________________________________________________________________________________________


    
  
