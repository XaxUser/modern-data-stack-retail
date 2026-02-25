# 🚀 Migration & Modernisation Data Platform Retail (Azure + Snowflake)

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Azure](https://img.shields.io/badge/Microsoft_Azure-Serverless-0089D6?logo=microsoft-azure)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Cloud-29B5E8?logo=snowflake)
![dbt](https://img.shields.io/badge/dbt-Analytics_Engineering-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?logo=apache-airflow)

## 📌 Contexte du Projet
Ce projet est une implémentation *End-to-End* d'une **Modern Data Stack (MDS)** orientée Retail. L'objectif est de migrer un flux de données transactionnelles *legacy* (fichiers plats) vers une architecture Cloud Native événementielle (Event-Driven) garantissant scalabilité, observabilité et qualité de la donnée.



## 🏗️ Architecture Technique

1. **Ingestion Serverless :** Une application `Azure Functions v2` (Python) génère continuellement des données de ventes et de stocks, poussées vers une Landing Zone dans **Azure Data Lake Storage (ADLS Gen2)**.
2. **Auto-Ingestion (Event-Driven) :** Intégration d'**Azure Event Grid** avec **Snowpipe** pour ingérer de manière asynchrone et quasi temps-réel les nouveaux fichiers vers les tables `RAW` de Snowflake.
3. **Transformation & Modélisation :** Utilisation de **dbt Core** pour modéliser la donnée en *Architecture Médaillon* (Bronze, Silver, Gold). Application de tests de Data Quality et documentation du lignage (Data Lineage).
4. **Orchestration :** **Apache Airflow** conteneurisé (Docker) déployé sur une VM Azure (Linux Ubuntu). Configuration d'ordonnancement CRON bi-quotidien et gestion des alertes d'échec.

## 📂 Structure du Référentiel

```text
📦 modern-data-stack-retail
 ┣ 📂 dags                  # DAGs Apache Airflow (Orchestration)
 ┣ 📂 dbt                   # Projet dbt (Modèles SQL, tests, yaml)
 ┣ 📂 generateur_cloud      # Code source de l'Azure Function (Ingestion)
 ┣ 📜 docker-compose.yaml   # Stack Airflow + PostgreSQL
 ┣ 📜 .github/workflows     # Pipelines CI/CD (DataOps)
 ┗ 📜 README.md