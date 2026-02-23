from airflow import DAG
from pendulum import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig


DBT_PROJECT_PATH = "/opt/airflow/dbt/retail_project" # Où est le code SQL
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt" # Où est le moteur dbt 
PROFILES_YML_PATH = "/opt/airflow/dbt_home/.dbt/profiles.yml" # Où est le mot de passe Snowflake 

# 2. On configure la connexion (Cosmos lit le fichier profiles.yml)
profile_config = ProfileConfig(
    profile_name="retail_project", # Le nom du projet dans le dbt_project.yml
    target_name="dev",             # L'environnement cible
    profiles_yml_filepath=PROFILES_YML_PATH
)

# 3. On crée la routine (Le DAG)
with DAG(
    dag_id="mon_premier_pipeline_dbt",
    start_date=datetime(2024, 1, 1, tz="UTC"), # Date fictive de début
    schedule="10 2 * * *",                     # Tous les jours à 02:10 du matin
    catchup=False,                             # Ne pas rattraper le temps perdu si on l'éteint
    max_active_runs=1,                   
    tags=["retail", "dbt", "snowflake"],
) as dag:

    # 4. On crée le groupe de tâches dbt (Cosmos va traduire les fichiers SQL en tâches Airflow)
    transformation_dbt = DbtTaskGroup(
        group_id="transformation_dbt",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
    )

    # Ordre d'exécution 
    transformation_dbt