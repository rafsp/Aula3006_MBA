from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from cosmos import (DbtTaskGroup, ProjectConfig,
                    ProfileConfig, ExecutionConfig)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
 
DBT_PROJECT_DIR = '/usr/local/airflow/dags/dbt/olist'
DBT_EXECUTABLE  = '/usr/local/airflow/dbt_venv/bin/dbt'
 
profile_config = ProfileConfig(
    profile_name='olist',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_default',
        profile_args={'database': 'OLIST_LAB', 'schema': 'STAGING'},
    ),
)
 
with DAG(
    dag_id='pipeline_olist_completo',
    start_date=datetime(2025, 1, 1),
    schedule=None,            # depois trocamos por '@daily'
    catchup=False,
    tags=['fiap', 'olist', 'dbt'],
) as dag:
 
    @task(task_id='extract_and_load_bronze')
    def extract_and_load_bronze():
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
 
        BASE = ('https://raw.githubusercontent.com/'
                'rafsp/Aula2603_MBA/main/datasets/')
        arquivos = {
            'ORDERS':      'olist_orders_dataset.csv',
            'ORDER_ITEMS': 'olist_order_items_dataset.csv',
            'CUSTOMERS':   'olist_customers_dataset.csv',
            'PRODUCTS':    'olist_products_dataset.csv',
            'SELLERS':     'olist_sellers_dataset.csv',
            'PAYMENTS':    'olist_order_payments_dataset.csv',
            'REVIEWS':     'olist_order_reviews_dataset.csv',
        }
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute('CREATE DATABASE IF NOT EXISTS OLIST_LAB')
        cur.execute('CREATE SCHEMA IF NOT EXISTS OLIST_LAB.BRONZE')
        for tabela, nome in arquivos.items():
            df = pd.read_csv(BASE + nome)
            df.columns = [c.upper() for c in df.columns]
            write_pandas(conn, df, tabela, auto_create_table=True,
                         overwrite=True, database='OLIST_LAB',
                         schema='BRONZE')
            print(f'{tabela}: {len(df)} linhas')
        cur.close(); conn.close()
 
    transform = DbtTaskGroup(
        group_id='dbt_transform',
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE),
        operator_args={'install_deps': False},
    )
 
    @task(task_id='quality_checks')
    def quality_checks():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        total = hook.get_first(
            'SELECT COUNT(*) FROM OLIST_LAB.GOLD.FCT_RECEITA_ESTADO')[0]
        if total == 0:
            raise ValueError('Gold vazio — o pipeline falhou')
        print(f'OK - {total} estados na camada Gold')
 
    extract_and_load_bronze() >> transform >> quality_checks()
