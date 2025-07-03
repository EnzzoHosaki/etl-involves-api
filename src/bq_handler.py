import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config import BIGQUERY_DATASET

def save_to_bigquery(data: list, table_id: str, write_disposition: str = 'WRITE_TRUNCATE'):
    if not data:
        print(f"Nenhum dado fornecido para a tabela do BigQuery: '{table_id}'.")
        return

    df = pd.DataFrame(data)
    
    for col in df.columns:
        if 'ID' in col.upper():
            df[col] = df[col].astype(str).replace(r'\.0$', '', regex=True)
        
        if df[col].dtype == 'bool':
            df[col] = df[col].astype('boolean')
            
        if 'DATA' in col.upper():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    client = bigquery.Client()
    full_table_id = f"{client.project}.{BIGQUERY_DATASET}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True, 
    )

    try:
        print(f"Carregando {len(df)} registros para a tabela '{full_table_id}'...")
        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result() 
        print(f"✅ Sucesso! Dados carregados em '{full_table_id}'.")
    except Exception as e:
        print(f"❌ Erro ao carregar dados para a tabela '{full_table_id}': {e}")


def read_bq_column_as_set(table_id: str, column_name: str) -> set:
    client = bigquery.Client()
    full_table_id = f"{BIGQUERY_DATASET}.{table_id}"
    
    try:
        client.get_table(full_table_id)
    except NotFound:
        print(f"[INFO] Tabela '{full_table_id}' não encontrada. Assumindo que é a primeira execução.")
        return set()

    try:
        query = f"SELECT DISTINCT `{column_name}` FROM `{client.project}.{full_table_id}`"
        df = client.query(query).to_dataframe()
        
        if not df.empty:
            return set(df[column_name].dropna().astype(str))
        else:
            return set()
            
    except Exception as e:
        print(f"❌ Erro ao ler a coluna '{column_name}' da tabela '{full_table_id}': {e}")
        return set()