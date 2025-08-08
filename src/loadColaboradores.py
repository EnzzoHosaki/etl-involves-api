# loadColaboradores.py
from core.data_processor import (
    process_employees,
    process_supervisors,
    process_leaves
)
from core.bq_handler import save_to_bigquery

def load_colaboradores():
    """
    Executa o ETL completo para todas as entidades relacionadas a Colaboradores.
    """
    print("--- INICIANDO ETL DE COLABORADORES ---")

    # Processa e carrega fato de Colaboradores
    colaboradores_data = process_employees()
    save_to_bigquery(colaboradores_data, 'INVOLVES_COLABORADORES')
    
    # Processa e carrega dimensão de Supervisores
    supervisores_data = process_supervisors(colaboradores_data)
    save_to_bigquery(supervisores_data, 'INVOLVES_SUPERVISORES')

    # Processa e carrega fato de Afastamentos
    afastamentos_data = process_leaves()
    save_to_bigquery(afastamentos_data, 'INVOLVES_AFASTAMENTOS')

    print("\n--- ETL DE COLABORADORES CONCLUÍDO ---")

if __name__ == "__main__":
    load_colaboradores()
