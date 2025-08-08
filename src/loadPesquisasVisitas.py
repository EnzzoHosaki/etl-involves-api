# loadPesquisasVisitas.py
from core.data_processor import (
    process_employees,
    process_scheduled_visits,
    process_surveys_and_answers,
    process_forms_and_fields,
    process_itineraries_and_noshows
)
from core.bq_handler import save_to_bigquery, read_bq_column_as_set

def load_pesquisas_visitas():
    """
    Executa o ETL para as entidades transacionais e incrementais.
    """
    print("--- INICIANDO ETL DE PESQUISAS E VISITAS ---")

    # Processa e carrega incrementalmente Pesquisas e Respostas
    existing_survey_ids = read_bq_column_as_set('INVOLVES_PESQUISAS', 'IDPESQUISA')
    survey_data = process_surveys_and_answers(existing_survey_ids)
    save_to_bigquery(survey_data.get("new_surveys"), "INVOLVES_PESQUISAS", write_disposition='WRITE_APPEND')
    save_to_bigquery(survey_data.get("new_answers"), "INVOLVES_RESPOSTAS", write_disposition='WRITE_APPEND')

    # Processa e carrega incrementalmente Formulários e Campos
    existing_form_ids = read_bq_column_as_set('INVOLVES_FORMULARIOS', 'IDFORMULARIO')
    new_form_ids_to_fetch = survey_data.get("new_form_ids") - existing_form_ids
    form_data = process_forms_and_fields(new_form_ids_to_fetch)
    save_to_bigquery(form_data.get("forms"), "INVOLVES_FORMULARIOS", write_disposition='WRITE_APPEND')
    save_to_bigquery(form_data.get("form_fields"), "INVOLVES_FORMULARIOS_CAMPOS", write_disposition='WRITE_APPEND')
    
    # Para buscar visitas, primeiro precisamos da lista de colaboradores
    print("\nBuscando lista de colaboradores para processar visitas...")
    colaboradores_data = process_employees()
    
    # Processa e carrega Visitas Agendadas
    visitas_data = process_scheduled_visits(colaboradores_data)
    save_to_bigquery(visitas_data, 'INVOLVES_VISITAS_AGENDADAS')
    
    # Processa e carrega Roteiros e Justificativas de Falta
    itinerary_data = process_itineraries_and_noshows()
    save_to_bigquery(itinerary_data.get("itineraries"), 'INVOLVES_ROTEIROS')
    save_to_bigquery(itinerary_data.get("noshows"), 'INVOLVES_JUSTIFICATIVAS_FALTA')

    print("\n--- ETL DE PESQUISAS E VISITAS CONCLUÍDO ---")

if __name__ == "__main__":
    load_pesquisas_visitas()
