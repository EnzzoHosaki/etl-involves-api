from data_processor import (
    process_product_dimensions, process_skus, process_categories_from_skus,
    process_point_of_sales, process_pdv_dimensions, process_employees,
    process_supervisors, process_leaves, process_scheduled_visits,
    process_surveys_and_answers, process_forms_and_fields,
    process_itineraries_and_noshows
)
from bq_handler import save_to_bigquery, read_bq_column_as_set

def run_etl():
    print("--- INICIANDO PROCESSO DE ETL INVOLVES -> BIGQUERY ---")

    print("\n--- EXECUTANDO CARGA COMPLETA (FULL) ---")
    
    product_dims = process_product_dimensions()
    save_to_bigquery(product_dims.get("brands"), 'INVOLVES_MARCAS')
    save_to_bigquery(product_dims.get("supercategories"), 'INVOLVES_SUPERCATEGORIAS')
    save_to_bigquery(product_dims.get("productlines"), 'INVOLVES_LINHAS_DE_PRODUTO')
    
    produtos_data = process_skus()
    save_to_bigquery(produtos_data, 'INVOLVES_PRODUTOS')

    categorias_data = process_categories_from_skus(produtos_data)
    save_to_bigquery(categorias_data, 'INVOLVES_CATEGORIAS')

    pdv_data = process_point_of_sales()
    save_to_bigquery(pdv_data, 'INVOLVES_PDV')
    
    pdv_dims = process_pdv_dimensions(pdv_data)
    save_to_bigquery(pdv_dims.get("macroregionals"), 'INVOLVES_MACRORREGIONAIS')
    save_to_bigquery(pdv_dims.get("regionals"), 'INVOLVES_REGIONAIS')
    save_to_bigquery(pdv_dims.get("chains"), 'INVOLVES_REDES')
    save_to_bigquery(pdv_dims.get("banners"), 'INVOLVES_BANNERS')
    save_to_bigquery(pdv_dims.get("pos_types"), 'INVOLVES_TIPOS_PDV')
    save_to_bigquery(pdv_dims.get("pos_profiles"), 'INVOLVES_PERFIS_PDV')
    save_to_bigquery(pdv_dims.get("channels"), 'INVOLVES_CANAIS')

    colaboradores_data = process_employees()
    save_to_bigquery(colaboradores_data, 'INVOLVES_COLABORADORES')
    
    supervisores_data = process_supervisors(colaboradores_data)
    save_to_bigquery(supervisores_data, 'INVOLVES_SUPERVISORES')

    print("\n--- EXECUTANDO CARGAS INCREMENTAIS E TRANSACIONAIS ---")

    existing_survey_ids = read_bq_column_as_set('INVOLVES_PESQUISAS', 'IDPESQUISA')
    survey_data = process_surveys_and_answers(existing_survey_ids)
    save_to_bigquery(survey_data.get("new_surveys"), "INVOLVES_PESQUISAS", write_disposition='WRITE_APPEND')
    save_to_bigquery(survey_data.get("new_answers"), "INVOLVES_RESPOSTAS", write_disposition='WRITE_APPEND')

    existing_form_ids = read_bq_column_as_set('INVOLVES_FORMULARIOS', 'IDFORMULARIO')
    new_form_ids_to_fetch = survey_data.get("new_form_ids") - existing_form_ids
    form_data = process_forms_and_fields(new_form_ids_to_fetch)
    save_to_bigquery(form_data.get("forms"), "INVOLVES_FORMULARIOS", write_disposition='WRITE_APPEND')
    save_to_bigquery(form_data.get("form_fields"), "INVOLVES_FORMULARIOS_CAMPOS", write_disposition='WRITE_APPEND')

    afastamentos_data = process_leaves()
    save_to_bigquery(afastamentos_data, 'INVOLVES_AFASTAMENTOS')
    
    visitas_data = process_scheduled_visits(colaboradores_data)
    save_to_bigquery(visitas_data, 'INVOLVES_VISITAS_AGENDADAS')
    
    itinerary_data = process_itineraries_and_noshows()
    save_to_bigquery(itinerary_data.get("itineraries"), 'INVOLVES_ROTEIROS')
    save_to_bigquery(itinerary_data.get("noshows"), 'INVOLVES_JUSTIFICATIVAS_FALTA')

    print("\n--- PROCESSO DE ETL PARA O BIGQUERY CONCLUÍDO ---")

if __name__ == "__main__":
    run_etl()