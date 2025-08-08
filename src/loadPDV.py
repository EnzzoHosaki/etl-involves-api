# loadPDV.py
from core.data_processor import (
    process_point_of_sales,
    process_pdv_dimensions
)
from core.bq_handler import save_to_bigquery

def load_pdv():
    """
    Executa o ETL completo para todas as entidades relacionadas a Pontos de Venda (PDV).
    """
    print("--- INICIANDO ETL DE PONTOS DE VENDA (PDV) ---")

    # Processa e carrega fato de Pontos de Venda (PDVs)
    pdv_data = process_point_of_sales()
    save_to_bigquery(pdv_data, 'INVOLVES_PDV')
    
    # Processa e carrega dimensões de PDV
    pdv_dims = process_pdv_dimensions(pdv_data)
    save_to_bigquery(pdv_dims.get("macroregionals"), 'INVOLVES_MACRORREGIONAIS')
    save_to_bigquery(pdv_dims.get("regionals"), 'INVOLVES_REGIONAIS')
    save_to_bigquery(pdv_dims.get("chains"), 'INVOLVES_REDES')
    save_to_bigquery(pdv_dims.get("banners"), 'INVOLVES_BANNERS')
    save_to_bigquery(pdv_dims.get("pos_types"), 'INVOLVES_TIPOS_PDV')
    save_to_bigquery(pdv_dims.get("pos_profiles"), 'INVOLVES_PERFIS_PDV')
    save_to_bigquery(pdv_dims.get("channels"), 'INVOLVES_CANAIS')

    print("\n--- ETL DE PONTOS DE VENDA (PDV) CONCLUÍDO ---")

if __name__ == "__main__":
    load_pdv()
