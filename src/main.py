from data_processor import (
    process_product_dimensions,
    process_skus,
    process_categories_from_skus,
    process_all_pdv_related_data,
    process_employees,
    process_supervisors,
    process_leaves,
    process_scheduled_visits
)
from file_handler import save_to_excel

def run_etl():
    print("--- INICIANDO PROCESSO DE ETL INVOLVES ---")

    product_dims = process_product_dimensions()
    save_to_excel(product_dims.get("brands"), 'involves_marcas')
    save_to_excel(product_dims.get("supercategories"), 'involves_supercategorias')
    save_to_excel(product_dims.get("productlines"), 'involves_linhas_de_produto')
    
    produtos_data = process_skus()
    save_to_excel(produtos_data, 'involves_produtos')

    categorias_data = process_categories_from_skus(produtos_data)
    save_to_excel(categorias_data, 'involves_categorias')

    pdv_datasets = process_all_pdv_related_data()
    save_to_excel(pdv_datasets.get("pdvs"), 'involves_pdv')
    save_to_excel(pdv_datasets.get("macroregionals"), 'involves_macroregionais')
    save_to_excel(pdv_datasets.get("regionals"), 'involves_regionais')
    save_to_excel(pdv_datasets.get("chains"), 'involves_redes')
    save_to_excel(pdv_datasets.get("banners"), 'involves_banners')
    save_to_excel(pdv_datasets.get("pos_types"), 'involves_tipos_pdv')
    save_to_excel(pdv_datasets.get("pos_profiles"), 'involves_perfis_pdv')
    save_to_excel(pdv_datasets.get("channels"), 'involves_canais')

    colaboradores_data = process_employees()
    save_to_excel(colaboradores_data, 'involves_colaboradores')
    
    supervisores_data = process_supervisors(colaboradores_data)
    save_to_excel(supervisores_data, 'involves_supervisores')
    
    afastamentos_data = process_leaves()
    save_to_excel(afastamentos_data, 'involves_afastamentos')
    
    visitas_data = process_scheduled_visits(colaboradores_data)
    save_to_excel(visitas_data, 'involves_visitas_agendadas')

    print("\n--- PROCESSO DE ETL CONCLUÍDO ---")


if __name__ == "__main__":
    run_etl()
