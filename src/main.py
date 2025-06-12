from data_processor import (
    process_product_dimensions,
    process_skus,
    process_categories_from_skus,
    process_point_of_sales,
    process_pdv_dimensions,
    process_employees,
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

    pdv_data = process_point_of_sales()
    save_to_excel(pdv_data, 'involves_pdv')
    
    pdv_dims = process_pdv_dimensions()
    save_to_excel(pdv_dims.get("macroregionals"), 'involves_macroregionais')
    save_to_excel(pdv_dims.get("regionals"), 'involves_regionais')
    save_to_excel(pdv_dims.get("chains"), 'involves_redes')
    save_to_excel(pdv_dims.get("banners"), 'involves_banners')
    save_to_excel(pdv_dims.get("pos_types"), 'involves_tipos_pdv')
    save_to_excel(pdv_dims.get("pos_profiles"), 'involves_perfis_pdv')
    save_to_excel(pdv_dims.get("channels"), 'involves_canais')

    colaboradores_data = process_employees()
    save_to_excel(colaboradores_data, 'involves_colaboradores')
    
    afastamentos_data = process_leaves()
    save_to_excel(afastamentos_data, 'involves_afastamentos')
    
    visitas_data = process_scheduled_visits(colaboradores_data)
    save_to_excel(visitas_data, 'involves_visitas_agendadas')

    print("\n--- PROCESSO DE ETL CONCLUÍDO ---")


if __name__ == "__main__":
    run_etl()
