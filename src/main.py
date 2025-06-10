from data_processor import (
    process_brands,
    process_categories,
    process_supercategories,
    process_productlines,
    process_skus,
    process_point_of_sales,
    process_macroregionals,
    process_regionals,
    process_banners,
    process_chains,
    process_pos_types,
    process_pos_profiles,
    process_channels
)
from file_handler import save_to_excel

def run_etl():
    print("--- INICIANDO PROCESSO DE ETL INVOLVES ---")

    marcas_data = process_brands()
    save_to_excel(marcas_data, 'involves_marcas')

    categorias_data = process_categories()
    save_to_excel(categorias_data, 'involves_categorias')

    supercategorias_data = process_supercategories()
    save_to_excel(supercategorias_data, 'involves_supercategorias')

    linhas_produto_data = process_productlines()
    save_to_excel(linhas_produto_data, 'involves_linhas_de_produto')
    
    macroregionais_data = process_macroregionals()
    save_to_excel(macroregionais_data, 'involves_macroregionais')

    regionais_data = process_regionals()
    save_to_excel(regionais_data, 'involves_regionais')

    chains_data = process_chains()
    save_to_excel(chains_data, 'involves_redes')
    
    banners_data = process_banners()
    save_to_excel(banners_data, 'involves_banners')
    
    tipos_pdv_data = process_pos_types()
    save_to_excel(tipos_pdv_data, 'involves_tipos_pdv')
    
    perfis_pdv_data = process_pos_profiles()
    save_to_excel(perfis_pdv_data, 'involves_perfis_pdv')
    
    canais_data = process_channels()
    save_to_excel(canais_data, 'involves_canais')

    produtos_data = process_skus()
    save_to_excel(produtos_data, 'involves_produtos')

    pdv_data = process_point_of_sales()
    save_to_excel(pdv_data, 'involves_pdv')

    print("\n--- PROCESSO DE ETL CONCLUÍDO ---")


if __name__ == "__main__":
    run_etl()