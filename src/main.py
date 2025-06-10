from data_processor import (
    process_brands,
    process_categories,
    process_supercategories,
    process_productlines,
    process_skus,
    process_point_of_sales
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
    
    produtos_data = process_skus()
    save_to_excel(produtos_data, 'involves_produtos')

    pdv_data = process_point_of_sales()
    save_to_excel(pdv_data, 'involves_pdv')

    print("\n--- PROCESSO DE ETL CONCLUÍDO ---")


if __name__ == "__main__":
    run_etl()