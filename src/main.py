from data_processor import (
    process_brands,
    process_categories,
    process_supercategories,
    process_productlines,
    process_skus
)
from file_handler import save_to_excel

def run_etl():
    print("--- INICIANDO PROCESSO DE ETL INVOLVES ---")

    # Passo 1: Marcas
    marcas_data = process_brands()
    save_to_excel(marcas_data, 'involves_marcas')

    # Passo 2: Categorias
    categorias_data = process_categories()
    save_to_excel(categorias_data, 'involves_categorias')

    # Passo 3: Supercategorias
    supercategorias_data = process_supercategories()
    save_to_excel(supercategorias_data, 'involves_supercategorias')

    # Passo 4: Linhas de Produto
    linhas_produto_data = process_productlines()
    save_to_excel(linhas_produto_data, 'involves_linhas_de_produto')
    
    # Passo 5: Produtos (SKUs)
    produtos_data = process_skus()
    save_to_excel(produtos_data, 'involves_produtos')

    print("\n--- PROCESSO DE ETL CONCLUÍDO ---")


if __name__ == "__main__":
    run_etl()