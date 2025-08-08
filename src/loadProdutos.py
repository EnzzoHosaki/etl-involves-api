# loadProdutos.py
from core.data_processor import (
    process_product_dimensions,
    process_skus,
    process_categories_from_skus
)
from core.bq_handler import save_to_bigquery

def load_produtos():
    """
    Executa o ETL completo para todas as entidades relacionadas a Produtos.
    """
    print("--- INICIANDO CARGA DE PRODUTOS ---")

    # Processa e carrega dimensões de produto
    product_dims = process_product_dimensions()
    save_to_bigquery(product_dims.get("brands"), 'INVOLVES_MARCAS')
    save_to_bigquery(product_dims.get("supercategories"), 'INVOLVES_SUPERCATEGORIAS')
    save_to_bigquery(product_dims.get("productlines"), 'INVOLVES_LINHAS_DE_PRODUTO')
    
    # Processa e carrega fato de produtos (SKUs)
    produtos_data = process_skus()
    save_to_bigquery(produtos_data, 'INVOLVES_PRODUTOS')

    # Processa e carrega dimensão de categorias (derivada dos produtos)
    categorias_data = process_categories_from_skus(produtos_data)
    save_to_bigquery(categorias_data, 'INVOLVES_CATEGORIAS')
    
    print("\n--- CARGA DE PRODUTOS CONCLUÍDA ---")

if __name__ == "__main__":
    load_produtos()