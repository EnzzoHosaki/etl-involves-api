# data_processor.py
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import INVOLVES_BASE_URL, INVOLVES_ENVIRONMENT_ID
from api_client import get_api_data

# --- Módulo de Extração Genérica ---

def _fetch_paginated_summaries(endpoint_name: str) -> list:
    """Busca os resumos de todos os itens de um endpoint paginado."""
    all_items = []
    page_num = 1
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/{endpoint_name}"
    
    print(f"\n--- Iniciando extração de resumos para: '{endpoint_name}' ---")
    while True:
        paginated_url = f"{base_url}?page={page_num}&size=100"
        response_data = get_api_data(paginated_url)
        if not response_data or not isinstance(response_data, dict): break
        
        items_on_page = response_data.get('items', [])
        if not items_on_page: break

        all_items.extend(items_on_page)
        print(f"  > Página {page_num} processada. Total de {len(all_items)} itens acumulados.")
        page_num += 1
        time.sleep(0.2)
    
    print(f"Extração de resumos de '{endpoint_name}' finalizada. Total de {len(all_items)} itens encontrados.")
    return all_items

def _fetch_details_in_parallel(url_template: str, ids: set) -> list:
    """Busca detalhes para um conjunto de IDs em paralelo usando uma URL template."""
    if not ids: return []
    
    processed_details = []
    MAX_WORKERS = 5
    total_ids = len(ids)
    endpoint_name = url_template.split('/')[-2]

    def fetch_single_detail(item_id):
        detail_url = url_template.format(id=item_id)
        return get_api_data(detail_url)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(fetch_single_detail, item_id): item_id for item_id in ids}
        for i, future in enumerate(as_completed(future_to_id)):
            try:
                result = future.result()
                if result: processed_details.append(result)
            except Exception as exc: print(f'Um item com ID atrelado a {future_to_id[future]} gerou uma exceção: {exc}')
            print(f"\r  > Detalhes de '{endpoint_name}' processados: {i + 1}/{total_ids}", end="", flush=True)

    print()
    return processed_details

# --- Módulos de Processamento Específico ---

def process_product_dimensions():
    """Extrai e processa todas as dimensões de produto que possuem listas públicas."""
    print("\n--- INICIANDO EXTRAÇÃO DE DIMENSÕES DE PRODUTO ---")
    
    brands = [{'ID': b.get('id'), 'NOME': b.get('name')} for b in _fetch_paginated_summaries('brands') if isinstance(b, dict)]
    supercategories = [{'ID': sc.get('id'), 'NOME': sc.get('name')} for sc in _fetch_paginated_summaries('supercategories') if isinstance(sc, dict)]
    productlines = [{'ID': pl.get('id'), 'NOME': pl.get('name')} for pl in _fetch_paginated_summaries('productlines') if isinstance(pl, dict)]
    
    return {
        "brands": brands,
        "supercategories": supercategories,
        "productlines": productlines
    }

def process_skus() -> list:
    """Busca e formata os dados de produtos (SKUs)."""
    raw_data = _fetch_paginated_summaries('skus')
    print("\n--- Processando dataset de Produtos para o formato final ---")
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None
    for sku in raw_data:
        if not isinstance(sku, dict): continue
        custom_fields_list = sku.get('customFields', [])
        row = {
            'IDPROD': to_str(sku.get('id')), 'NOMEPROD': sku.get('name'),
            'ISACTIVE': sku.get('active'), 'EAN': to_str(sku.get('barCode')),
            'CODPROD': to_str(sku.get('integrationCode')),
            'IDLINHAPRODUTO': to_str(sku.get('productLine', {}).get('id')),
            'IDMARCA': to_str(sku.get('brand', {}).get('id')),
            'IDCATEGORIA': to_str(sku.get('category', {}).get('id')),
            'IDSUPERCATEGORIA': to_str(sku.get('supercategory', {}).get('id')),
            'CUSTOMFIELDS': json.dumps(custom_fields_list) if custom_fields_list else None
        }
        processed_data.append(row)
    return processed_data

def process_categories_from_skus(all_skus: list) -> list:
    """
    Coleta IDs de categoria da lista de SKUs e busca seus detalhes.
    """
    print("\n--- INICIANDO EXTRAÇÃO DE DIMENSÃO DE CATEGORIAS (VIA SKUS) ---")
    if not all_skus:
        print("Nenhum SKU encontrado para extrair categorias.")
        return []

    category_ids = {sku.get('IDCATEGORIA') for sku in all_skus if sku.get('IDCATEGORIA')}
    category_ids.discard(None) # Remove qualquer valor nulo que possa ter sido adicionado

    url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/categories/{{id}}"
    category_details = _fetch_details_in_parallel(url_template, category_ids)

    return [{'ID': c.get('id'), 'NOME': c.get('name'), 'IDSUPERCATEGORIA': c.get('supercategory', {}).get('id')} for c in category_details if isinstance(c, dict)]


def process_all_pdv_related_data():
    """
    Orquestra a extração de PDVs e de todas as suas dimensões relacionadas
    de forma otimizada, buscando apenas os IDs necessários.
    """
    print("\n--- INICIANDO EXTRAÇÃO DE PDVS E DIMENSÕES RELACIONADAS ---")
    
    # 1. Busca os detalhes completos de todos os PDVs em paralelo
    pdv_summaries = _fetch_paginated_summaries('pointofsales')
    print("\n--- Processando detalhes de cada Ponto de Venda (utilizando threads) ---")
    url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales/{{id}}"
    pdv_ids = {pdv['id'] for pdv in pdv_summaries if pdv.get('id')}
    pdv_details = _fetch_details_in_parallel(url_template, pdv_ids)

    # 2. Coleta todos os IDs únicos das dimensões a partir dos detalhes dos PDVs
    dimension_ids = {
        'macroregional': set(), 'regional': set(), 'banner': set(),
        'type': set(), 'profile': set(), 'channel': set(), 'chain': set()
    }
    
    for pdv in pdv_details:
        if not isinstance(pdv, dict): continue
        dimension_ids['macroregional'] |= {pdv.get('macroregional', {}).get('id')}
        dimension_ids['regional'] |= {pdv.get('regional', {}).get('id')}
        dimension_ids['banner'] |= {pdv.get('banner', {}).get('id')}
        dimension_ids['type'] |= {pdv.get('type', {}).get('id')}
        dimension_ids['profile'] |= {pdv.get('profile', {}).get('id')}
        dimension_ids['channel'] |= {pdv.get('channel', {}).get('id')}
    
    for key in dimension_ids:
        dimension_ids[key].discard(None)

    # 3. Busca os detalhes apenas para os IDs únicos coletados
    print("\n--- Buscando detalhes das dimensões de PDV ---")
    macroregionals = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/macroregionals/{{id}}", dimension_ids['macroregional'])
    regionals = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/regionals/{{id}}", dimension_ids['regional'])
    banners = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/banners/{{id}}", dimension_ids['banner'])
    
    dimension_ids['chain'] |= {b.get('chain', {}).get('id') for b in banners if isinstance(b, dict)}
    dimension_ids['chain'].discard(None)
    
    chains = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v3/chains/{{id}}", dimension_ids['chain'])
    
    pos_types = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v1/pointofsaletype/{{id}}", dimension_ids['type'])
    pos_profiles = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/pointofsaleprofile/{{id}}", dimension_ids['profile'])
    channels = _fetch_details_in_parallel(f"{INVOLVES_BASE_URL}/v3/pointofsalechannels/{{id}}", dimension_ids['channel'])

    # 4. Formata todos os datasets para o formato final
    to_str = lambda v: str(v) if v is not None else None
    
    processed_pdvs = [{
        'IDPDV': to_str(pdv.get('id')), 'RAZAOSOCIAL': pdv.get('legalBusinessName'),
        'FANTASIA': pdv.get('tradeName'), 'CODCLI': to_str(pdv.get('code')),
        'CNPJ': pdv.get('companyRegistrationNumber'), 'TELEFONE': pdv.get('phone'),
        'ISACTIVE': pdv.get('active'),
        'IDMACROREGIONAL': to_str(pdv.get('macroregional', {}).get('id')),
        'IDREGIONAL': to_str(pdv.get('regional', {}).get('id')),
        'IDBANNER': to_str(pdv.get('banner', {}).get('id')),
        'IDTIPO': to_str(pdv.get('type', {}).get('id')),
        'IDPERFIL': to_str(pdv.get('profile', {}).get('id')),
        'IDCANAL': to_str(pdv.get('channel', {}).get('id'))
    } for pdv in pdv_details]

    return {
        "pdvs": processed_pdvs,
        "macroregionals": [{'ID': item.get('id'), 'NOME': item.get('name')} for item in macroregionals],
        "regionals": [{'ID': item.get('id'), 'NOME': item.get('name'), 'IDMACROREGIONAL': item.get('macroregional', {}).get('id')} for item in regionals],
        "banners": [{'ID': item.get('id'), 'NOME': item.get('name'), 'IDREDE': item.get('chain', {}).get('id')} for item in banners],
        "chains": [{'ID': item.get('id'), 'NOME': item.get('name'), 'CODIGO': item.get('code')} for item in chains],
        "pos_types": [{'ID': item.get('id'), 'NOME': item.get('name')} for item in pos_types],
        "pos_profiles": [{'ID': item.get('id'), 'NOME': item.get('name')} for item in pos_profiles],
        "channels": [{'ID': item.get('id'), 'NOME': item.get('name')} for item in channels],
    }
