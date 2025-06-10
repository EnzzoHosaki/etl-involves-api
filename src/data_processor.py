import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import INVOLVES_BASE_URL, INVOLVES_ENVIRONMENT_ID
from api_client import get_api_data

def _fetch_all_paginated_data(base_url: str) -> list:
    """Busca todos os dados de um endpoint paginado, a partir de uma URL base."""
    all_items = []
    page_num = 1
    endpoint_name = base_url.split('/')[-1]
    
    print(f"\n--- Iniciando extração para: '{endpoint_name}' ---")

    while True:
        paginated_url = f"{base_url}?page={page_num}&size=100"
        response_data = get_api_data(paginated_url)

        if not response_data:
            break
        
        items_on_page = []
        if isinstance(response_data, list):
            items_on_page = response_data
        elif isinstance(response_data, dict):
            items_on_page = response_data.get('items', [])

        if not items_on_page:
            break

        all_items.extend(items_on_page)
        print(f"  > Página {page_num} processada. Total de {len(all_items)} itens acumulados.")
        page_num += 1
        time.sleep(0.2)
    
    print(f"Extração de '{endpoint_name}' finalizada. Total de {len(all_items)} itens encontrados.")
    return all_items

def process_brands() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/brands"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': b.get('id'), 'NOME': b.get('name')} for b in raw_data if isinstance(b, dict)]

def process_categories() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/categories"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': c.get('id'), 'NOME': c.get('name'), 'IDSUPERCATEGORIA': c.get('supercategory', {}).get('id')} for c in raw_data if isinstance(c, dict)]

def process_supercategories() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/supercategories"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': sc.get('id'), 'NOME': sc.get('name')} for sc in raw_data if isinstance(sc, dict)]

def process_productlines() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/productlines"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': pl.get('id'), 'NOME': pl.get('name')} for pl in raw_data if isinstance(pl, dict)]

def process_macroregionals() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/macroregionals"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': item.get('id'), 'NOME': item.get('name')} for item in raw_data if isinstance(item, dict)]

def process_regionals() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/regionals"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': item.get('id'), 'NOME': item.get('name'), 'IDMACROREGIONAL': item.get('macroregional', {}).get('id')} for item in raw_data if isinstance(item, dict)]

def process_banners() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/banners"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': item.get('id'), 'NOME': item.get('name'), 'IDREDE': item.get('chain', {}).get('id')} for item in raw_data if isinstance(item, dict)]

def process_chains() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/chains"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': item.get('id'), 'NOME': item.get('name'), 'CODIGO': item.get('code')} for item in raw_data if isinstance(item, dict)]

def process_pos_types() -> list:
    url = f"{INVOLVES_BASE_URL}/v1/pointofsaletype/find"
    print(f"\n--- Iniciando extração para: 'tipos de pdv (v1)' ---")
    raw_data = get_api_data(url)
    if not raw_data: return []
    return [{'ID': item.get('id'), 'NOME': item.get('name')} for item in raw_data if isinstance(item, dict)]

def process_pos_profiles() -> list:
    url = f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/pointofsaleprofile/find"
    print(f"\n--- Iniciando extração para: 'perfis de pdv (v1)' ---")
    raw_data = get_api_data(url)
    if not raw_data: return []
    return [{'ID': item.get('id'), 'NOME': item.get('name')} for item in raw_data if isinstance(item, dict)]

def process_channels() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/pointofsalechannels"
    raw_data = _fetch_all_paginated_data(base_url)
    return [{'ID': item.get('id'), 'NOME': item.get('name')} for item in raw_data if isinstance(item, dict)]

def process_skus() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/skus"
    raw_data = _fetch_all_paginated_data(base_url)
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

def process_point_of_sales() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales"
    pdv_summaries = _fetch_all_paginated_data(base_url)
    print("\n--- Processando detalhes de cada Ponto de Venda (utilizando threads) ---")
    processed_data, total_pdvs, MAX_WORKERS = [], len(pdv_summaries), 5
    to_str = lambda v: str(v) if v is not None else None

    def fetch_and_process_detail(pdv_summary):
        if not isinstance(pdv_summary, dict): return None
        pdv_id = pdv_summary.get('id')
        if not pdv_id: return None
        detail_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales/{pdv_id}"
        pdv_detail = get_api_data(detail_url)
        if not pdv_detail: return None
        return {
            'IDPDV': to_str(pdv_detail.get('id')),
            'RAZAOSOCIAL': pdv_detail.get('legalBusinessName'),
            'FANTASIA': pdv_detail.get('tradeName'),
            'CODCLI': to_str(pdv_detail.get('code')),
            'CNPJ': pdv_detail.get('companyRegistrationNumber'),
            'TELEFONE': pdv_detail.get('phone'),
            'ISACTIVE': pdv_detail.get('active'),
            'IDMACROREGIONAL': to_str(pdv_detail.get('macroregional', {}).get('id')),
            'IDREGIONAL': to_str(pdv_detail.get('regional', {}).get('id')),
            'IDBANNER': to_str(pdv_detail.get('banner', {}).get('id')),
            'IDTIPO': to_str(pdv_detail.get('type', {}).get('id')),
            'IDPERFIL': to_str(pdv_detail.get('profile', {}).get('id')),
            'IDCANAL': to_str(pdv_detail.get('channel', {}).get('id'))
        }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_pdv = {executor.submit(fetch_and_process_detail, pdv): pdv for pdv in pdv_summaries}
        for i, future in enumerate(as_completed(future_to_pdv)):
            try:
                result = future.result()
                if result: processed_data.append(result)
            except Exception as exc: print(f'Um PDV gerou uma exceção: {exc}')
            print(f"\r  > PDVs processados: {i + 1}/{total_pdvs}", end="", flush=True)
            
    print("\nProcessamento de detalhes concluído.")
    if processed_data: processed_data.sort(key=lambda x: int(x['IDPDV']))
    return processed_data
