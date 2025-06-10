import time
import json
from config import INVOLVES_BASE_URL, INVOLVES_ENVIRONMENT_ID
from api_client import get_api_data

def _fetch_all_paginated_data(endpoint_name: str) -> list:
    all_items = []
    page_num = 1
    endpoint_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/{endpoint_name}"
    
    print(f"\n--- Iniciando extração para: '{endpoint_name}' ---")

    while True:
        paginated_url = f"{endpoint_url}?page={page_num}&size=100"
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
    raw_data = _fetch_all_paginated_data('brands')
    return [{'ID': b.get('id'), 'NOME': b.get('name')} for b in raw_data if isinstance(b, dict)]

def process_categories() -> list:
    raw_data = _fetch_all_paginated_data('categories')
    return [{'ID': c.get('id'), 'NOME': c.get('name'), 'IDSUPERCATEGORIA': c.get('supercategory', {}).get('id')} for c in raw_data if isinstance(c, dict)]

def process_supercategories() -> list:
    raw_data = _fetch_all_paginated_data('supercategories')
    return [{'ID': sc.get('id'), 'NOME': sc.get('name')} for sc in raw_data if isinstance(sc, dict)]

def process_productlines() -> list:
    raw_data = _fetch_all_paginated_data('productlines')
    return [{'ID': pl.get('id'), 'NOME': pl.get('name')} for pl in raw_data if isinstance(pl, dict)]

def process_skus() -> list:
    raw_data = _fetch_all_paginated_data('skus')
    
    print("\n--- Processando dataset de Produtos para o formato final ---")
    processed_data = []
    
    def to_str(value):
        return str(value) if value is not None else None

    for sku in raw_data:
        if not isinstance(sku, dict):
            continue

        custom_fields_list = sku.get('customFields', [])
        custom_fields_json = json.dumps(custom_fields_list) if custom_fields_list else None

        row = {
            'IDPROD': to_str(sku.get('id')),
            'NOMEPROD': sku.get('name'),
            'ISACTIVE': sku.get('active'),
            'EAN': to_str(sku.get('barCode')),
            'CODPROD': to_str(sku.get('integrationCode')),
            'IDLINHAPRODUTO': to_str(sku.get('productLine', {}).get('id')),
            'IDMARCA': to_str(sku.get('brand', {}).get('id')),
            'IDCATEGORIA': to_str(sku.get('category', {}).get('id')),
            'IDSUPERCATEGORIA': to_str(sku.get('supercategory', {}).get('id')),
            'CUSTOMFIELDS': custom_fields_json
        }
        processed_data.append(row)
        
    return processed_data

def process_point_of_sales() -> list:
    pdv_summaries = _fetch_all_paginated_data('pointofsales')
    
    print("\n--- Processando detalhes de cada Ponto de Venda ---")
    processed_data = []
    total_pdvs = len(pdv_summaries)

    def to_str(value):
        return str(value) if value is not None else None

    for i, pdv_summary in enumerate(pdv_summaries):
        if not isinstance(pdv_summary, dict):
            continue
        
        pdv_id = pdv_summary.get('id')
        if not pdv_id:
            continue
            
        print(f"  > Buscando detalhes do PDV {i+1}/{total_pdvs} (ID: {pdv_id})...")
        
        detail_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales/{pdv_id}"
        pdv_detail = get_api_data(detail_url)

        if not pdv_detail:
            continue

        row = {
            'IDPDV': to_str(pdv_detail.get('id')),
            'RAZAOSOCIAL': pdv_detail.get('legalBusinessName'),
            'FANTASIA': pdv_detail.get('tradeName'),
            'CODCLI': to_str(pdv_detail.get('code')),
            'CNPJ': pdv_detail.get('companyRegistrationNumber'),
            'TELEFONE': pdv_detail.get('phone'), # Novo campo adicionado
            'ISACTIVE': pdv_detail.get('active'),
            'IDMACROREGIONAL': to_str(pdv_detail.get('macroregional').get('id')) if pdv_detail.get('macroregional') else None,
            'IDREGIONAL': to_str(pdv_detail.get('regional').get('id')) if pdv_detail.get('regional') else None,
            'IDBANNER': to_str(pdv_detail.get('banner').get('id')) if pdv_detail.get('banner') else None,
            'IDTIPO': to_str(pdv_detail.get('type').get('id')) if pdv_detail.get('type') else None,
            'IDPERFIL': to_str(pdv_detail.get('profile').get('id')) if pdv_detail.get('profile') else None,
            'IDCANAL': to_str(pdv_detail.get('channel').get('id')) if pdv_detail.get('channel') else None
        }
        processed_data.append(row)

    return processed_data
