import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import INVOLVES_BASE_URL, INVOLVES_ENVIRONMENT_ID
from api_client import get_api_data

def _fetch_paginated_data(base_url: str) -> list:
    all_items = []
    page_num = 1
    endpoint_name = base_url.split('/')[-1] if '?' not in base_url else base_url.split('/')[-1].split('?')[0]
    
    print(f"\n--- Iniciando extração para: '{endpoint_name}' ---")

    while True:
        separator = '&' if '?' in base_url else '?'
        paginated_url = f"{base_url}{separator}page={page_num}&size=100"
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


def _fetch_details_in_parallel(url_template: str, ids: set) -> list:
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

def process_product_dimensions():
    print("\n--- INICIANDO EXTRAÇÃO DE DIMENSÕES DE PRODUTO ---")
    
    brands = [{'ID': b.get('id'), 'NOME': b.get('name')} for b in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/brands") if isinstance(b, dict)]
    supercategories = [{'ID': sc.get('id'), 'NOME': sc.get('name')} for sc in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/supercategories") if isinstance(sc, dict)]
    productlines = [{'ID': pl.get('id'), 'NOME': pl.get('name')} for pl in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/productlines") if isinstance(pl, dict)]
    
    return {
        "brands": brands,
        "supercategories": supercategories,
        "productlines": productlines
    }

def process_skus() -> list:
    raw_data = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/skus")
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
    print("\n--- INICIANDO EXTRAÇÃO DE DIMENSÃO DE CATEGORIAS (VIA SKUS) ---")
    if not all_skus:
        print("Nenhum SKU encontrado para extrair categorias.")
        return []

    category_ids = {sku.get('IDCATEGORIA') for sku in all_skus if sku.get('IDCATEGORIA')}
    category_ids.discard(None) 

    url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/categories/{{id}}"
    category_details = _fetch_details_in_parallel(url_template, category_ids)

    return [{'ID': c.get('id'), 'NOME': c.get('name'), 'IDSUPERCATEGORIA': c.get('supercategory', {}).get('id')} for c in category_details if isinstance(c, dict)]


def process_all_pdv_related_data():
    print("\n--- INICIANDO EXTRAÇÃO DE PDVS E DIMENSÕES RELACIONADAS ---")
    
    pdv_summaries = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales")
    print("\n--- Processando detalhes de cada Ponto de Venda (utilizando threads) ---")
    url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales/{{id}}"
    pdv_ids = {pdv['id'] for pdv in pdv_summaries if pdv.get('id')}
    pdv_details = _fetch_details_in_parallel(url_template, pdv_ids)

    dimension_ids = {
        'macroregional': set(), 'regional': set(), 'banner': set(),
        'type': set(), 'profile': set(), 'channel': set(), 'chain': set()
    }
    
    print("\n--- Coletando IDs de dimensão a partir dos PDVs ---")
    for pdv in pdv_details:
        if not isinstance(pdv, dict): continue
        if pdv.get('macroregional'): dimension_ids['macroregional'].add(pdv.get('macroregional').get('id'))
        if pdv.get('regional'): dimension_ids['regional'].add(pdv.get('regional').get('id'))
        if pdv.get('banner'): dimension_ids['banner'].add(pdv.get('banner').get('id'))
        if pdv.get('type'): dimension_ids['type'].add(pdv.get('type').get('id'))
        if pdv.get('profile'): dimension_ids['profile'].add(pdv.get('profile').get('id'))
        if pdv.get('channel'): dimension_ids['channel'].add(pdv.get('channel').get('id'))
    
    for key in dimension_ids:
        dimension_ids[key].discard(None)

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

    to_str = lambda v: str(v) if v is not None else None
    
    processed_pdvs = [{
        'IDPDV': to_str(pdv.get('id')), 'RAZAOSOCIAL': pdv.get('legalBusinessName'),
        'FANTASIA': pdv.get('tradeName'), 'CODCLI': to_str(pdv.get('code')),
        'CNPJ': pdv.get('companyRegistrationNumber'), 'TELEFONE': pdv.get('phone'),
        'ISACTIVE': pdv.get('active'),
        'IDMACROREGIONAL': to_str(pdv['macroregional'].get('id')) if pdv.get('macroregional') else None,
        'IDREGIONAL': to_str(pdv['regional'].get('id')) if pdv.get('regional') else None,
        'IDBANNER': to_str(pdv['banner'].get('id')) if pdv.get('banner') else None,
        'IDTIPO': to_str(pdv['type'].get('id')) if pdv.get('type') else None,
        'IDPERFIL': to_str(pdv['profile'].get('id')) if pdv.get('profile') else None,
        'IDCANAL': to_str(pdv['channel'].get('id')) if pdv.get('channel') else None
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

def process_employees() -> list:
    employee_summaries = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/employeeenvironment")
    
    print("\n--- Processando detalhes de cada Colaborador (utilizando threads) ---")
    employee_ids = {emp.get('id') for emp in employee_summaries if emp.get('id')}
    detail_url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/employees/{{id}}"
    employee_details = _fetch_details_in_parallel(detail_url_template, employee_ids)

    print("\n--- Formatando dataset de Colaboradores ---")
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None
    
    for emp in employee_details:
        if not isinstance(emp, dict): continue
        row = {
            'IDCOLABORADOR': to_str(emp.get('id')),
            'NOME': emp.get('name'),
            'LOGIN': emp.get('login'),
            'EMAIL': emp.get('email'),
            'TELEFONE': emp.get('companyPhone'),
            'CPF': to_str(emp.get('individualTaxpayerRegistration')),
            'RG': to_str(emp.get('idNumber')),
            'DATANASCIMENTO': emp.get('dateOfBirth'),
            'SEXO': emp.get('gender'),
            'ISACTIVE': emp.get('enabled'),
            'IDSUPERVISOR': to_str(emp['supervisor'].get('id')) if emp.get('supervisor') else None,
            'IDPERFILACESSO': to_str(emp['accessProfile'].get('id')) if emp.get('accessProfile') else None
        }
        processed_data.append(row)

    if processed_data:
        processed_data.sort(key=lambda x: int(x['IDCOLABORADOR']))

    return processed_data
