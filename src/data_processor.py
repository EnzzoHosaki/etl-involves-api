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

def process_point_of_sales() -> list:
    raw_data = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/pointofsales")
    print("\n--- Processando dataset de Pontos de Venda para o formato final ---")
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None
    for pdv in raw_data:
        if not isinstance(pdv, dict): continue
        row = {
            'IDPDV': to_str(pdv.get('id')),
            'RAZAOSOCIAL': pdv.get('legalBusinessName'),
            'FANTASIA': pdv.get('tradeName'),
            'CODCLI': to_str(pdv.get('code')),
            'CNPJ': pdv.get('companyRegistrationNumber'),
            'ISACTIVE': pdv.get('active'),
            'IDMACROREGIONAL': to_str(pdv['macroregional'].get('id')) if pdv.get('macroregional') else None,
            'IDREGIONAL': to_str(pdv['regional'].get('id')) if pdv.get('regional') else None,
            'IDBANNER': to_str(pdv['banner'].get('id')) if pdv.get('banner') else None,
            'IDTIPO': to_str(pdv['type'].get('id')) if pdv.get('type') else None,
            'IDPERFIL': to_str(pdv['profile'].get('id')) if pdv.get('profile') else None,
            'IDCANAL': to_str(pdv['channel'].get('id')) if pdv.get('channel') else None
        }
        processed_data.append(row)
    return processed_data

def process_pdv_dimensions():
    print("\n--- INICIANDO EXTRAÇÃO DE DIMENSÕES DE PDV ---")
    
    macroregionals = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/macroregionals")]
    regionals = [{'ID': i.get('id'), 'NOME': i.get('name'), 'IDMACROREGIONAL': i.get('macroregional', {}).get('id')} for i in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/regionals")]
    banners = [{'ID': i.get('id'), 'NOME': i.get('name'), 'IDREDE': i.get('chain', {}).get('id')} for i in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/banners")]
    chains = [{'ID': i.get('id'), 'NOME': i.get('name'), 'CODIGO': i.get('code')} for i in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/chains")]
    channels = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/pointofsalechannels")]
    
    pos_types_raw = get_api_data(f"{INVOLVES_BASE_URL}/v1/pointofsaletype/find")
    pos_types = []
    if isinstance(pos_types_raw, list):
        pos_types = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in pos_types_raw if isinstance(i, dict)]

    pos_profiles_raw = get_api_data(f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/pointofsaleprofile/find")
    pos_profiles = []
    if isinstance(pos_profiles_raw, list):
        pos_profiles = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in pos_profiles_raw if isinstance(i, dict)]

    return {
        "macroregionals": macroregionals,
        "regionals": regionals,
        "banners": banners,
        "chains": chains,
        "pos_types": pos_types,
        "pos_profiles": pos_profiles,
        "channels": channels,
    }

def process_employees() -> list:
    raw_data = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/employeeenvironment")
    
    print("\n--- Formatando dataset de Colaboradores ---")
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None
    
    for emp in raw_data:
        if not isinstance(emp, dict): continue
        address = emp.get('address', {})
        row = {
            'IDCOLABORADOR': to_str(emp.get('id')),
            'NOME': emp.get('name'),
            'CARGO': emp.get('role'),
            'LOGIN': emp.get('login'),
            'EMAIL': emp.get('email'),
            'TELEFONE': emp.get('workPhone'),
            'RG': to_str(emp.get('nationalIdCard1')),
            'CPF': to_str(emp.get('nationalIdCard2')),
            'NOMEPAI': emp.get('fatherName'),
            'NOMEMAE': emp.get('motherName'),
            'ISACTIVE': emp.get('enabled'),
            'IDGRUPO': to_str(emp.get('userGroup', {}).get('id')),
            'GRUPO': emp.get('userGroup', {}).get('name'),
            'IDPERFIL': to_str(emp.get('profile', {}).get('id')),
            'PERFIL': emp.get('profile', {}).get('name'),
            'IDSUPERVISOR': to_str(emp['employeeEnvironmentLeader'].get('id')) if emp.get('employeeEnvironmentLeader') else None,
            'ENDERECO': address.get('address'),
            'NUMERO': to_str(address.get('number')),
            'COMPLEMENTO': address.get('complement'),
            'BAIRRO': address.get('neighborhood'),
            'CEP': to_str(address.get('zipCode')),
            'CIDADE': address.get('city', {}).get('name'),
            'ESTADO': address.get('city', {}).get('state', {}).get('name'),
        }
        processed_data.append(row)

    if processed_data: processed_data.sort(key=lambda x: int(x['IDCOLABORADOR']))
    return processed_data

def process_leaves() -> list:
    base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/leaves"
    raw_data = _fetch_paginated_data(base_url)
    
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None

    for leave in raw_data:
        if not isinstance(leave, dict): continue
        row = {
            'IDAFFASTAIMENTO': to_str(leave.get('id')),
            'DATAINICIO': leave.get('startDate'),
            'DATAFIM': leave.get('endDate'),
            'MOTIVO': leave.get('reason'),
            'OBSERVACAO': leave.get('note'),
            'IDCOLABORADOR': to_str(leave.get('employee', {}).get('id')),
            'IDREGISTRADOPOR': to_str(leave.get('registeredBy', {}).get('id')),
            'IDSUBSTITUTO': to_str(leave.get('substitute', {}).get('id'))
        }
        processed_data.append(row)
    
    return processed_data

def process_scheduled_visits(all_employees: list) -> list:
    print("\n--- INICIANDO EXTRAÇÃO DE VISITAS AGENDADAS (POR COLABORADOR) ---")
    if not all_employees:
        print("Nenhum colaborador encontrado para buscar visitas.")
        return []

    all_visits = []
    total_employees = len(all_employees)
    to_str = lambda v: str(v) if v is not None else None
    
    start_date = "2024-01-01"
    end_date = "2025-12-31"
    print(f"Buscando visitas agendadas no período de {start_date} a {end_date}")

    for i, employee in enumerate(all_employees):
        employee_id = employee.get('IDCOLABORADOR')
        if not employee_id: continue
        
        print(f"\rBuscando visitas para o colaborador {i + 1}/{total_employees} (ID: {employee_id})", end="", flush=True)
        
        base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/employees/{employee_id}/scheduledvisits?startDate={start_date}&endDate={end_date}"
        visits_for_employee = _fetch_paginated_data(base_url)

        if not visits_for_employee:
            continue

        for visit in visits_for_employee:
            if not isinstance(visit, dict): continue
            
            row = {
                'IDCOLABORADOR': to_str(employee_id),
                'IDPDV': to_str(visit.get('pointOfSale', {}).get('id')),
                'DATAVISITA': visit.get('visitDate'),
                'INICIOESPERADO': visit.get('expectedStart'),
                'FIMESPERADO': visit.get('expectedEnd'),
                'FOIVISITADO': visit.get('visited')
            }
            all_visits.append(row)
    
    print(f"\nExtração de visitas agendadas concluída. Total de {len(all_visits)} visitas encontradas.")
    return all_visits
