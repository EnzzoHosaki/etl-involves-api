import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import INVOLVES_BASE_URL, INVOLVES_ENVIRONMENT_ID
from api_client import get_api_data

def _fetch_paginated_data(base_url: str) -> list:
    all_items = []
    page_num = 1
    total_pages = None

    endpoint_name = base_url.split('/')[-1] if '?' not in base_url else base_url.split('/')[-1].split('?')[0]
    
    if 'itinerary' not in endpoint_name:
        print(f"\n--- Iniciando extração para: '{endpoint_name}' ---")

    while True:
        if total_pages is not None and page_num > total_pages:
            if 'itinerary' not in endpoint_name:
                print(f"Atingido o número total de páginas ({total_pages}).")
            break

        separator = '&' if '?' in base_url else '?'
        paginated_url = f"{base_url}{separator}page={page_num}&size=100"
        response_data = get_api_data(paginated_url)

        if not response_data:
            if total_pages is not None and page_num < total_pages:
                print(f"  > Falha ao buscar a página {page_num}. Tentando a próxima...")
                page_num += 1
                continue
            else:
                break
        
        items_on_page = []
        if isinstance(response_data, list):
            items_on_page = response_data
            if not items_on_page:
                break
        elif isinstance(response_data, dict):
            items_on_page = response_data.get('items', [])
            if total_pages is None:
                total_pages = response_data.get('totalPages')
                if total_pages and 'itinerary' not in endpoint_name:
                    print(f"  > API informou um total de {total_pages} páginas.")

        if not items_on_page:
            break

        all_items.extend(items_on_page)
        if 'itinerary' not in endpoint_name:
            print(f"  > Página {page_num} processada. Total de {len(all_items)} itens acumulados.")
        page_num += 1
        time.sleep(0.2)
    
    if 'itinerary' not in endpoint_name:
        print(f"Extração de '{endpoint_name}' finalizada. Total de {len(all_items)} itens encontrados.")
    return all_items


def _fetch_details_in_parallel(url_template: str, ids: set, attach_id_field_name: str = None) -> list:
    valid_ids = {item_id for item_id in ids if item_id}
    if not valid_ids:
        return []
    
    processed_details = []
    MAX_WORKERS = 5
    total_ids = len(valid_ids)
    try:
        endpoint_name = url_template.split('/')[-2]
    except IndexError:
        endpoint_name = "desconhecido"

    def fetch_single_detail(item_id):
        detail_url = url_template.format(id=item_id)
        return get_api_data(detail_url)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(fetch_single_detail, item_id): item_id for item_id in valid_ids}
        for i, future in enumerate(as_completed(future_to_id)):
            try:
                result = future.result()
                if result and isinstance(result, dict):
                    if attach_id_field_name:
                        result[attach_id_field_name] = future_to_id[future]
                    processed_details.append(result)
            except Exception as exc: 
                print(f'Um item com ID atrelado a {future_to_id[future]} gerou uma exceção: {exc}')
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
            'IDLINHAPRODUTO': to_str(sku['productLine'].get('id')) if sku.get('productLine') else None,
            'IDMARCA': to_str(sku['brand'].get('id')) if sku.get('brand') else None,
            'IDCATEGORIA': to_str(sku['category'].get('id')) if sku.get('category') else None,
            'IDSUPERCATEGORIA': to_str(sku['supercategory'].get('id')) if sku.get('supercategory') else None,
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
    
    url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/categories/{{id}}"
    category_details = _fetch_details_in_parallel(url_template, category_ids)

    processed_data = []
    for c in category_details:
        if not isinstance(c, dict): continue
        row = {
            'ID': c.get('id'), 
            'NOME': c.get('name'), 
            'IDSUPERCATEGORIA': c['supercategory'].get('id') if c.get('supercategory') else None
        }
        processed_data.append(row)
    return processed_data

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

def process_pdv_dimensions(pdv_data: list):
    print("\n--- INICIANDO EXTRAÇÃO DE DIMENSÕES DE PDV ---")

    if not pdv_data:
        print("Nenhum dado de PDV fornecido. Impossível extrair dimensões.")
        return {"macroregionals": [], "regionals": [], "chains": [], "banners": [], "pos_types": [], "pos_profiles": [], "channels": []}

    print("\n- Extraindo dimensões a partir dos dados de PDVs...")
    macroregional_ids = {d.get('IDMACROREGIONAL') for d in pdv_data if d.get('IDMACROREGIONAL')}
    regional_ids = {d.get('IDREGIONAL') for d in pdv_data if d.get('IDREGIONAL')}
    banner_ids = {d.get('IDBANNER') for d in pdv_data if d.get('IDBANNER')}

    macroregional_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/macroregionals/{{id}}"
    macroregional_details = _fetch_details_in_parallel(macroregional_url, macroregional_ids)
    macroregionals = [{'ID': m.get('id'), 'NOME': m.get('name')} for m in macroregional_details if m]
    
    regional_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/regionals/{{id}}"
    regional_details = _fetch_details_in_parallel(regional_url, regional_ids)
    regionals = [{'ID': r.get('id'), 'NOME': r.get('name'), 'IDMACROREGIONAL': r['macroregional'].get('id') if r.get('macroregional') else None} for r in regional_details if r]

    banner_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/banners/{{id}}"
    banner_details = _fetch_details_in_parallel(banner_url, banner_ids)
    banners = [{'ID': b.get('id'), 'NOME': b.get('name'), 'IDREDE': b['chain'].get('id') if b.get('chain') else None} for b in banner_details if b]

    chain_ids = {b.get('IDREDE') for b in banners if b.get('IDREDE')}
    chain_url = f"{INVOLVES_BASE_URL}/v3/chains/{{id}}"
    chain_details = _fetch_details_in_parallel(chain_url, chain_ids)
    chains = [{'ID': c.get('id'), 'NOME': c.get('name'), 'CODIGO': c.get('code')} for c in chain_details if c]

    print("\n- Extraindo outras dimensões de PDV (Canais, Tipos, Perfis)...")
    channels = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/pointofsalechannels")]
    
    pos_types_raw = get_api_data(f"{INVOLVES_BASE_URL}/v1/pointofsaletype/find")
    pos_types = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in pos_types_raw if isinstance(i, dict)] if isinstance(pos_types_raw, list) else []

    pos_profiles_raw = get_api_data(f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/pointofsaleprofile/find")
    pos_profiles = [{'ID': i.get('id'), 'NOME': i.get('name')} for i in pos_profiles_raw if isinstance(i, dict)] if isinstance(pos_profiles_raw, list) else []

    return {"macroregionals": macroregionals, "regionals": regionals, "banners": banners, "chains": chains, "pos_types": pos_types, "pos_profiles": pos_profiles, "channels": channels}

def process_employees() -> list:
    raw_data = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/employeeenvironment")
    
    print("\n--- Formatando dataset de Colaboradores ---")
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None
    
    for emp in raw_data:
        if not isinstance(emp, dict): continue
        address, city, state = emp.get('address'), None, None
        if address: city = address.get('city')
        if city: state = city.get('state')
        
        row = {
            'IDCOLABORADOR': to_str(emp.get('id')), 'NOME': emp.get('name'), 'CARGO': emp.get('role'), 'LOGIN': emp.get('login'),
            'EMAIL': emp.get('email'), 'TELEFONE': emp.get('workPhone'), 'RG': to_str(emp.get('nationalIdCard1')), 'CPF': to_str(emp.get('nationalIdCard2')),
            'NOMEPAI': emp.get('fatherName'), 'NOMEMAE': emp.get('motherName'), 'ISACTIVE': emp.get('enabled'),
            'IDGRUPO': to_str(emp['userGroup'].get('id')) if emp.get('userGroup') else None, 'GRUPO': emp['userGroup'].get('name') if emp.get('userGroup') else None,
            'IDPERFIL': to_str(emp['profile'].get('id')) if emp.get('profile') else None, 'PERFIL': emp['profile'].get('name') if emp.get('profile') else None,
            'IDSUPERVISOR': to_str(emp['employeeEnvironmentLeader'].get('id')) if emp.get('employeeEnvironmentLeader') else None,
            'ENDERECO': address.get('address') if address else None, 'NUMERO': to_str(address.get('number')) if address else None,
            'COMPLEMENTO': address.get('complement') if address else None, 'BAIRRO': address.get('neighborhood') if address else None,
            'CEP': to_str(address.get('zipCode')) if address else None, 'CIDADE': city.get('name') if city else None,
            'ESTADO': state.get('name') if state else None,
        }
        processed_data.append(row)

    if processed_data: 
        processed_data.sort(key=lambda x: int(x['IDCOLABORADOR']) if x.get('IDCOLABORADOR') and x['IDCOLABORADOR'].isdigit() else 0)
    return processed_data

def process_supervisors(all_employees: list) -> list:
    print("\n--- Criando dataset de Supervisores ---")
    if not all_employees: return []
    
    supervisors_map = {}
    employee_map = {emp.get('IDCOLABORADOR'): emp.get('NOME') for emp in all_employees}

    for emp in all_employees:
        supervisor_id = emp.get('IDSUPERVISOR')
        if supervisor_id and supervisor_id not in supervisors_map:
            leader_name = employee_map.get(supervisor_id, "Não encontrado")
            supervisors_map[supervisor_id] = leader_name
    
    processed_data = [{'ID': id, 'NOME': name} for id, name in supervisors_map.items()]
    if processed_data:
        processed_data.sort(key=lambda x: int(x['ID']) if x.get('ID') and x['ID'].isdigit() else 0)
    return processed_data

def process_leaves() -> list:
    raw_data = _fetch_paginated_data(f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/leaves")
    processed_data = []
    to_str = lambda v: str(v) if v is not None else None
    for leave in raw_data:
        if not isinstance(leave, dict): continue
        row = {
            'IDAFFASTAIMENTO': to_str(leave.get('id')), 'DATAINICIO': leave.get('startDate'), 'DATAFIM': leave.get('endDate'),
            'MOTIVO': leave.get('reason'), 'OBSERVACAO': leave.get('note'),
            'IDCOLABORADOR': to_str(leave['employee'].get('id')) if leave.get('employee') else None,
            'IDREGISTRADOPOR': to_str(leave['registeredBy'].get('id')) if leave.get('registeredBy') else None,
            'IDSUBSTITUTO': to_str(leave['substitute'].get('id')) if leave.get('substitute') else None
        }
        processed_data.append(row)
    return processed_data

def process_scheduled_visits(all_employees: list) -> list:
    print("\n--- INICIANDO EXTRAÇÃO DE VISITAS AGENDADAS (POR COLABORADOR) ---")
    if not all_employees:
        print("Nenhum colaborador encontrado para buscar visitas.")
        return []

    all_visits = []
    to_str = lambda v: str(v) if v is not None else None
    start_date, end_date = f"{datetime.now().year}-01-01", f"{datetime.now().year + 1}-12-31"
    print(f"Buscando visitas agendadas no período de {start_date} a {end_date}")

    for i, employee in enumerate(all_employees):
        employee_id = employee.get('IDCOLABORADOR')
        if not employee_id: continue
        
        print(f"\rBuscando visitas para o colaborador {i + 1}/{len(all_employees)} (ID: {employee_id})", end="", flush=True)
        base_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/employees/{employee_id}/scheduledvisits?startDate={start_date}&endDate={end_date}"
        visits_for_employee = _fetch_paginated_data(base_url)

        for visit in visits_for_employee:
            if not isinstance(visit, dict): continue
            row = {
                'IDCOLABORADOR': to_str(employee_id), 'IDPDV': to_str(visit['pointOfSale'].get('id')) if visit.get('pointOfSale') else None,
                'DATAVISITA': visit.get('visitDate'), 'INICIOESPERADO': visit.get('expectedStart'),
                'FIMESPERADO': visit.get('expectedEnd'), 'FOIVISITADO': visit.get('visited')
            }
            all_visits.append(row)
    
    print(f"\nExtração de visitas agendadas concluída. Total de {len(all_visits)} visitas encontradas.")
    return all_visits

def process_surveys_and_answers(existing_survey_ids: set):
    print("\n--- INICIANDO EXTRAÇÃO INCREMENTAL DE PESQUISAS E RESPOSTAS ---")
    
    surveys_url = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/surveys"
    survey_summaries = get_api_data(surveys_url)
    if not survey_summaries or not isinstance(survey_summaries, list):
        print("Nenhuma pesquisa encontrada ou formato de resposta inesperado.")
        return {"new_surveys": [], "new_answers": [], "new_form_ids": set()}
    
    all_survey_ids = {str(s['id']) for s in survey_summaries if s.get('id')}
    new_survey_ids = all_survey_ids - existing_survey_ids
    if not new_survey_ids:
        print("Nenhuma pesquisa nova para processar.")
        return {"new_surveys": [], "new_answers": [], "new_form_ids": set()}

    print(f"\n--- Processando detalhes de {len(new_survey_ids)} novas pesquisas (utilizando threads) ---")
    detail_url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/surveys/{{id}}"
    survey_details = _fetch_details_in_parallel(detail_url_template, new_survey_ids)

    processed_surveys, processed_answers, new_form_ids = [], [], set()
    to_str = lambda v: str(v) if v is not None else None
    for survey in survey_details:
        if not isinstance(survey, dict): continue
        survey_id, form = survey.get('id'), survey.get('form')
        form_id = form.get('id') if form else None
        if form_id: new_form_ids.add(str(form_id))
        survey_row = {
            'IDPESQUISA': to_str(survey_id), 'LABEL': survey.get('label'), 'STATUS': survey.get('status'), 'DATAFIM': survey.get('expirationDate'),
            'DATARESPOSTA': survey.get('responseDate'), 'IDPROJETO': to_str(survey.get('projectId')), 'IDPDV': to_str(survey.get('pointOfSaleId')),
            'IDCOLABORADOR': to_str(survey.get('ownerId')), 'IDFORMULARIO': to_str(form_id)
        }
        processed_surveys.append(survey_row)
        for answer in survey.get('answers', []):
            if not isinstance(answer, dict): continue
            question, item = answer.get('question'), answer.get('item')
            answer_row = {
                'IDRESPOSTA': to_str(answer.get('id')), 'IDPESQUISA': to_str(survey_id), 'VALOR': answer.get('value'), 'PONTUACAO': answer.get('score'),
                'IDPERGUNTA': to_str(question.get('id')) if question else None, 'TIPOPERGUNTA': question.get('type') if question else None,
                'IDITEM': to_str(item.get('id')) if item else None
            }
            processed_answers.append(answer_row)
    return {"new_surveys": processed_surveys, "new_answers": processed_answers, "new_form_ids": new_form_ids}

def process_forms_and_fields(form_ids: set):
    print("\n--- INICIANDO EXTRAÇÃO DE FORMULÁRIOS E CAMPOS ---")
    if not form_ids:
        print("Nenhum novo ID de formulário para buscar.")
        return {"forms": [], "form_fields": []}
    
    url_template = f"{INVOLVES_BASE_URL}/v1/{INVOLVES_ENVIRONMENT_ID}/form/{{id}}"
    form_details = _fetch_details_in_parallel(url_template, form_ids)
    processed_forms, processed_fields = [], []
    to_str = lambda v: str(v) if v is not None else None
    for form in form_details:
        if not isinstance(form, dict): continue
        form_id = form.get('id')
        form_row = {'IDFORMULARIO': to_str(form_id), 'NOME': form.get('name'), 'DESCRICAO': form.get('description'), 'PROPOSITO': form.get('formPurpose'), 'ISACTIVE': form.get('active')}
        processed_forms.append(form_row)
        for field in form.get('formFields', []):
            if not isinstance(field, dict): continue
            info = field.get('information')
            field_row = {
                'IDCAMPO': to_str(field.get('id')), 'IDFORMULARIO': to_str(form_id), 'LABEL': info.get('label') if info else None,
                'TIPO': info.get('informationType') if info else None, 'ORDEM': field.get('order'), 'OBRIGATORIO': field.get('required'),
                'OCULTO': field.get('hidden'), 'SISTEMA': field.get('system')
            }
            processed_fields.append(field_row)
    return {"forms": processed_forms, "form_fields": processed_fields}

def process_itineraries_and_noshows():
    print("\n--- INICIANDO EXTRAÇÃO DE ROTEIROS E JUSTIFICATIVAS DE FALTA ---")
    
    all_itineraries_raw = []
    today = datetime.now().date()
    start_date = today - timedelta(days=today.weekday())
    end_date = today + timedelta(days=90)
    
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    print(f"Buscando roteiros de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")

    total_itineraries_found = 0
    for i, date in enumerate(date_range):
        # A barra de progresso agora fica fora do _fetch_paginated_data
        print(f"\r  > Processando data: {date.strftime('%Y-%m-%d')} ({i + 1}/{len(date_range)})", end="", flush=True)
        url = f"{INVOLVES_BASE_URL}/v2/environments/{INVOLVES_ENVIRONMENT_ID}/itinerary?date={date.strftime('%Y-%m-%d')}&ignoreInactive=true"
        daily_itineraries = _fetch_paginated_data(url)
        if daily_itineraries:
            total_itineraries_found += len(daily_itineraries)
            all_itineraries_raw.extend(daily_itineraries)
    
    print(f"\nExtração de roteiros finalizada. Total de {total_itineraries_found} visitas encontradas.")
    if not all_itineraries_raw:
        return {"itineraries": [], "noshows": []}

    processed_itineraries, visit_ids = [], set()
    to_str = lambda v: str(v) if v is not None else None
    for item in all_itineraries_raw:
        if not isinstance(item, dict): continue
        visit_id = item.get('itineraryId')
        if visit_id:
            visit_ids.add(visit_id)
        
        row = {
            'IDVISITA': to_str(visit_id), 
            'IDCOLABORADOR': to_str(item.get('employeeId')), 
            'NOMECOLABORADOR': item.get('employeeName'), 
            'IDPDV': to_str(item.get('pointOfSaleId')), 
            'NOMEPDV': item.get('pointOfSaleName'), 
            'CNPJPDV': item.get('pointOfSaleTaxPayerCode'), 
            'ORDEMVISITA': item.get('visitOrder')
        }
        processed_itineraries.append(row)

    print(f"\n--- Buscando detalhes de 'No Show' para {len(visit_ids)} visitas únicas ---")
    noshow_url_template = f"{INVOLVES_BASE_URL}/v3/environments/{INVOLVES_ENVIRONMENT_ID}/visits/{{id}}/noshow"
    noshow_details = _fetch_details_in_parallel(noshow_url_template, visit_ids, attach_id_field_name='IDVISITA')

    processed_noshows = []
    for item in noshow_details:
        if not isinstance(item, dict): continue
        row = {
            'IDJUSTIFICATIVA': to_str(item.get('id')),
            'IDVISITA': to_str(item.get('IDVISITA')),
            'IDMOTIVO': to_str(item.get('excuseId')),
            'DATA': item.get('date'), 'MOTIVO': item.get('excuse'),
            'OBSERVACAO': item.get('note'), 'STATUS': item.get('status'),
            'IDCOLABORADOR': to_str(item['employee'].get('id')) if item.get('employee') else None
        }
        processed_noshows.append(row)
        
    return {"itineraries": processed_itineraries, "noshows": processed_noshows}