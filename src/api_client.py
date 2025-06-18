import requests
import time
import json
from config import HEADERS

_cache = {}

def get_api_data(url: str):
    if url in _cache:
        return _cache[url]

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()

            if response.status_code == 204:
                _cache[url] = None
                return None

            data = response.json()
            _cache[url] = data
            return data
            
        except requests.exceptions.RequestException as e:
            if e.response is not None and e.response.status_code == 404:
                pass
            else:
                print(f"\n[Tentativa {attempt + 1}/{max_retries}] Erro na requisição para a URL {url}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(attempt + 1)
            else:
                if not (e.response is not None and e.response.status_code == 404):
                    print(f"  > Desistindo após {max_retries} tentativas.")
                return None
        except Exception as exc:
            print(f"Ocorreu um erro inesperado: {exc}")
            return None
    return None
