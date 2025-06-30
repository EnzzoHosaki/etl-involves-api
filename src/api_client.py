import requests
import time
import json
from config import HEADERS

_cache = {}

def get_api_data(url: str, suppress_404: bool = False):
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

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                if not suppress_404:
                    print(f"\n[INFO] Recurso não encontrado (404) na URL: {url}")
                break
            else:
                print(f"\n[Tentativa {attempt + 1}/{max_retries}] Erro HTTP {e.response.status_code} na requisição para a URL {url}")
                if attempt < max_retries - 1:
                    time.sleep(attempt + 1)
                else:
                    print(f"  > Desistindo após {max_retries} tentativas.")

        except requests.exceptions.RequestException as e:
            print(f"\n[Tentativa {attempt + 1}/{max_retries}] Erro de conexão para a URL {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(attempt + 1)
            else:
                print(f"  > Desistindo após {max_retries} tentativas.")
        
        except Exception as exc:
            print(f"Ocorreu um erro inesperado durante a requisição para {url}: {exc}")
            break

    return None