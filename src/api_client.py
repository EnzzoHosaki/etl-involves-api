import requests
from config import HEADERS

_cache = {}

def get_api_data(url: str):
    if url in _cache:
        return _cache[url]

    try:
        response = requests.get(url, headers=HEADERS)
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
            print(f"Erro na requisição para a URL {url}: {e}")
        return None
    except requests.exceptions.JSONDecodeError:
        print(f"Erro: A resposta da URL {url} não é um JSON válido.")
        return None
