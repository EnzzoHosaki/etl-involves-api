import pandas as pd
from datetime import datetime
import os

def save_to_excel(data: list, filename_prefix: str):
    if not data:
        print(f"Nenhum dado de '{filename_prefix}' para salvar.")
        return

    try:
        output_dir = 'dataset'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Diretório '{output_dir}' criado.")

        df = pd.DataFrame(data)
        
        base_filename = f"{filename_prefix}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
        full_filepath = os.path.join(output_dir, base_filename)
        
        df.to_excel(full_filepath, index=False, engine='openpyxl')
        print(f"✅ Dataset '{filename_prefix}' salvo com sucesso em '{full_filepath}' com {len(df)} registros.")

    except Exception as e:
        print(f"Erro ao salvar o arquivo Excel para '{filename_prefix}': {e}")