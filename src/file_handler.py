import pandas as pd
import os

def read_excel_column_as_set(filename: str, column_name: str) -> set:
    filepath = os.path.join('dataset', f"{filename}.xlsx")
    if not os.path.exists(filepath):
        return set()
    try:
        df = pd.read_excel(filepath, dtype={column_name: str})
        return set(df[column_name].dropna())
    except Exception as e:
        print(f"Erro ao ler a coluna '{column_name}' do arquivo {filepath}: {e}")
        return set()

def save_to_excel(data: list, filename: str, append: bool = False):
    if not data:
        print(f"Nenhum dado novo de '{filename}' para salvar.")
        return

    output_dir = 'dataset'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Diretório '{output_dir}' criado.")
    
    filepath = os.path.join(output_dir, f"{filename}.xlsx")

    try:
        new_df = pd.DataFrame(data)
        
        for col in new_df.columns:
            if 'ID' in col.upper():
                new_df[col] = new_df[col].astype(str).replace(r'\.0$', '', regex=True)

        if append and os.path.exists(filepath):
            try:
                existing_df = pd.read_excel(filepath, dtype=str)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            except FileNotFoundError:
                combined_df = new_df
            
            id_column = combined_df.columns[0]
            combined_df.drop_duplicates(subset=[id_column], keep='last', inplace=True)
            
            combined_df.to_excel(filepath, index=False, engine='openpyxl')
            print(f"✅ Dataset '{filename}' atualizado em '{filepath}'. Adicionados/atualizados {len(new_df)} registros. Total: {len(combined_df)}.")
        else:
            new_df.to_excel(filepath, index=False, engine='openpyxl')
            print(f"✅ Dataset '{filename}' salvo/sobrescrito com sucesso em '{filepath}' com {len(new_df)} registros.")

    except Exception as e:
        print(f"Erro ao salvar o arquivo Excel para '{filename}': {e}")