# ETL de Integração - Involves API para Datasets

Este projeto contém um processo de ETL (Extração, Transformação e Carga) desenvolvido em Python para extrair dados da API da plataforma de trade marketing Involves, processá-los e salvá-los em datasets separados no formato Excel.

O objetivo é criar "tabelas fato" e "dimensões" limpas e prontas para serem carregadas em um data warehouse, como o Google BigQuery.

## Estrutura do Projeto

O código é segmentado em módulos, cada um com uma responsabilidade específica para garantir organização e manutenibilidade.

- `main.py`: **Orquestrador da ETL**. É o ponto de entrada que chama as funções dos outros módulos na ordem correta para executar todo o processo.
- `config.py`: **Configurações**. Carrega as credenciais e configurações da API a partir do arquivo `.env` e prepara os cabeçalhos de autenticação.
- `api_client.py`: **Cliente da API**. Responsável por realizar as requisições HTTP para a API da Involves, incluindo lógica de cache para otimização.
- `data_processor.py`: **Processador de Dados**. Contém a lógica de extração paginada e a transformação dos dados brutos da API para o formato final de cada dataset.
- `file_handler.py`: **Manipulador de Arquivos**. Responsável por salvar os dataframes processados em arquivos Excel, dentro de um diretório específico.
- `.env`: Arquivo para armazenar as credenciais e variáveis de ambiente de forma segura (não versionado pelo Git).
- `requirements.txt`: Lista as dependências Python do projeto.

## Setup e Instalação

Siga os passos abaixo para configurar e executar o projeto.

1.  **Clone o Repositório**

    ```bash
    git clone <url-do-seu-repositorio>
    cd ETL-Involves
    ```

2.  **Crie um Ambiente Virtual** (Recomendado)

    ```bash
    python -m venv venv
    ```

    Ative o ambiente:

    - No Windows: `.\venv\Scripts\Activate.ps1`
    - No macOS/Linux: `source venv/bin/activate`

3.  **Instale as Dependências**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure o Arquivo `.env`**
    Crie um arquivo chamado `.env` na raiz do projeto com o seguinte conteúdo, substituindo pelos seus dados:
    ```env
    INVOLVES_USERNAME="seu_usuario"
    INVOLVES_PASSWORD="sua_senha"
    INVOLVES_BASE_URL="[https://suaempresa.involves.com/webservices/api](https://suaempresa.involves.com/webservices/api)"
    INVOLVES_ENVIRONMENT_ID="seu_id_de_ambiente"
    ```

## Como Executar

Com o ambiente virtual ativado e o `.env` configurado, execute o orquestrador principal:

```bash
python main.py
```

O script irá processar cada entidade e salvar os arquivos resultantes na pasta `dataset/`, que será criada automaticamente na raiz do projeto.

## Datasets Gerados e Mapeamento de Campos

A seguir, a descrição de cada dataset gerado e o mapeamento dos campos da API para as colunas do arquivo Excel final.

### 1. Produtos (SKUs)

- **Arquivo:** `involves_produtos_YYYY-MM-DD_HHMMSS.xlsx`
- **Descrição:** Tabela fato principal, contendo todos os produtos e os IDs de referência para as dimensões.

| Campo na API (`sku`) | Coluna no Dataset  | Descrição                                                       |
| :------------------- | :----------------- | :-------------------------------------------------------------- |
| `id`                 | `IDPROD`           | Identificador único do produto (SKU).                           |
| `name`               | `NOMEPROD`         | Nome/descrição do produto.                                      |
| `active`             | `ISACTIVE`         | Status booleano (True/False) indicando se o produto está ativo. |
| `barCode`            | `EAN`              | Código de barras do produto.                                    |
| `integrationCode`    | `CODPROD`          | Código de integração do produto, usado em sistemas ERP.         |
| `productLine.id`     | `IDLINHAPRODUTO`   | Chave estrangeira para a tabela de Linhas de Produto.           |
| `brand.id`           | `IDMARCA`          | Chave estrangeira para a tabela de Marcas.                      |
| `category.id`        | `IDCATEGORIA`      | Chave estrangeira para a tabela de Categorias.                  |
| `supercategory.id`   | `IDSUPERCATEGORIA` | Chave estrangeira para a tabela de Supercategorias.             |
| `customFields`       | `CUSTOMFIELDS`     | Campos customizados, armazenados como um texto JSON.            |

### 2. Marcas

- **Arquivo:** `involves_marcas_YYYY-MM-DD_HHMMSS.xlsx`
- **Descrição:** Tabela de dimensão com todas as marcas.

| Campo na API (`brand`) | Coluna no Dataset | Descrição                     |
| :--------------------- | :---------------- | :---------------------------- |
| `id`                   | `ID`              | Identificador único da marca. |
| `name`                 | `NOME`            | Nome da marca.                |

### 3. Categorias

- **Arquivo:** `involves_categorias_YYYY-MM-DD_HHMMSS.xlsx`
- **Descrição:** Tabela de dimensão com todas as categorias de produto. _Nota: Este arquivo pode não ser gerado se o endpoint de lista de categorias não existir na API._

| Campo na API (`category`) | Coluna no Dataset  | Descrição                                |
| :------------------------ | :----------------- | :--------------------------------------- |
| `id`                      | `ID`               | Identificador único da categoria.        |
| `name`                    | `NOME`             | Nome da categoria.                       |
| `supercategory.id`        | `IDSUPERCATEGORIA` | Chave estrangeira para a Supercategoria. |

### 4. Supercategorias

- **Arquivo:** `involves_supercategorias_YYYY-MM-DD_HHMMSS.xlsx`
- **Descrição:** Tabela de dimensão com todas as supercategorias.

| Campo na API (`supercategory`) | Coluna no Dataset | Descrição                              |
| :----------------------------- | :---------------- | :------------------------------------- |
| `id`                           | `ID`              | Identificador único da supercategoria. |
| `name`                         | `NOME`            | Nome da supercategoria.                |

### 5. Linhas de Produto

- **Arquivo:** `involves_linhas_de_produto_YYYY-MM-DD_HHMMSS.xlsx`
- **Descrição:** Tabela de dimensão com todas as linhas de produto.

| Campo na API (`productline`) | Coluna no Dataset | Descrição                                |
| :--------------------------- | :---------------- | :--------------------------------------- |
| `id`                         | `ID`              | Identificador único da linha de produto. |
| `name`                       | `NOME`            | Nome da linha de produto.                |
