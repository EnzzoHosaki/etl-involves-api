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

### Tabelas Fato

---

#### 1. Produtos (SKUs)

- **Arquivo:** `involves_produtos_...`

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

#### 2. Pontos de Venda (PDVs)

- **Arquivo:** `involves_pdv_...`

| Campo na API (`pointofsale`) | Coluna no Dataset | Descrição                                                   |
| :--------------------------- | :---------------- | :---------------------------------------------------------- |
| `id`                         | `IDPDV`           | Identificador único do PDV.                                 |
| `legalBusinessName`          | `RAZAOSOCIAL`     | Razão Social da empresa.                                    |
| `tradeName`                  | `FANTASIA`        | Nome Fantasia do PDV.                                       |
| `code`                       | `CODCLI`          | Código interno do cliente/PDV.                              |
| `companyRegistrationNumber`  | `CNPJ`            | CNPJ do estabelecimento.                                    |
| `phone`                      | `TELEFONE`        | Telefone de contato do PDV.                                 |
| `active`                     | `ISACTIVE`        | Status booleano (True/False) indicando se o PDV está ativo. |
| `macroregional.id`           | `IDMACROREGIONAL` | Chave estrangeira para a tabela de Macrorregionais.         |
| `regional.id`                | `IDREGIONAL`      | Chave estrangeira para a tabela de Regionais.               |
| `banner.id`                  | `IDBANNER`        | Chave estrangeira para a tabela de Banners/Bandeiras.       |
| `type.id`                    | `IDTIPO`          | Chave estrangeira para a tabela de Tipos de PDV.            |
| `profile.id`                 | `IDPERFIL`        | Chave estrangeira para a tabela de Perfis de PDV.           |
| `channel.id`                 | `IDCANAL`         | Chave estrangeira para a tabela de Canais de Venda.         |

#### 3. Colaboradores

- **Arquivo:** `involves_colaboradores_...`

| Campo na API (`employee`)        | Coluna no Dataset | Descrição                                                           |
| :------------------------------- | :---------------- | :------------------------------------------------------------------ |
| `id`                             | `IDCOLABORADOR`   | Identificador único do colaborador.                                 |
| `name`                           | `NOME`            | Nome completo do colaborador.                                       |
| `login`                          | `LOGIN`           | Login/matrícula do colaborador.                                     |
| `email`                          | `EMAIL`           | E-mail do colaborador.                                              |
| `companyPhone`                   | `TELEFONE`        | Telefone de contato corporativo.                                    |
| `individualTaxpayerRegistration` | `CPF`             | CPF do colaborador.                                                 |
| `idNumber`                       | `RG`              | RG do colaborador.                                                  |
| `dateOfBirth`                    | `DATANASCIMENTO`  | Data de nascimento.                                                 |
| `gender`                         | `SEXO`            | Gênero do colaborador.                                              |
| `enabled`                        | `ISACTIVE`        | Status booleano (True/False) indicando se o colaborador está ativo. |
| `supervisor.id`                  | `IDSUPERVISOR`    | Chave estrangeira para o supervisor direto.                         |
| `accessProfile.id`               | `IDPERFILACESSO`  | Chave estrangeira para o perfil de acesso/grupo de usuário.         |

### Tabelas de Dimensão

---

#### 4. Marcas

- **Arquivo:** `involves_marcas_...`
- **Colunas:** `ID`, `NOME`

#### 5. Categorias

- **Arquivo:** `involves_categorias_...`
- **Colunas:** `ID`, `NOME`, `IDSUPERCATEGORIA`

#### 6. Supercategorias

- **Arquivo:** `involves_supercategorias_...`
- **Colunas:** `ID`, `NOME`

#### 7. Linhas de Produto

- **Arquivo:** `involves_linhas_de_produto_...`
- **Colunas:** `ID`, `NOME`

#### 8. Macrorregionais

- **Arquivo:** `involves_macroregionais_...`
- **Colunas:** `ID`, `NOME`

#### 9. Regionais

- **Arquivo:** `involves_regionais_...`
- **Colunas:** `ID`, `NOME`, `IDMACROREGIONAL`

#### 10. Redes (Chains)

- **Arquivo:** `involves_redes_...`
- **Colunas:** `ID`, `NOME`, `CODIGO`

#### 11. Banners (Bandeiras)

- **Arquivo:** `involves_banners_...`
- **Colunas:** `ID`, `NOME`, `IDREDE`

#### 12. Tipos de PDV

- **Arquivo:** `involves_tipos_pdv_...`
- **Colunas:** `ID`, `NOME`

#### 13. Perfis de PDV

- **Arquivo:** `involves_perfis_pdv_...`
- **Colunas:** `ID`, `NOME`

#### 14. Canais de Venda

- **Arquivo:** `involves_canais_...`
- **Colunas:** `ID`, `NOME`
