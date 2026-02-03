# 🚀 Projeto ETL – Data Warehouse (MongoDB → PostgreSQL)

Este projeto implementa um **pipeline de ETL (Extract, Transform, Load)** responsável por migrar dados operacionais armazenados em **MongoDB** para um **Data Warehouse em PostgreSQL**, utilizando **modelagem dimensional (Star Schema)** para análises analíticas e relatórios financeiros.

O objetivo é demonstrar boas práticas de **Engenharia de Dados**, incluindo:
- Tratamento de dados semi-estruturados (JSON)
- Padronização e qualidade de dados
- Uso de variáveis de ambiente
- Modelagem OLAP

---

## 🧱 Arquitetura do Projeto

**Fonte (OLTP / NoSQL)**
- MongoDB
  - users
  - products
  - carts

**Destino (OLAP / SQL)**
- PostgreSQL
  - dim_usuario
  - dim_produto
  - fact_transacao

---

## ⭐ Modelagem Dimensional

### 🔹 Dimensões

**dim_usuario**
- user_id
- name
- email
- gender
- birthdate
- city
- state
- country

**dim_produto**
- product_id
- product_name
- category
- brand
- price
- rating
- stock
- barcode
- created_at
- sku


### 🔹 Fato

**fact_transacao**
- sale_id
- user_id
- product_id
- date_id
- quantity
- unit_price
- total_price
- discount_pct
- final_price

**Grão da Fato:**
> 1 linha = 1 produto vendido em uma transação

---

## 📁 Estrutura do Projeto

```text
etl-dw/
├── db/
│   ├── mongo_config.py
│   └── postgres_config.py
│   sql/
│   ├── analises.sql
│   └── tabelas.sql
├── dim_usuario.py
├── dim_produto.py
├── fact_transacao.py
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

---

## 🔐 Variáveis de Ambiente

As credenciais **não ficam no código**. Elas são carregadas via `.env`.

### 📄 Exemplo de `.env`

```env
# MongoDB
MONGO_USER=admin
MONGO_PASSWORD=password123
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=raw_data
MONGO_AUTH_SOURCE=admin

# PostgreSQL
PG_USER=user_analytics
PG_PASSWORD=password123
PG_HOST=localhost
PG_PORT=5432
PG_DB=analytics_db
```


---

## 📦 Dependências

Instale todas as dependências com:

```bash
python -m pip install -r requirements.txt
```

Conteúdo do `requirements.txt`:

```text
pandas>=2.1,<3.0
pymongo>=4.6
sqlalchemy>=2.0,<3.0
psycopg2-binary>=2.9
python-dotenv>=1.0
openpyxl>=3.1
```

---
## ▶️ Restaurar o banco PostgreSQL com dados


docker-compose up -d

docker exec -i dest_postgres psql \
  -U user_analytics \
  -d analytics_db < postgres_dump.sql



## ▶️ Como Executar o Pipeline

### 1️⃣ Criar ambiente virtual

```bash
python -m venv venv
source venv/bin/activate   # Linux / Mac
# venv\Scripts\activate   # Windows
```

### 2️⃣ Instalar dependências

```bash
python -m pip install -r requirements.txt
```

### 3️⃣ Executar ETL

```bash
python dim_usuario.py
python dim_produto.py
python fact_transacao.py
```

---

## 🧠 Principais Decisões Técnicas

- Datas normalizadas a partir do **nível da transação (cart)**
- Conversão robusta de múltiplos formatos de data (ISO, BR, Unix timestamp)
- Dados sensíveis removidos
- Uso de **variáveis de ambiente**
- Fato com métricas financeiras prontas para BI

---

## 📊 Pronto para BI

O Data Warehouse gerado é compatível com:
- Power BI
- Tableau
- Looker

Permite análises como:
- Faturamento diário/mensal
- Ticket médio
- Produtos mais vendidos
- Descontos aplicados
- Análise por usuário e região

---

## 👨‍💻 Autor

Projeto desenvolvido como **case técnico de Engenharia de Dados**, com foco em boas práticas, clareza arquitetural e qualidade de dados.

---


