
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import ast
from pathlib import Path
import os
from db.mongo_config import MongoDatabaseConfig
from db.postgres_config import PostgresDatabaseConfig
from config.logger_config import get_logger
from db.postgres_utils import truncate_table


logger = get_logger("dim_produto")

# ===============================
# EXTRACT
# ===============================

try:
    mongo_config = MongoDatabaseConfig(
        user=os.getenv("MONGO_USER"),
        password=os.getenv("MONGO_PASSWORD"),
        host=os.getenv("MONGO_HOST"),
        port=int(os.getenv("MONGO_PORT")),
        database=os.getenv("MONGO_DB"),
        auth_source=os.getenv("MONGO_AUTH_SOURCE", "admin")
    )

    db = mongo_config.connect()
    produtos_docs = list(db.products.find({}, {"_id": 0}))
    produtos = pd.DataFrame(produtos_docs)

    logger.info(f"Extraídos {len(produtos)} produtos do MongoDB")

except Exception as e:
    logger.error("Falha ao extrair produtos do MongoDB", exc_info=True)
    raise

# ===============================
# TRANSFORM
# ===============================

# Padronizar colunas
produtos.columns = produtos.columns.str.lower()

# Remover duplicados
before = len(produtos)
produtos = produtos.drop_duplicates(subset=["id"])
logger.info(f"Removidos {before - len(produtos)} produtos duplicados")


# Renomear campos (padrão DW)
produtos.rename(columns={
    "id": "product_id",
    "title": "product_name"
}, inplace=True)

# Normalizar textos
for col in ["product_name", "category", "brand"]:
    if col in produtos.columns:
        produtos[col] = (
            produtos[col]
            .astype(str)
            .str.strip()
            .str.title()
        )

# Tipagem numérica
if "price" in produtos.columns:
    produtos["price"] = pd.to_numeric(produtos["price"], errors="coerce")

if "rating" in produtos.columns:
    produtos["rating"] = pd.to_numeric(produtos["rating"], errors="coerce")

if "stock" in produtos.columns:
    produtos["stock"] = pd.to_numeric(produtos["stock"], errors="coerce")

# Normalização SKU

if "sku" in produtos.columns:
    produtos["sku"] = (
        produtos["sku"]
        .astype(str)
        .str.strip()
        .str.upper()
    )


# ===============================
# TRATAMENTO DA COLUNA PRODUCT
# ===============================
def parse_json_column(col):
    def parse(x):
        if x is None or pd.isna(x):
            return {}
        if isinstance(x, dict):
            return x
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except Exception:
                logger.warning("Erro ao converter campo meta para dict")
                return {}
        return {}
    return col.apply(parse)


# ===============================
# META (NORMALIZAÇÃO)
# ===============================
if "meta" in produtos.columns:
    produtos["meta"] = parse_json_column(produtos["meta"])

    produtos["barcode"] = produtos["meta"].apply(lambda x: x.get("barcode"))

    produtos["created_at"] = pd.to_datetime(
        produtos["meta"].apply(lambda x: x.get("createdAt")),
        errors="coerce"
    ).dt.date

    produtos.drop(columns=["meta"], inplace=True)
else:
    produtos["barcode"] = None
    produtos["created_at"] = None

# ===============================
# REMOVER CAMPOS NÃO ANALÍTICOS
# ===============================
colunas_remover = [
    "description",
    "images",
    "thumbnail"
]

produtos.drop(
    columns=[c for c in colunas_remover if c in produtos.columns],
    inplace=True
)

# ===============================
# SELEÇÃO FINAL DAS COLUNAS DA DIM
# ===============================
colunas_dim_produto = [
    "product_id",
    "sku",
    "product_name",
    "category",
    "brand",
    "price",
    "rating",
    "stock",
    "barcode",
    "created_at"
]

produtos = produtos[[c for c in colunas_dim_produto if c in produtos.columns]]

# ===============================
# AUDITORIA DE QUALIDADE
# ===============================
logger.info("Auditoria de qualidade da dim_produto")
logger.info(f"Produtos com preço nulo: {produtos['price'].isna().sum()}")
logger.info(f"Produtos com categoria nula: {produtos['category'].isna().sum()}")

# ===============================
# LOAD 
# ===============================


try:
    engine = PostgresDatabaseConfig(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT")),
        database=os.getenv("PG_DB")
    ).create_engine()

    truncate_table(engine, "dim_produto")
    logger.info("Tabela dim_produto truncada com sucesso")

    produtos.to_sql(
        "dim_produto",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    logger.info("dim_produto carregada com sucesso")

except Exception:
    logger.error("Erro na carga da dim_produto", exc_info=True)
    raise