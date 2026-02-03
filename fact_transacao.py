from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import ast
from pathlib import Path
import os
import sqlalchemy
from db.mongo_config import MongoDatabaseConfig
from db.postgres_config import PostgresDatabaseConfig
from config.logger_config import get_logger
from db.postgres_utils import truncate_table

logger = get_logger("fact_transacao")

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

    carts_docs = list(db.carts.find({}, {"_id": 0}))
    carts = pd.DataFrame(carts_docs)

    logger.info(f"Extraídos {len(carts)} carrinhos do MongoDB")

except Exception as e:
    logger.error("Falha ao extrair carrinhos do MongoDB", exc_info=True)
    raise

carts.columns = carts.columns.str.lower()
carts = carts.drop_duplicates(subset=["id"])


def parse_transaction_date(value, logger=None):

    if pd.isna(value):
        return None

    # Unix timestamp
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            return pd.to_datetime(int(value), unit="s", utc=True)
    except Exception as e:
        if logger:
            logger.warning(f"Falha ao converter timestamp: {value} | {e}")
        return None

    # ISO
    try:
        if isinstance(value, str) and value[:4].isdigit():
            return pd.to_datetime(value, errors="coerce", utc=True)
    except Exception:
        pass

    # Formato local
    try:
        return pd.to_datetime(value, errors="coerce", dayfirst=True, utc=True)
    except Exception as e:
        if logger:
            logger.warning(f"Data inválida ignorada: {value} | {e}")
        return None



# ===============================
# FUNÇÃO DE PARSE DE COLUNA PRODUCTS
# ===============================
def parse_json_column(col):
    def parse(x):
        # Caso 1: já é lista (caso correto dos carts)
        if isinstance(x, list):
            return x

        # Caso 2: nulo real
        if x is None:
            return []

        # Caso 3: NaN (somente se não for lista)
        if isinstance(x, float) and pd.isna(x):
            return []

        # Caso 4: string representando lista
        if isinstance(x, str):
            try:
                parsed = ast.literal_eval(x)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []

        # Qualquer outro tipo inesperado
        return []

    return col.apply(parse)


# ===============================
# NORMALIZAÇÃO DOS PRODUTOS
# ===============================
carts["products"] = parse_json_column(carts["products"])

# Cria coluna tratada no nível do cart
carts["transaction_date_parsed"] = carts["transaction_date"].apply(parse_transaction_date)
# ===============================

fact_rows = []

for _, row in carts.iterrows():

    sale_date = row["transaction_date_parsed"]

    for product in row["products"]:
        fact_rows.append({
            "sale_id": row["id"],
            "user_id": row["userid"],
            "product_id": product.get("id"),

            # Data da transação (nível da venda, não do produto)
            "transaction_date": sale_date,

            # Métricas
            "quantity": product.get("quantity", 0),
            "unit_price": product.get("price", 0),

            # Valores consolidados vindos da fonte
            "total_price": product.get("total", 0),
            "discount_pct": product.get("discountPercentage", 0),
            "final_price": product.get("discountedTotal", 0)
        })

        sale_date = parse_transaction_date(
            row.get("transaction_date") or row.get("date"),
            logger
            )

        if sale_date is None:
                logger.warning(
                    f"Registro ignorado por data inválida | sale_id={row['id']}"
                )
                continue



fact_sales = pd.DataFrame(fact_rows)

# ===============================
# TIPAGEM FINAL
# ===============================
fact_sales = pd.DataFrame(fact_rows)

numeric_cols = [
    "quantity",
    "unit_price",
    "total_price",
    "discount_pct",
    "final_price"
]

for col in numeric_cols:
    fact_sales[col] = pd.to_numeric(fact_sales[col], errors="coerce")



# ===============================
# AUDITORIA
# ===============================

logger.info("Auditoria de qualidade da fact_transacao")
logger.info(f"Total registros: {len(fact_sales)}")      
logger.info(f"product_id nulos: {fact_sales['product_id'].isna().sum()}")

# ===============================
# LOAD
# ===============================


try:
    engine = pg_config = PostgresDatabaseConfig(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT")),
        database=os.getenv("PG_DB")
    ).create_engine()

    truncate_table(pg_config, "fact_transacao")
    logger.info("Tabela fact_transacao truncada com sucesso")



    fact_sales.to_sql(
        "fact_transacao",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    logger.info("fact_transacao carregada com sucesso")
except Exception as e:
    logger.error("❌ Erro ao carregar fact_transacao:", e)

    
