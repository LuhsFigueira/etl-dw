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


logger = get_logger("dim_usuario")

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
    users_docs = list(db.users.find({}, {"_id": 0}))
    users = pd.DataFrame(users_docs)

    logger.info(f"Extraídos {len(users)} usuários do MongoDB")

except Exception as e:
    logger.error("Falha ao extrair usuários do MongoDB", exc_info=True)
    raise

# ===============================
# TRANSFORM
# ===============================

# Padronizar nomes de colunas
users.columns = users.columns.str.lower()

# Normalizar nomes básicos
for col in ["firstname", "lastname", "maidenname"]:
    if col in users.columns:
        users[col] = (
            users[col]
            .astype(str)
            .str.strip()
            .str.title()
            .replace("Nan", None)
        )

# Criar NAME unificado (campo analítico)
def build_name(row):
    parts = [row["firstname"]]

    if pd.notna(row.get("maidenname")):
        parts.append(row["maidenname"])
    elif pd.notna(row.get("lastname")):
        parts.append(row["lastname"])

    return " ".join([p for p in parts if p])

users["name"] = users.apply(build_name, axis=1)

# Normalizar email
users["email"] = users["email"].str.lower().str.strip()

# Tipagem de datas
users["birthdate"] = pd.to_datetime(users["birthdate"], errors="coerce")


# Validação de idade

users.loc[
    (users["age"] < 0) | (users["age"] > 120),
    "age"
] = None

logger.info("Idades inválidas foram mantidas como NULL")




# Validação de gênero
users["gender"] = users["gender"].where(
    users["gender"].isin(["male", "female"]),
    "unknown"
)

logger.info("Gêneros inválidos normalizados para 'unknown'")



# Duplicados por email

users = users.drop_duplicates(subset=["id"], keep="first")
logger.info("Duplicidade tratada com base no user_id")


# ===============================
# NORMALIZAÇÃO DE CAMPOS JSON (somente o que agrega)
# ===============================
def parse_json_column(col):
    def parse(x):
        if pd.isna(x):
            return None

        if isinstance(x, dict):
            return x

        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except Exception:
                return {}

        return None

    return col.apply(parse)


# Address → localização (valor analítico real)
users["address"] = parse_json_column(users["address"])
users["city"] = users["address"].apply(lambda x: x.get("city"))
users["state"] = users["address"].apply(lambda x: x.get("state"))
users["country"] = users["address"].apply(lambda x: x.get("country"))

users.drop(columns=["address"], inplace=True)


# ===============================
# DADOS SENSÍVEIS (REMOVER TOTAL)
# ===============================
colunas_sensiveis = [
    "password",
    "cpf",
    "cnpj",
    "bank",
    "company",
    "crypto",
    "cardnumber",
    "cardexpire",
    "cardsymbol"
]

users.drop(
    columns=[c for c in colunas_sensiveis if c in users.columns],
    inplace=True
)

# ===============================
# IDADE USUÁRIO
# ===============================
users["birthdate"] = pd.to_datetime(
    users["birthdate"],
    errors="coerce"
).dt.date




# ===============================
# SELEÇÃO FINAL (DIM_USER)
# ===============================
dim_user = users[
    [
        "id",
        "name",
        "email",
        "gender",
        "birthdate",
        "city",
        "state",
        "country"
    ]
].rename(columns={"id": "user_id"})

# ===============================
# AUDITORIA DE QUALIDADE
# ===============================


logger.info("Auditoria de qualidade da dim_usuario")
logger.info(f"Total registros: {len(dim_user)}")    
logger.info(f"Emails nulos: {dim_user['email'].isna().sum()}")
logger.info(f"Usuários sem cidade: {dim_user['city'].isna().sum()}")


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

    truncate_table(pg_config, "dim_usuario")
    logger.info("Tabela dim_usuario truncada com sucesso")


    dim_user.to_sql(
        "dim_usuario",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    logger.info("dim_usuario carregada com sucesso")
    
except Exception as e:
    logger.error("❌ Erro ao carregar dim_usuario: %s", e)