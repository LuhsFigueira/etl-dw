from sqlalchemy import text

def truncate_table(engine, table_name: str):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))

