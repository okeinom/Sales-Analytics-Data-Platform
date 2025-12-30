from sqlalchemy import create_engine
import os


def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def load_df(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists="replace", index=False)
