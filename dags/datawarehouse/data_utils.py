from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg
from psycopg.rows import dict_row #database adapter for Python

table = "yt_api"

def get_conn_cursor():
    """
    Description: Creates a connection to the PostgreSQL database.
    Input: None
    Output: tuple with dict_row factory:
        1. conn (connection object)
        2. cur (cursor object)
    """

    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(row_factory=dict_row)
    return conn, cur

def close_conn_cursor(conn,cur):
    """
    Description: Closes the cursor and connection to the PostgreSQL database.
    Input: None
    Output: tuple with dict_row factory:
        1. conn (connection object)
        2. cur (cursor object)
    """
    cur.close()
    conn.close()

def create_schema(schema):
    """
    Description: Creates a schema in the PostgreSQL database if it does not exist.
    Input: schema (string) - name of the schema to be created
    Output: None
    """

    conn,cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):
    """
    Description: Creates a table in the PostgreSQL database if it does not exist.
    Input: schema (string) - name of the schema where the table will be created.
    Output: None
    """
    conn, cur = get_conn_cursor()
    if schema == 'staging':
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table}(
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMESTAMP NOT NULL,
                        "Duration" VARCHAR(20) NOT NULL,
                        "Video_Views" INT,
                        "Likes_Count" INT,
                        "Comments_count" INT
                    );
                """
    else:
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table}(
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMESTAMP NOT NULL,
                        "Duration" TIME NOT NULL,
                        "Video_type" VARCHAR(10) NOT NULL,
                        "Video_Views" INT,
                        "Likes_Count" INT,
                        "Comments_count" INT
                    );
                """
        
    cur.execute(table_sql)
    conn.commit()    
    close_conn_cursor(conn, cur)
        

def get_video_ids(cur, schema):
    """
    Description: Retrieves the IDs of all videos from the specified schema and table.
    Input: cur (cursor object), schema (string) - name of the schema from which to retrieve video IDs
    Output: list of video IDs
    """

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids
