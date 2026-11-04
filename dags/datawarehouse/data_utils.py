from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor #database adapter for Python

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.curssor(cursfor_factory=RealDictCursor)

    return conn, cur

def close_conn_cursor(conn,cur):
        cur.close()
        conn.close()

def create_schema(schema):

    conn,cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXIST {schema}"
    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):
    conn, cur = get_conn_cursor()
    if schema == 'staging':
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table}(
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMSTAMP NOT NULL,
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
                        "Upload_Date" TIMSTAMP NOT NULL,
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
    
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()

    video_ids = [row["video_ID"] for row in ids]

    return video_ids