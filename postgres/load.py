import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

conn = psycopg2.connect(
    dbname=db_name,
    host=db_host,
    password=db_password,
    user=db_username,
    port=db_port
)

conn.close() 