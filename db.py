import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="ProgTestDb",
        user="postgres",
        password="0000"
    )