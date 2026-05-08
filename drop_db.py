from src.pss.storage.postgres.client import PostgresClient

client = PostgresClient()

client.drop_db()