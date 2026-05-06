import psycopg2
from contextlib import contextmanager
from pss_config.config import settings
from src.pss.datatypes.raw_market import RawMarket
from src.pss.datatypes.validated_market import ValidatedMarket
from src.pss.storage.shared.client import Client
from typing import Union
from src.pss.storage.postgres.queries import _upload_markets, _upsert_markets, _insert_snapshots, _get_markets_for_classification, \
    _mark_processed, _insert_classifications, _insert_pass_results, _drop_db

Market = Union[RawMarket, ValidatedMarket]

class PostgresClient(Client):
    def upload_markets(self, markets: list[Market]) -> list[str]:
        return _upload_markets(markets, self._get_conn())

    def upsert_markets(self, raw_ids: list[str], markets: list[ValidatedMarket], is_valid: bool) -> list[str]:
        return _upsert_markets(raw_ids, markets, is_valid, self._get_conn())

    def insert_snapshots(self, market_ids: list[str], markets: list[ValidatedMarket]) -> None:
        return _insert_snapshots(market_ids, markets, self._get_conn())

    def get_markets_for_classification(self) -> list[dict]:
        return _get_markets_for_classification(self._get_conn())


    def mark_processed(self, raw_market_ids: list[str]) -> None:
        return _mark_processed(raw_market_ids, self._get_conn())


    def insert_classifications(self, results: list[dict]) -> None:
        return _insert_classifications(results, self._get_conn())

    def insert_pass_results(self, results_map: dict, pass_number: int) -> None:
        return _insert_pass_results(results_map, pass_number, self._get_conn())

    def drop_db(self) -> None:
        return _drop_db(self._get_conn())

    # private methods:
    @staticmethod
    def _get_connection():
        return psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db,
            user=settings.db_user,
            password=settings.db_password,
            connect_timeout=10,
            sslmode="require",
        )

    @contextmanager
    def _get_conn(self):
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
