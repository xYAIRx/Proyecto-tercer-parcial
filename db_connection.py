"""
Módulo de conexión a MariaDB.
Singleton para manejar la conexión y ejecutar queries.
"""

import mysql.connector
from mysql.connector import Error


class DBConnection:
    _instance = None
    _connection = None
    _config = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def configure(self, host: str, port: int, user: str, password: str):
        self._config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "autocommit": True,
        }

    def connect(self) -> bool:
        try:
            if self._connection and self._connection.is_connected():
                self._connection.close()
            self._connection = mysql.connector.connect(**self._config)
            return True
        except Error as e:
            raise ConnectionError(f"Error al conectar: {e}")

    def disconnect(self):
        if self._connection and self._connection.is_connected():
            self._connection.close()
            self._connection = None

    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.is_connected()

    def get_connection(self):
        if not self.is_connected():
            raise ConnectionError("No hay conexión activa a la base de datos.")
        return self._connection

    def execute_query(self, query: str, params=None, database: str = None):
        """Ejecuta una query y retorna (columnas, filas) o (None, affected_rows)."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if database:
                cursor.execute(f"USE `{database}`")
            cursor.execute(query, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return columns, rows
            else:
                conn.commit()
                return None, cursor.rowcount
        except Error as e:
            raise RuntimeError(f"Error ejecutando query: {e}")
        finally:
            cursor.close()

    def execute_many(self, queries: str, database: str = None):
        """Ejecuta múltiples statements separados por ;"""
        conn = self.get_connection()
        cursor = conn.cursor()
        results = []
        try:
            if database:
                cursor.execute(f"USE `{database}`")
            for result in cursor.execute(queries, multi=True):
                if result.with_rows:
                    columns = [desc[0] for desc in result.description]
                    rows = result.fetchall()
                    results.append(("result", columns, rows))
                else:
                    results.append(("affected", None, result.rowcount))
            return results
        except Error as e:
            raise RuntimeError(f"Error ejecutando queries: {e}")
        finally:
            cursor.close()

    def get_databases(self) -> list:
        _, rows = self.execute_query("SHOW DATABASES")
        return [row[0] for row in rows]

    def get_tables(self, database: str) -> list:
        _, rows = self.execute_query(f"SHOW TABLES FROM `{database}`")
        return [row[0] for row in rows]

    def get_server_info(self) -> dict:
        conn = self.get_connection()
        return {
            "server_version": conn.get_server_info(),
            "server_host": self._config.get("host", ""),
            "server_port": self._config.get("port", 3307),
            "user": self._config.get("user", ""),
        }
