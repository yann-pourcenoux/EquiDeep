"""Database schema creation and management for Avanza CLI."""

import sqlite3
from .datastore import get_conn


# PRD DDL - table definitions for stocks and metrics only
PRD_DDL = """
-- Stocks table: core stock information
CREATE TABLE IF NOT EXISTS stocks (
    id INTEGER PRIMARY KEY,
    ticker TEXT UNIQUE NOT NULL,
    name TEXT,
    avanza_url TEXT
);

-- Metrics table: stock metrics with unique constraint on (stock_id, metric_key, as_of_date)
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    metric_key TEXT NOT NULL,
    metric_value TEXT NOT NULL,
    as_of_date TEXT,
    UNIQUE(stock_id, metric_key, as_of_date)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_metrics_stock_id ON metrics(stock_id);
CREATE INDEX IF NOT EXISTS idx_metrics_as_of_date ON metrics(as_of_date);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Execute the PRD DDL to create tables and indexes.
    
    Args:
        conn: SQLite connection to execute DDL on
    """
    conn.executescript(PRD_DDL)


def init_db(db_path: str = './data/stocks.sqlite') -> sqlite3.Connection:
    """Initialize database with schema and return configured connection.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Configured SQLite connection with schema initialized
    """
    conn = get_conn(db_path)
    ensure_schema(conn)
    return conn
