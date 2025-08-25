"""Data storage functionality for Avanza CLI."""

import os
import sqlite3
import datetime
from contextlib import contextmanager
from typing import Generator


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply SQLite PRAGMAs for optimal performance and safety."""
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")


def get_conn(db_path: str = './data/stocks.sqlite') -> sqlite3.Connection:
    """Get a SQLite connection with proper configuration.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Configured SQLite connection with Row factory and PRAGMAs applied
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create connection with type detection
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    
    # Set row factory for dict-like access
    conn.row_factory = sqlite3.Row
    
    # Apply PRAGMAs
    _apply_pragmas(conn)
    
    return conn


@contextmanager
def write_txn(conn: sqlite3.Connection, mode: str = 'IMMEDIATE') -> Generator[None, None, None]:
    """Context manager for write transactions with proper error handling.
    
    Args:
        conn: SQLite connection
        mode: Transaction mode ('IMMEDIATE', 'EXCLUSIVE', etc.)
        
    Yields:
        None - use for write operations within the transaction
    """
    conn.execute(f"BEGIN {mode};")
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def upsert_stock(ticker: str, name: str, avanza_url: str, db_path: str = './data/stocks.sqlite') -> int:
    """Insert or update a stock record and return its ID.
    
    Args:
        ticker: Stock ticker symbol (must be non-empty)
        name: Stock name
        avanza_url: URL to Avanza page for this stock
        db_path: Path to the SQLite database file
        
    Returns:
        The stock ID (integer)
        
    Raises:
        ValueError: If ticker is empty
    """
    if not ticker.strip():
        raise ValueError("Ticker cannot be empty")
    
    # Convert to string to ensure consistent storage
    avanza_url = str(avanza_url)
    
    conn = get_conn(db_path)
    
    try:
        with write_txn(conn):
            # Try the primary SQL with RETURNING
            try:
                result = conn.execute("""
                    INSERT INTO stocks(ticker, name, avanza_url) VALUES(?, ?, ?)
                    ON CONFLICT(ticker) DO UPDATE SET 
                        name = excluded.name, 
                        avanza_url = excluded.avanza_url
                    RETURNING id;
                """, (ticker, name, avanza_url))
                
                row = result.fetchone()
                if not row:
                    raise RuntimeError(f"Failed to get ID from RETURNING for ticker: {ticker}")
                return row[0]
                
            except sqlite3.OperationalError as e:
                # Fallback for environments without RETURNING support
                if "RETURNING" in str(e).upper():
                    # Execute the upsert without RETURNING
                    conn.execute("""
                        INSERT INTO stocks(ticker, name, avanza_url) VALUES(?, ?, ?)
                        ON CONFLICT(ticker) DO UPDATE SET 
                            name = excluded.name, 
                            avanza_url = excluded.avanza_url;
                    """, (ticker, name, avanza_url))
                    
                    # Then SELECT the ID
                    result = conn.execute("SELECT id FROM stocks WHERE ticker = ?;", (ticker,))
                    row = result.fetchone()
                    if not row:
                        raise RuntimeError(f"Failed to retrieve stock ID for ticker: {ticker}")
                    return row[0]
                else:
                    # Re-raise if it's a different error
                    raise
    finally:
        conn.close()


def insert_metric(stock_id: int, metric_key: str, metric_value: str, as_of_date: str | None = None, db_path: str = './data/stocks.sqlite') -> int:
    """Insert a metric record and return its ID.
    
    Args:
        stock_id: ID of the stock this metric belongs to
        metric_key: Key/name of the metric (must be non-empty)
        metric_value: Value of the metric (must be non-empty)
        as_of_date: Date for the metric (ISO format), defaults to today
        db_path: Path to the SQLite database file
        
    Returns:
        The metric ID (integer)
        
    Raises:
        ValueError: If metric_key or metric_value is empty
        sqlite3.IntegrityError: If duplicate (stock_id, metric_key, as_of_date) or invalid stock_id
    """
    if not metric_key.strip():
        raise ValueError("Metric key cannot be empty")
    if not metric_value.strip():
        raise ValueError("Metric value cannot be empty")
    
    # Default to today's date if not provided
    if as_of_date is None:
        as_of_date = datetime.date.today().isoformat()
    
    conn = get_conn(db_path)
    
    try:
        with write_txn(conn):
            # Try INSERT with RETURNING
            try:
                result = conn.execute("""
                    INSERT INTO metrics(stock_id, metric_key, metric_value, as_of_date) 
                    VALUES(?, ?, ?, ?) 
                    RETURNING id;
                """, (stock_id, metric_key, metric_value, as_of_date))
                
                row = result.fetchone()
                if not row:
                    raise RuntimeError(f"Failed to get ID from RETURNING for metric: {metric_key}")
                return row[0]
                
            except sqlite3.OperationalError as e:
                # Fallback for environments without RETURNING support
                if "RETURNING" in str(e).upper():
                    # Execute INSERT without RETURNING
                    conn.execute("""
                        INSERT INTO metrics(stock_id, metric_key, metric_value, as_of_date) 
                        VALUES(?, ?, ?, ?);
                    """, (stock_id, metric_key, metric_value, as_of_date))
                    
                    # Get the ID using last_insert_rowid
                    result = conn.execute("SELECT last_insert_rowid();")
                    row = result.fetchone()
                    if not row:
                        raise RuntimeError(f"Failed to retrieve metric ID for key: {metric_key}")
                    return row[0]
                else:
                    # Re-raise if it's a different error
                    raise
    finally:
        conn.close()


def count_summary_rows(db_path: str = './data/stocks.sqlite') -> dict:
    """Count the number of rows in each main table for reporting.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dictionary with counts: {'stocks': n_stocks, 'metrics': n_metrics}
    """
    conn = get_conn(db_path)
    
    try:
        # Count rows in each table
        stocks_count = conn.execute("SELECT COUNT(*) FROM stocks;").fetchone()[0]
        metrics_count = conn.execute("SELECT COUNT(*) FROM metrics;").fetchone()[0]
        
        return {
            'stocks': stocks_count,
            'metrics': metrics_count
        }
    finally:
        conn.close()