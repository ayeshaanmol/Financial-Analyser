import sqlite3
from typing import List, Tuple, Optional

DB_PATH = "spending.db"


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_total_outflow(start_date: Optional[str] = None, end_date: Optional[str] = None) -> float:
    query = """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM transactions
        WHERE transaction_type = 'outflow'
    """
    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    with _connect() as conn:
        row = conn.execute(query, params).fetchone()
        return float(row["total"])


def get_total_inflow(start_date: Optional[str] = None, end_date: Optional[str] = None) -> float:
    query = """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM transactions
        WHERE transaction_type = 'inflow'
    """
    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    with _connect() as conn:
        row = conn.execute(query, params).fetchone()
        return float(row["total"])


def get_net(start_date: Optional[str] = None, end_date: Optional[str] = None) -> float:
    return get_total_inflow(start_date, end_date) - get_total_outflow(start_date, end_date)


def get_spending_by_merchant(
    limit: int = 50,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Tuple[str, float]]:
    query = """
        SELECT merchant, COALESCE(SUM(amount), 0) AS total
        FROM transactions
        WHERE transaction_type = 'outflow'
    """
    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += """
        GROUP BY merchant
        ORDER BY total DESC
        LIMIT ? 
    """
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return [(r["merchant"], float(r["total"])) for r in rows]


def get_spending_by_date(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Tuple[str, float]]:
    query = """
        SELECT date, COALESCE(SUM(amount), 0) AS total
        FROM transactions
        WHERE transaction_type = 'outflow'
    """
    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += """
        GROUP BY date
        ORDER BY date ASC
    """

    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return [(r["date"], float(r["total"])) for r in rows]


def _money(x: float, currency: str = "GBP") -> str:
    symbols = {"GBP": "£", "USD": "$", "EUR": "€"}
    return f"{symbols[currency]}{x:,.2f}"
