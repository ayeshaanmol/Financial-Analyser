import io
import sqlite3
import pandas
import streamlit
import requests
from datetime import date, timedelta

from analyse_spending import (
    get_total_outflow,
    get_total_inflow,
    get_net,
    get_spending_by_merchant,
    get_spending_by_date,
    _money
)

DB_PATH = "spending.db"

streamlit.set_page_config(page_title="Spending Tracker", layout="wide")
streamlit.title("Spending Tracker Dashboard")


@streamlit.cache_data(ttl=3600)
def get_exchange_rate(base: str = "GBP", target: str = "GBP") -> float:
    if base == target:
        return 1.0

    try:
        url = f"https://api.exchangerate.host/latest?base={base}&symbols={target}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()

        rates = data.get("rates")
        if isinstance(rates, dict) and target in rates:
            return float(rates[target])

        err = data.get("error") or {}
        info = err.get("info") or data.get("message") or str(data)
        raise ValueError(f"exchangerate.host returned no rates: {info}")

    except Exception:
        url = f"https://api.frankfurter.dev/v1/latest?base={base}&symbols={target}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()

        rates = data.get("rates")
        if isinstance(rates, dict) and target in rates:
            return float(rates[target])

        raise ValueError(f"Frankfurter returned no rates: {data}")


def import_csv_bytes_to_db(csv_bytes: bytes, db_path: str = DB_PATH):
    warnings = []

    try:
        df = pandas.read_csv(io.BytesIO(csv_bytes))
    except Exception as e:
        raise ValueError(f"Could not read CSV: {e}")

    df.columns = [c.strip().lower() for c in df.columns]

    required_cols = {"date", "merchant", "transaction_type", "amount"}
    missing = required_cols - set(df.columns)

    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    df["amount"] = pandas.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        """
        INSERT INTO transactions (date, merchant, transaction_type, amount)
        VALUES (?, ?, ?, ?)
        """,
        df[["date", "merchant", "transaction_type", "amount"]].values.tolist(),
    )

    conn.commit()
    conn.close()

    return len(df), warnings


def fetch_recent_transactions(limit=20, start_date=None, end_date=None):
    query = """
        SELECT date, merchant, transaction_type, amount
        FROM transactions
        WHERE 1=1
    """

    params = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC, id DESC LIMIT ?"
    params.append(limit)

    conn = sqlite3.connect(DB_PATH)
    df = pandas.read_sql_query(query, conn, params=params)
    conn.close()

    return df


streamlit.sidebar.header("Controls")

streamlit.sidebar.subheader("Currency")

currency = streamlit.sidebar.selectbox(
    "Select currency",
    ["GBP", "USD", "EUR"]
)

try:
    exchange_rate = get_exchange_rate("GBP", currency)
except Exception as e:
    streamlit.sidebar.warning(f"Could not fetch live exchange rate. Using 1.0. ({e})")
    exchange_rate = 1.0


streamlit.sidebar.caption(f"Exchange rate: 1 GBP = {exchange_rate:.3f} {currency}")

top_n = streamlit.sidebar.slider("Top merchants", 5, 50, 10)
recent_n = streamlit.sidebar.slider("Recent transactions", 5, 50, 20)

streamlit.sidebar.subheader("Date range")

range_mode = streamlit.sidebar.selectbox(
    "Range",
    ["All time", "Last 7 days", "Last 30 days", "This month", "Custom"],
)

today = date.today()

start = None
end = None

if range_mode == "Last 7 days":
    start = today - timedelta(days=6)
    end = today

elif range_mode == "Last 30 days":
    start = today - timedelta(days=29)
    end = today

elif range_mode == "This month":
    start = today.replace(day=1)
    end = today

elif range_mode == "Custom":
    start = streamlit.sidebar.date_input("Start date", today.replace(day=1))
    end = streamlit.sidebar.date_input("End date", today)

start_str = start.isoformat() if start else None
end_str = end.isoformat() if end else None


streamlit.sidebar.subheader("Import CSV")

uploaded = streamlit.sidebar.file_uploader("Upload CSV", type=["csv"])

if uploaded:
    if streamlit.sidebar.button("Import"):
        inserted, warnings = import_csv_bytes_to_db(uploaded.getvalue())
        streamlit.sidebar.success(f"Imported {inserted} rows")
        streamlit.rerun()


streamlit.subheader("Summary")

total_in = get_total_inflow(start_str, end_str) * exchange_rate
total_out = get_total_outflow(start_str, end_str) * exchange_rate
net = get_net(start_str, end_str) * exchange_rate

col1, col2, col3 = streamlit.columns(3)

col1.metric("Total inflow", _money(total_in, currency))
col2.metric("Total outflow", _money(total_out, currency))
col3.metric("Net", _money(net, currency))

streamlit.divider()


left, right = streamlit.columns(2)

with left:
    streamlit.subheader("Spending by Merchant")

    rows = get_spending_by_merchant(
        limit=top_n,
        start_date=start_str,
        end_date=end_str
    )

    df_m = pandas.DataFrame(rows, columns=["Merchant", "Total"])

    if not df_m.empty:
        df_m["Total"] *= exchange_rate

        streamlit.dataframe(df_m, use_container_width=True)
        streamlit.bar_chart(df_m.set_index("Merchant")["Total"])
    else:
        streamlit.info("No data")


with right:
    streamlit.subheader("Spending Over Time")

    rows = get_spending_by_date(
        start_date=start_str,
        end_date=end_str
    )

    df_d = pandas.DataFrame(rows, columns=["Date", "Total"])

    if not df_d.empty:
        df_d["Total"] *= exchange_rate

        streamlit.dataframe(df_d, use_container_width=True)
        streamlit.line_chart(df_d.set_index("Date")["Total"])
    else:
        streamlit.info("No data")

streamlit.divider()


streamlit.subheader("Recent Transactions")

df_recent = fetch_recent_transactions(
    limit=recent_n,
    start_date=start_str,
    end_date=end_str
)

if not df_recent.empty:
    df_recent["amount"] = df_recent["amount"].astype(float) * exchange_rate

    df_recent.rename(columns={
        "date": "Date",
        "merchant": "Merchant",
        "transaction_type": "Type",
        "amount": "Amount"
    }, inplace=True)

    streamlit.dataframe(df_recent, use_container_width=True)
else:
    streamlit.info("No transactions")
