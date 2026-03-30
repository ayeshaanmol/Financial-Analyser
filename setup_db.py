import sqlite3

connection = sqlite3.connect("spending.db")

cursor = connection.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    merchant TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    amount REAL NOT NULL
)
""")

connection.commit()
connection.close()

print("Database and table created successfully.")