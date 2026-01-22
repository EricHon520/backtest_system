CREATE TABLE IF NOT EXISTS historical_data (
    ticker TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    frequency TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    source TEXT NOT NULL,
    created_at INTEGER NOT NULL,

    PRIMARY KEY (ticker, timestamp, frequency)
)