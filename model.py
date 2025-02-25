#!/usr/bin/env python3
import datetime
import logging
import random
import uuid
import time_uuid
from cassandra.query import BatchStatement
from cassandra.util import datetime_from_uuid1

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance DECIMAL,
        name TEXT STATIC,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity DECIMAL,
        PRIMARY KEY ((account),symbol)
    )
"""

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

SELECT_ACC_NUMS = """
    SELECT account_number 
    FROM accounts_by_user 
    WHERE username = ? """

SELECT_ACCOUNT_POSITIONS = """
    SELECT symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""

USERS = [
    ('mike', 'Michael Jones'),
    ('stacy', 'Stacy Malibu'),
    ('john', 'John Doe'),
    ('marie', 'Marie Condo'),
    ('tom', 'Tomas Train')
]
INSTRUMENTS = [
    'ETSY', 'PINS', 'SE', 'SHOP', 'SQ', 'MELI', 'ISRG', 'DIS', 'BRK.A', 'AMZN',
    'VOO', 'VEA', 'VGT', 'VIG', 'MBB', 'QQQ', 'SPY', 'BSV', 'BND', 'MUB',
    'VSMPX', 'VFIAX', 'FXAIX', 'VTSAX', 'SPAXX', 'VMFXX', 'FDRXX', 'FGXX'
]

def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)
    session.execute(batch)


def bulk_insert(session):
    acc_stmt = session.prepare("INSERT INTO accounts_by_user (username, account_number, cash_balance, name) VALUES (?, ?, ?, ?)")
    pos_stmt = session.prepare("INSERT INTO positions_by_account(account, symbol, quantity) VALUES (?, ?, ?)")
    tad_stmt = session.prepare("INSERT INTO trades_by_a_d (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    accounts = []

    accounts_num=10
    positions_by_account=100
    trades_by_account=1000
   
    # Generate accounts by user
    data = []
    for i in range(accounts_num):
        user = random.choice(USERS)
        account_number = str(uuid.uuid4())
        accounts.append(account_number)
        cash_balance = random.uniform(0.1, 100000.0)
        data.append((user[0], account_number, cash_balance, user[1]))
    execute_batch(session, acc_stmt, data)
    
   
    # Genetate possitions by account
    acc_sym = {}
    data = []
    for i in range(positions_by_account):
        while True:
            acc = random.choice(accounts)
            sym = random.choice(INSTRUMENTS)
            if acc+'_'+sym not in acc_sym:
                acc_sym[acc+'_'+sym] = True
                quantity = random.randint(1, 500)
                data.append((acc, sym, quantity))
                break
    execute_batch(session, pos_stmt, data)

    # Generate trades by account
    data = []
    for i in range(trades_by_account):
        trade_id = random_date(datetime.datetime(2013, 1, 1), datetime.datetime(2022, 8, 31))
        acc = random.choice(accounts)
        sym = random.choice(INSTRUMENTS)
        trade_type = random.choice(['buy', 'sell'])
        shares = random.randint(1, 5000)
        price = random.uniform(0.1, 100000.0)
        amount = shares * price
        data.append((acc, trade_id, trade_type, sym, shares, price, amount))
    execute_batch(session, tad_stmt, data)


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(rand_date))


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)

# Option 1
def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")

# Option 2
def get_positions(session, account):
    log.info(f"Retrieving {account} positions")
    stmt = session.prepare(SELECT_ACCOUNT_POSITIONS)
    rows = session.execute(stmt, [account])
    print(f"=== Account: {account} ===")
    for row in rows:
        print(f"- {row.symbol}: {row.quantity}")

def get_account_positions(session, username):
    log.info(f"Retrieving {username} positions")
    stmt = session.prepare(SELECT_ACC_NUMS)
    accs = session.execute(stmt, [username])
    for acc in accs:
        get_positions(session, acc.account_number)

# Option 3
#  1: "All Trades. (Optional date range. Defaults to latest 30 days)",
#  2: "Trades by type (Buy or Sell). (Optional date range. Defaults to latest 30 days)",
#  3: "Transaction by type (Buy or Sell) with instrument symbol. (Optional date range. Defaults to latest 30 days)",
#  4: "Trades by symbol. (Optional date range. Defaults to latest 30 days)",

def get_trades_by_date(session, account, start_date, end_date):
    log.info(f"Retrieving {account} trades by date")
    stmt1 = session.prepare(SELECT_ACC_NUMS)
    accs = session.execute(stmt1, [account])
    for acc in accs:
        print(acc.account_number)
        stmt = session.prepare("SELECT * FROM trades_by_a_d WHERE account = ? AND trade_id >= ? AND trade_id <= ?")
        rows = session.execute(stmt, [acc.account_number, start_date, end_date])
        for row in rows:
            print(f"=== Trade: {row.trade_id} ===")
            print(f"- Type: {row.type}")
            print(f"- Symbol: {row.symbol}")
            print(f"- Shares: {row.shares}")
            print(f"- Price: {row.price}")
            print(f"- Amount: {row.amount}")
    
def get_trades_by_type(session, account, trade_type, start_date, end_date):
    log.info(f"Retrieving {account} trades by type")
    stmt1 = session.prepare(SELECT_ACC_NUMS)
    accs = session.execute(stmt1, [account])
    for acc in accs:
        print(acc.account_number)
        stmt = session.prepare("SELECT * FROM trades_by_a_d WHERE account = ? AND type = ? AND trade_id >= ? AND trade_id <= ?")
        rows = session.execute(stmt, [account, trade_type, start_date, end_date])
        for row in rows:
            print(f"=== Trade: {row.trade_id} ===")
            print(f"- Type: {row.type}")
            print(f"- Symbol: {row.symbol}")
            print(f"- Shares: {row.shares}")
            print(f"- Price: {row.price}")
            print(f"- Amount: {row.amount}")

def get_transaction_by_type_symbol(session, account, trade_type, symbol, start_date, end_date):
    log.info(f"Retrieving {account} trades by type and symbol")
    stmt1 = session.prepare(SELECT_ACC_NUMS)
    accs = session.execute(stmt1, [account])
    for acc in accs:
        print(acc.account_number)
        stmt = session.prepare("SELECT * FROM trades_by_a_d WHERE account = ? AND type = ? AND symbol = ? AND trade_id >= ? AND trade_id <= ?")
        rows = session.execute(stmt, [account, trade_type, symbol, start_date, end_date])
        for row in rows:
            print(f"=== Trade: {row.trade_id} ===")
            print(f"- Type: {row.type}")
            print(f"- Symbol: {row.symbol}")
            print(f"- Shares: {row.shares}")
            print(f"- Price: {row.price}")
            print(f"- Amount: {row.amount}")

def get_trades_by_symbol(session, account, symbol, start_date, end_date):
    log.info(f"Retrieving {account} trades by symbol")
    stmt1 = session.prepare(SELECT_ACC_NUMS)
    accs = session.execute(stmt1, [account])
    for acc in accs:
        print(acc.account_number)
        stmt = session.prepare("SELECT * FROM trades_by_a_d WHERE account = ? AND symbol = ? AND trade_id >= ? AND trade_id <= ?")
        rows = session.execute(stmt, [account, symbol, start_date, end_date])
        for row in rows:
            print(f"=== Trade: {row.trade_id} ===")
            print(f"- Type: {row.type}")
            print(f"- Symbol: {row.symbol}")
            print(f"- Shares: {row.shares}")
            print(f"- Price: {row.price}")
            print(f"- Amount: {row.amount}")

def trade_history_controller(tv_option, session, username):

    # default date range is latest 30 days from upper limit (predefined limit)
    start_date = datetime.datetime(2022, 8, 31) - datetime.timedelta(days=30)
    end_date = datetime.datetime(2022, 8, 31)

    input_date_range = input("Define date range? (y/n): ")
    input_date_range = input_date_range.lower()

    if input_date_range == 'y' or input_date_range == 'yes':
        start_date = input('Enter start date (YYYY-MM-DD): ')
        end_date = input('Enter end date (YYYY-MM-DD): ')
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    print(start_date, end_date)
    
    # Convert dates to TimeUUID
    start_date_uuid = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(start_date))
    end_date_uuid = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(end_date))

    print(f"Date range: {start_date} to {end_date}")
    
    if tv_option == 1:
        get_trades_by_date(session, username, start_date_uuid, end_date_uuid)
    if tv_option == 2:
        trade_type = input('Enter trade type (buy or sell): ')
        get_trades_by_type(session, username, trade_type, start_date_uuid, end_date_uuid)
    if tv_option == 3:
        trade_type = input('Enter trade type (buy or sell): ')
        symbol = input('Enter symbol: ')
        get_transaction_by_type_symbol(session, username, trade_type, symbol, start_date_uuid, end_date_uuid)
    if tv_option == 4:
        symbol = input('Enter symbol: ')
        get_trades_by_symbol(session, username, symbol, start_date_uuid, end_date_uuid)