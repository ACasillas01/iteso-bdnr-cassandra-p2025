#!/usr/bin/env python3
import logging
import os

from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        0: "Populate data",
        1: "Show accounts",
        2: "Show positions",
        3: "Show trade history",
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def print_trade_history_menu():
    thm_options = {
        1: "All Trades. (Optional date range. Defaults to latest 30 days)",
        2: "Trades by type (Buy or Sell). (Optional date range. Defaults to latest 30 days)",
        3: "Transaction by type (Buy or Sell) with instrument symbol. (Optional date range. Defaults to latest 30 days)",
        4: "Trades by symbol. (Optional date range. Defaults to latest 30 days)",
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def set_username():
    username = input('**** Username to use app: ')
    log.info(f"Username set to {username}")
    return username


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    username = set_username()

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 0:
            model.bulk_insert(session)
        if option == 1:
            model.get_user_accounts(session, username)
        if option == 2:
            model.get_account_positions(session, username)
        if option == 3:
            print_trade_history_menu()
            tv_option = int(input('Enter your trade view choice: '))
            model.trade_history_controller(tv_option, session, username)
        if option == 4:
            username = set_username()
        if option == 5:
            exit(0)


if __name__ == '__main__':
    main()
