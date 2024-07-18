#!/usr/bin/env python3
'''A script to find and react to BBH commands in comments'''
import os
import jinja2
import configparser
import time
import requests
import sqlite3
from datetime import date, datetime, timedelta
from hiveengine.api import Api
from hiveengine.wallet import Wallet

### Global configuration

BLOCK_STATE_FILE_NAME = 'lastblock.txt'

config = configparser.ConfigParser()
config.read('../../bbhbot.config')

ENABLE_COMMENTS = config['Global']['ENABLE_COMMENTS'] == 'True'
ENABLE_TRANSFERS = config['HiveEngine']['ENABLE_TRANSFERS'] == 'True'

ACCOUNT_NAME = config['Global']['ACCOUNT_NAME']
ACCOUNT_POSTING_KEY = config['Global']['ACCOUNT_POSTING_KEY']
HIVE_API_NODE = config['Global']['HIVE_API_NODE']
setApi = Api(url="https://api.primersion.com/")
TOKEN_NAME = config['HiveEngine']['TOKEN_NAME']
BOT_COMMAND_STR = config['Global']['BOT_COMMAND_STR']
SQLITE_DATABASE_FILE = 'bbhbot.db'
SQLITE_GIFTS_TABLE = 'bbh_bot_gifts'

### END Global configuration

print('Loaded configs:')
for section in config.keys():
    for key in config[section].keys():
        if '_key' in key: continue  # don't log posting/active keys
        print('%s : %s = %s' % (section, key, config[section][key]))

# Markdown templates for comments
comment_fail_template = jinja2.Template(open(os.path.join('../../templates', 'comment_fail.template'), 'r').read())
comment_outofstock_template = jinja2.Template(open(os.path.join('../../templates', 'comment_outofstock.template'), 'r').read())
comment_success_template = jinja2.Template(open(os.path.join('../../templates', 'comment_success.template'), 'r').read())
comment_daily_limit_template = jinja2.Template(open(os.path.join('../../templates', 'comment_daily_limit.template'), 'r').read())

### sqlite3 database helpers

def db_create_tables():
    db_conn = sqlite3.connect(SQLITE_DATABASE_FILE)
    c = db_conn.cursor()
    c.execute(f"CREATE TABLE IF NOT EXISTS {SQLITE_GIFTS_TABLE}(date TEXT NOT NULL, invoker TEXT NOT NULL, recipient TEXT NOT NULL, block_num INTEGER NOT NULL);")
    db_conn.commit()
    db_conn.close()

def db_save_gift(date, invoker, recipient, block_num):
    db_conn = sqlite3.connect(SQLITE_DATABASE_FILE)
    c = db_conn.cursor()
    c.execute(f'INSERT INTO {SQLITE_GIFTS_TABLE} VALUES (?,?,?,?);', [date, invoker, recipient, block_num])
    db_conn.commit()
    db_conn.close()

def db_count_gifts(date, invoker):
    db_conn = sqlite3.connect(SQLITE_DATABASE_FILE)
    c = db_conn.cursor()
    c.execute(f"SELECT count(*) FROM {SQLITE_GIFTS_TABLE} WHERE date = '{date}' AND invoker = '{invoker}';")
    row = c.fetchone()
    db_conn.commit()
    db_conn.close()
    return row[0]

def db_count_gifts_unique(date, invoker, recipient):
    db_conn = sqlite3.connect(SQLITE_DATABASE_FILE)
    c = db_conn.cursor()
    c.execute(f"SELECT count(*) FROM {SQLITE_GIFTS_TABLE} WHERE date = '{date}' AND invoker = '{invoker}' AND recipient = '{recipient}';")
    row = c.fetchone()
    db_conn.commit()
    db_conn.close()
    return row[0]

def get_dynamic_global_properties():
    url = f"{HIVE_API_NODE}"
    payload = {
        "jsonrpc": "2.0",
        "method": "condenser_api.get_dynamic_global_properties",
        "params": [],
        "id": 1
    }
    response = requests.post(url, json=payload)
    return response.json()['result']

def get_block(block_num):
    url = f"{HIVE_API_NODE}"
    payload = {
        "jsonrpc": "2.0",
        "method": "condenser_api.get_block",
        "params": [block_num],
        "id": 1
    }
    response = requests.post(url, json=payload)
    return response.json().get('result', {})

def get_comment(author, permlink):
    url = f"{HIVE_API_NODE}"
    payload = {
        "jsonrpc": "2.0",
        "method": "bridge.get_post",
        "params": {"author": author, "permlink": permlink},
        "id": 1
    }
    response = requests.post(url, json=payload)
    return response.json().get('result', {})

def get_replies(author, permlink):
    url = f"{HIVE_API_NODE}"
    payload = {
        "jsonrpc": "2.0",
        "method": "condenser_api.get_content_replies",
        "params": [author, permlink],
        "id": 1
    }
    response = requests.post(url, json=payload)
    return response.json().get('result', [])

def get_account_posts(account):
    url = f"{HIVE_API_NODE}"
    payload = {
        "jsonrpc": "2.0",
        "method": "condenser_api.get_discussions_by_author_before_date",
        "params": [account, "", "1970-01-01T00:00:00", 10],
        "id": 1
    }
    response = requests.post(url, json=payload)
    return response.json().get('result', [])

def get_block_number():
    if not os.path.exists(BLOCK_STATE_FILE_NAME):
        return None
    with open(BLOCK_STATE_FILE_NAME, 'r') as infile:
        block_num = infile.read()
        block_num = int(block_num)
        return block_num

def set_block_number(block_num):
    with open(BLOCK_STATE_FILE_NAME, 'w') as outfile:
        outfile.write('%d' % block_num)

def has_already_replied(author, permlink):
    replies = get_replies(author, permlink)
    for reply in replies:
        if reply['author'] == ACCOUNT_NAME:
            return True
    return False

def post_comment(parent_author, parent_permlink, author, comment_body):
    if ENABLE_COMMENTS:
        print('Commenting!')
        url = f"{HIVE_API_NODE}"
        payload = {
            "jsonrpc": "2.0",
            "method": "condenser_api.comment",
            "params": {
                "parent_author": parent_author,
                "parent_permlink": parent_permlink,
                "author": author,
                "permlink": "re-" + parent_permlink,
                "title": "",
                "body": comment_body,
                "json_metadata": ""
            },
            "id": 1
        }
        response = requests.post(url, json=payload)
        print(response.json())
        time.sleep(3)  # sleep 3s before continuing
    else:
        print('Debug mode comment:')
        print(comment_body)

def daily_limit_reached(invoker_name, level=1):
    today = str(date.today())
    today_gift_count = db_count_gifts(today, invoker_name)
    access_level = 'AccessLevel%d' % level
    if today_gift_count >= int(config[access_level]['MAX_DAILY_GIFTS']):
        return True
    return False

def daily_limit_unique_reached(invoker_name, recipient_name, level=1):
    today = str(date.today())
    today_gift_count_unique = db_count_gifts_unique(today, invoker_name, recipient_name)
    access_level = 'AccessLevel%d' % level
    if today_gift_count_unique >= int(config[access_level]['MAX_DAILY_GIFTS_UNIQUE']):
        return True
    return False

def get_invoker_level(invoker_name):
    wallet_token_info = Wallet(invoker_name, api=setApi).get_token(TOKEN_NAME)
    try:
        invoker_balance = float(wallet_token_info['balance'])
    except:
        invoker_balance = float(0)
    if invoker_balance >= float(config['AccessLevel4']['MIN_TOKEN_BALANCE']):
        return 4
    if invoker_balance >= float(config['AccessLevel3']['MIN_TOKEN_BALANCE']):
        return 3
    if invoker_balance >= float(config['AccessLevel2']['MIN_TOKEN_BALANCE']):
        return 2
    if invoker_balance >= float(config['AccessLevel1']['MIN_TOKEN_BALANCE']):
        return 1
    return 0

def is_block_listed(name):
    return name in config['HiveEngine']['GIFT_BLOCK_LIST'].split(',')

def can_gift(invoker_name, recipient_name):
    if is_block_listed(invoker_name) or is_block_listed(recipient_name):
        return False
    level = get_invoker_level(invoker_name)
    if level == 0 or daily_limit_reached(invoker_name, level) or daily_limit_unique_reached(invoker_name, recipient_name, level):
        return False
    return True

def get_latest_block_num():
    props = get_dynamic_global_properties()
    return props['head_block_number']

def stream_comments(start_block=None, sleep_duration=3):
    if start_block is None:
        start_block = get_latest_block_num()
    current_block = start_block
    while True:
        block = get_block(current_block)
        if not block:
            print(f"Block {current_block} not found, retrying...")
            time.sleep(3)
            continue
        for tx in block.get('transactions', []):
            for op in tx.get('operations', []):
                if op[0] == 'comment':
                    comment = op[1]
                    comment['block_num'] = current_block
                    yield comment
        current_block += 1
        time.sleep(sleep_duration)

def main():
    db_create_tables()
    start_block = get_block_number()
    latest_block = get_latest_block_num()
    max_lag_blocks = 3 * 24 * 60 * 20  # Approximate number of blocks in a week (assuming 3-second block times)
    
    # If the saved block number is more than 3 days  old, process in batches to catch up --SET TIME above here (7 for a week, 3 for 3 days, right ;-))
    if start_block and latest_block - start_block > max_lag_blocks:
        print(f"Saved block number {start_block} is over 3 days old. Catching up...")
        while start_block < latest_block - max_lag_blocks:
            for comment in stream_comments(start_block, sleep_duration=1):
                process_comment(comment)
                start_block += 1
                set_block_number(start_block)
        start_block = latest_block - max_lag_blocks

    for comment in stream_comments(start_block):
        process_comment(comment)
        set_block_number(comment['block_num'])

def process_comment(comment):
    print(f"Processing comment in block {comment['block_num']} at {comment.get('timestamp', 'unknown time')}")
    if 'author' not in comment.keys():
        return
    author_account = comment['author']
    parent_author = comment['parent_author']
    reply_identifier = '@%s/%s' % (author_account, comment['permlink'])
    if parent_author == ACCOUNT_NAME:
        message_body = '%s replied with: %s' % (author_account, comment['body'])
    if BOT_COMMAND_STR not in comment['body']:
        return
    else:
        debug_message = 'Found %s command: https://peakd.com/%s in block %s' % (BOT_COMMAND_STR, reply_identifier, comment['block_num'])
        print(debug_message)
    if author_account == parent_author:
        return
    if not parent_author:
        return
    if parent_author == ACCOUNT_NAME:
        return
    message_body = '%s asked to send a tip to %s' % (author_account, parent_author)
    try:
        time.sleep(10)
        post = get_comment(author_account, comment['permlink'])
    except Exception as e:
        print('post not found or error occurred!', e)
        return
    if has_already_replied(author_account, comment['permlink']):
        print("We already replied!")
        return
    invoker_level = get_invoker_level(author_account)
    if is_block_listed(author_account) or is_block_listed(parent_author):
        return
    if not can_gift(author_account, parent_author):
        print('Invoker doesnt meet minimum requirements')
        min_balance = float(config['AccessLevel1']['MIN_TOKEN_BALANCE'])
        if invoker_level > 0 and daily_limit_reached(author_account, invoker_level):
            max_daily_gifts = config['AccessLevel%s' % invoker_level]['MAX_DAILY_GIFTS']
            comment_body = comment_daily_limit_template.render(token_name=TOKEN_NAME, target_account=author_account, max_daily_gifts=max_daily_gifts)
            message_body = '%s tried to send %s but reached the daily limit.' % (author_account, TOKEN_NAME)
            print(message_body)
        elif invoker_level > 0 and daily_limit_unique_reached(author_account, parent_author, invoker_level):
            message_body = '%s tried to send %s but reached the daily limit.' % (author_account, TOKEN_NAME)
            print(message_body)
        else:
            comment_body = comment_fail_template.render(token_name=TOKEN_NAME, target_account=author_account, min_balance=min_balance)
            message_body = '%s tried to send %s but didnt meet requirements.' % (author_account, TOKEN_NAME)
            post_comment(parent_author, comment['permlink'], ACCOUNT_NAME, comment_body)
            print(message_body)
        return
    TOKEN_GIFT_AMOUNT = float(config['HiveEngine']['TOKEN_GIFT_AMOUNT'])
    bot_wallet = Wallet(ACCOUNT_NAME, api=setApi)
    bot_token_info = bot_wallet.get_token(TOKEN_NAME)
    if bot_token_info is None or 'balance' not in bot_token_info:
        print(f"Error: Could not retrieve balance for {TOKEN_NAME}. Token info: {bot_token_info}")
        return
    bot_balance = float(bot_token_info['balance'])
    if bot_balance < TOKEN_GIFT_AMOUNT:
        message_body = 'Bot wallet has run out of %s' % TOKEN_NAME
        print(message_body)
        comment_body = comment_outofstock_template.render(token_name=TOKEN_NAME)
        post_comment(parent_author, comment['permlink'], ACCOUNT_NAME, comment_body)
        return
    if ENABLE_TRANSFERS:
        print('[*] Transfering %f %s from %s to %s' % (TOKEN_GIFT_AMOUNT, TOKEN_NAME, ACCOUNT_NAME, parent_author))
        bot_wallet.transfer(parent_author, TOKEN_GIFT_AMOUNT, TOKEN_NAME, memo=config['HiveEngine']['TRANSFER_MEMO'])
        today = str(date.today())
        db_save_gift(today, author_account, parent_author, comment['block_num'])
        message_body = 'I sent %f %s to %s' % (TOKEN_GIFT_AMOUNT, TOKEN_NAME, parent_author)
        print(message_body)
    else:
        print('[*] Skipping transfer of %f %s from %s to %s' % (TOKEN_GIFT_AMOUNT, TOKEN_NAME, ACCOUNT_NAME, parent_author))
    today = str(date.today())
    today_gift_count = db_count_gifts(today, author_account)
    max_daily_gifts = config['AccessLevel%s' % invoker_level]['MAX_DAILY_GIFTS'] if invoker_level > 0 else 0
    comment_body = comment_success_template.render(token_name=TOKEN_NAME, target_account=parent_author, token_amount=TOKEN_GIFT_AMOUNT, author_account=author_account, today_gift_count=today_gift_count, max_daily_gifts=max_daily_gifts)
    post_comment(parent_author, comment['permlink'], ACCOUNT_NAME, comment_body)

if __name__ == '__main__':
    main()
