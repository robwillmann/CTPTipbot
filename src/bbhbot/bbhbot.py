#!/usr/bin/env python3
'''A script to find and react to BBH commands in comments'''

import hashlib_patch
import logging
#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(filename='log.txt', level=logging.DEBUG, format='%(asctime)s %(message)s')


import os
import jinja2
import configparser
import time
import requests
import sqlite3
import json
from datetime import date, datetime, timedelta
from beem import Hive
from beem.transactionbuilder import TransactionBuilder
from beembase.operations import Comment
from beem.account import Account
from beem.exceptions import MissingKeyError
from beemgraphenebase.account import PrivateKey
from beembase.operations import Transfer
from hiveengine.api import Api
from hiveengine.wallet import Wallet
import re

### Global configuration

heartbeat_url = "https://uptime.betterstack.com/api/v1/heartbeat/BCcqsDBriwHoNVGLj44v3vap"

BLOCK_STATE_FILE_NAME = 'lastblock.txt'

config = configparser.ConfigParser()
config.read('../../bbhbot.config')


ENABLE_COMMENTS = config['Global']['ENABLE_COMMENTS'] == 'True'
ENABLE_TRANSFERS = config['HiveEngine']['ENABLE_TRANSFERS'] == 'True'

ACCOUNT_NAME = config['Global']['ACCOUNT_NAME']
ACCOUNT_POSTING_KEY = config['Global']['ACCOUNT_POSTING_KEY']
ACCOUNT_ACTIVE_KEY = config['Global']['ACCOUNT_ACTIVE_KEY']
HIVE_API_NODES = [
    config['Global']['HIVE_API_NODE'],
    'https://api.hive.blog',
    'https://anyx.io',
    'https://api.openhive.network',
    'https://api.deathwing.me',
    'https://hive-api.arcange.eu',
    'hive-api.3speak.tv'
]
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


#Betterstack Heartbeat
def send_heartbeat():
    try:
        response = requests.get(heartbeat_url)
        if response.status_code == 200:
            print("Heartbeat sent successfully.")
        else:
            print(f"Failed to send heartbeat. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending heartbeat: {e}")

def periodic_heartbeat(interval=300):
    while True:
        send_heartbeat()
        time.sleep(interval)

# Run the heartbeat monitor in the background
import threading
heartbeat_thread = threading.Thread(target=periodic_heartbeat)
heartbeat_thread.daemon = True
heartbeat_thread.start()


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
    for node in HIVE_API_NODES:
        try:
            response = requests.post(node, json={
                "jsonrpc": "2.0",
                "method": "condenser_api.get_dynamic_global_properties",
                "params": [],
                "id": 1
            })
            response.raise_for_status()
            result = response.json()
            if 'result' in result:
                return result['result']
            else:
                print(f"Unexpected response structure from {node}: {result}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get dynamic global properties from {node}: {e}")
    return None

def get_latest_block_num():
    props = get_dynamic_global_properties()
    return props['head_block_number'] if props else None

def get_block(block_num):
    for node in HIVE_API_NODES:
        try:
            response = requests.post(node, json={
                "jsonrpc": "2.0",
                "method": "condenser_api.get_block",
                "params": [block_num],
                "id": 1
            })
            response.raise_for_status()
            result = response.json()
            if 'result' in result:
                return result['result']
            else:
                print(f"Unexpected response structure from {node} for block {block_num}: {result}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get block {block_num} from {node}: {e}")
    return None

def get_comment(author, permlink):
    for node in HIVE_API_NODES:
        try:
            response = requests.post(node, json={
                "jsonrpc": "2.0",
                "method": "bridge.get_post",
                "params": {"author": author, "permlink": permlink},
                "id": 1
            })
            response.raise_for_status()
            result = response.json()
            if 'result' in result:
                return result['result']
            else:
                print(f"Unexpected response structure from {node} for comment {author}/{permlink}: {result}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get comment {author}/{permlink} from {node}: {e}")
    return None

def get_replies(author, permlink):
    for node in HIVE_API_NODES:
        try:
            response = requests.post(node, json={
                "jsonrpc": "2.0",
                "method": "condenser_api.get_content_replies",
                "params": [author, permlink],
                "id": 1
            })
            response.raise_for_status()
            result = response.json()
            if 'result' in result:
                return result['result']
            else:
                print(f"Unexpected response structure from {node} for replies {author}/{permlink}: {result}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get replies for {author}/{permlink} from {node}: {e}")
    return []

def get_account_posts(account):
    for node in HIVE_API_NODES:
        try:
            response = requests.post(node, json={
                "jsonrpc": "2.0",
                "method": "condenser_api.get_discussions_by_author_before_date",
                "params": [account, "", "1970-01-01T00:00:00", 10],
                "id": 1
            })
            response.raise_for_status()
            result = response.json()
            if 'result' in result:
                return result['result']
            else:
                print(f"Unexpected response structure from {node} for account posts {account}: {result}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get account posts for {account} from {node}: {e}")
    return []

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

def generate_valid_permlink(permlink):
    # Convert to lowercase, replace invalid characters with hyphens
    permlink = permlink.lower()
    permlink = re.sub(r'[^a-z0-9-]', '-', permlink)
    return permlink

def post_comment(parent_author, parent_permlink, author, comment_body):
    if ENABLE_COMMENTS:
        print('Commenting!')
        hive = Hive(keys=[ACCOUNT_POSTING_KEY])
        authorperm = generate_valid_permlink('re-' + parent_author + '-' + datetime.now().strftime("%Y%m%dT%H%M%S"))
        comment_op = Comment(
            **{
                "parent_author": parent_author,
                "parent_permlink": parent_permlink,
                "author": ACCOUNT_NAME,
                "permlink": authorperm,
                "title": '',
                "body": comment_body,
                "json_metadata": {}
            }
        )
        print('CommentDets:')
        print(comment_op)
        print('***********')
        for attempt in range(5):
            try:
                tx = TransactionBuilder(blockchain_instance=hive)
                tx.appendOps(comment_op)
                #tx.appendSigner(ACCOUNT_NAME, 'posting')
                tx.appendWif(ACCOUNT_POSTING_KEY)
                tx.sign()
                tx.broadcast()
                print(f"Comment posted to {parent_author}/{parent_permlink}")
                return
            except Exception as e:
                print(f"Failed to post comment (attempt {attempt + 1}/5): {e}")
                if 'comment_ptr != nullptr' in str(e):
                    print(f"The comment with permlink {parent_permlink} does not exist. Skipping.")
                    return
                if attempt < 4:
                    print(f"Retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)
                    # Switch node
                    hive.nodes = HIVE_API_NODES[attempt % len(HIVE_API_NODES)]
                else:
                    print("Max retries reached. Giving up on posting comment.")
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
    if invoker_balance >= float(config['AccessLevel5']['MIN_TOKEN_BALANCE']):
        return 5
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

def stream_comments(start_block=None, sleep_duration=2):
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

def fetch_ref_block_data():
    for node in HIVE_API_NODES:
        try:
            response = requests.post(node, json={
                "jsonrpc": "2.0",
                "method": "condenser_api.get_dynamic_global_properties",
                "params": [],
                "id": 1
            })
            response.raise_for_status()
            result = response.json()
            if 'result' in result:
                head_block_number = result['result']['head_block_number']
                block_response = requests.post(node, json={
                    "jsonrpc": "2.0",
                    "method": "condenser_api.get_block",
                    "params": [head_block_number],
                    "id": 1
                })
                block_response.raise_for_status()
                block_result = block_response.json()
                if 'result' in block_result:
                    block_id = block_result['result']['block_id']
                    return head_block_number, block_id
            else:
                print(f"Unexpected response structure from {node}: {result}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch reference block data from {node}: {e}")
    return None, None

def sign_and_broadcast_transaction(custom_json_operation):
    head_block_number, block_id = fetch_ref_block_data()
    if not head_block_number or not block_id:
        print("Failed to fetch reference block data. Cannot construct transaction.")
        return None

    ref_block_num = (head_block_number - 1) & 0xFFFF
    ref_block_prefix = int(block_id[:8], 16)

    transaction = {
        "expiration": (datetime.utcnow() + timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%S'),
        "ref_block_num": ref_block_num,
        "ref_block_prefix": ref_block_prefix,
        "operations": [[
            "custom_json",
            {
                "required_auths": [ACCOUNT_NAME],
                "required_posting_auths": [],
                "id": "ssc-mainnet-hive",
                "json": json.dumps(custom_json_operation)
            }
        ]],
        "extensions": []
    }





    # Sign the transaction
    tx = TransactionBuilder(transaction, hive_instance=hive)
    print('TX:')
    print(tx)
    print('++++++++')
    try:
        print(f"In Transaction: Account {ACCOUNT_NAME} has a balance of: {account['balance']}")
        #tx.appendSigner(ACCOUNT_NAME, 'posting')
        #tx.appendOps(Transfer(custom_json_operation))
        #tx.appendSigner(ACCOUNT_NAME, "active") # or 
        tx.appendWif(ACCOUNT_ACTIVE_KEY)

        print('Appendsigner happening...')
        # Add a delay before signing
        time.sleep(5)  # 5 seconds delay
        #tx.sign()
        try:
            #tx.sign()
            signed_tx = tx.sign()
            print('Transaction signed successfully.')
            print(signed_tx)
            print('- - - - -')
        except MissingKeyError:
            print("Loaded keys:", hive.wallet.getPublicKeys())
            print("Missing key error: Ensure the correct key is loaded in the wallet.")
        except Exception as e:
            print(f"An error occurred during signing: {e}")
        print('Transaction signed successfully.')
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

    for i, node in enumerate(HIVE_API_NODES):
        try:
            tx.broadcast()
            print(f"Transaction broadcasted successfully via node {node}")
            return {"status": "success", "node": node}
        except Exception as e:
            print(f"Failed to broadcast transaction to node {node}: {e}")

        # Rotate to the next node
        if i < len(HIVE_API_NODES) - 1:
            print(f"Retrying with next node in {2 ** i} seconds...")
            time.sleep(2 ** i)
        else:
            print("All nodes failed. Giving up.")
            return None

def transfer_token(to_account, amount, token_name, memo):
    # Prepare the custom JSON operation
    json_data = {
        "contractName": "tokens",
        "contractAction": "transfer",
        "contractPayload": {
            "symbol": token_name,
            "to": to_account,
            "quantity": f"{amount:.8f}",
            "memo": memo
        }
    }
    print('TokenDets:')
    print(json_data)
    print('***********')
    # Sign and broadcast the transaction
    response = sign_and_broadcast_transaction(json_data)
    print('TokenTransferResponse:')
    print(response)
    print('***********')
    if response:
        print(f"Transaction broadcasted successfully: {response}")
    else:
        print("Failed to broadcast transaction.")
    return response

def main():
    db_create_tables()
    start_block = get_block_number()
    latest_block = get_latest_block_num()

    if latest_block is None:
        print("Could not retrieve the latest block number. Exiting.")
        return

    max_lag_blocks = 3 * 24 * 60 * 20  # Approximate number of blocks in a week (assuming 3-second block times)
    
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

def get_comment_timestamp(comment):
    # Try to get timestamp from comment
    timestamp = comment.get('timestamp')

    # If timestamp is not found, retrieve it from the block data
    if not timestamp:
        block_num = comment.get('block_num')
        if block_num:
            block = get_block(block_num)
            if block and 'timestamp' in block:
                timestamp = block['timestamp']
            else:
                timestamp = 'unknown time'
        else:
            timestamp = 'unknown time'

    return timestamp

def process_comment(comment):
    comment_timestamp = get_comment_timestamp(comment)
    #print(f"Processing comment at {comment_timestamp}")
    print(f"Processing comment: Looking for {BOT_COMMAND_STR} in block {comment['block_num']} at {comment_timestamp}")
    if 'author' not in comment.keys():
        return
    author_account = comment['author']
    parent_author = comment.get('parent_author')
    parent_permlink = comment.get('parent_permlink')
    reply_identifier = '@%s/%s' % (author_account, comment['permlink'])
    if not parent_author or not parent_permlink:
        print(f'Not Parent_author or Parent_permlink: {parent_author} / {parent_permlink}')
        return
    if parent_author == ACCOUNT_NAME:
        message_body = '%s replied with: %s' % (author_account, comment['body'])
    if BOT_COMMAND_STR not in comment['body']:
        return
    else:
        debug_message = 'Found %s command: https://peakd.com/%s in block %s' % (BOT_COMMAND_STR, reply_identifier, comment['block_num'])
        print(debug_message)
    if author_account == parent_author:
        debug_message = 'Author_account: %s == Parent_author: %s' % (author_account, parent_author)
        print(debug_message)
        return
    if parent_author == ACCOUNT_NAME:
        debug_message = 'Parent_author: %s == Accountname: %s' % (parent_author, ACCOUNT_NAME)
        print(debug_message)
        return
    message_body = '%s asked to send a tip to %s' % (author_account, parent_author)
    try:
        time.sleep(10)
        post = get_comment(author_account, comment['permlink'])
        if not post:
            print(f"Parent comment {author_account}/{comment['permlink']} does not exist. Skipping.")
            return
    except Exception as e:
        print('Post not found or error occurred!', e)
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
            post_comment(parent_author, parent_permlink, ACCOUNT_NAME, comment_body)
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
        post_comment(parent_author, parent_permlink, ACCOUNT_NAME, comment_body)
        return
    if ENABLE_TRANSFERS:
        print('[*] Transfering %f %s from %s to %s' % (TOKEN_GIFT_AMOUNT, TOKEN_NAME, ACCOUNT_NAME, parent_author))
        try:
            transfer_token(parent_author, TOKEN_GIFT_AMOUNT, TOKEN_NAME, memo=config['HiveEngine']['TRANSFER_MEMO'])
            today = str(date.today())
            db_save_gift(today, author_account, parent_author, comment['block_num'])
            message_body = 'I sent %f %s to %s' % (TOKEN_GIFT_AMOUNT, TOKEN_NAME, parent_author)
            print(message_body)
        except Exception as e:
            print(f"Failed to transfer token:in process_comment {e} ")
            return
    else:
        print('[*] Skipping transfer of %f %s from %s to %s' % (TOKEN_GIFT_AMOUNT, TOKEN_NAME, ACCOUNT_NAME, parent_author))
    today = str(date.today())
    today_gift_count = db_count_gifts(today, author_account)
    max_daily_gifts = config['AccessLevel%s' % invoker_level]['MAX_DAILY_GIFTS'] if invoker_level > 0 else 0
    comment_body = comment_success_template.render(token_name=TOKEN_NAME, target_account=parent_author, token_amount=TOKEN_GIFT_AMOUNT, author_account=author_account, today_gift_count=today_gift_count, max_daily_gifts=max_daily_gifts)
    post_comment(parent_author, parent_permlink, ACCOUNT_NAME, comment_body)

if __name__ == '__main__':
    #print('Using key: ')
    #print(ACCOUNT_ACTIVE_KEY)
    hive = Hive(nodes=HIVE_API_NODES, keys=[ACCOUNT_ACTIVE_KEY])  # Initialize Hive instance
    
    print("Loaded keys:", hive.wallet.getPublicKeys())
    account = Account(ACCOUNT_NAME, blockchain_instance=hive)
    print(f"Account {ACCOUNT_NAME} has a balance of: {account['balance']}")
    
    # Check if the instance is working correctly
    print("Hive instance initialized successfully.")

    main()
