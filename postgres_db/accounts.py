from datetime import datetime
from typing import List
import time
from postgres_db.core import PostgreSQL, PostgreSQLTable


def update_account(word, account):
    postgres_manager = PostgreSQL()
    query = f"""UPDATE cm_scraping_words SET account = %s WHERE word= %s RETURNING *"""
    postgres_manager.execute_query_with_results(query, [account, word])


def get_account_links():
    postgres_manager = PostgreSQL()
    result = postgres_manager.execute_query_with_results(
        "SELECT link FROM cm_scraping_accounts where id in (SELECT acc_id from cm_tracked_accounts)")

    links = [row.get('ling') for row in result if row.get('link')]
    return links


def cm_add_account(link, name, network):
    postgres_manager = PostgreSQL()

    postgres_manager.execute_query_with_results(
        """
        INSERT INTO cm_scraping_accounts (link, name, network)
        VALUES (%s, %s, %s)
        RETURNING * 
        """,
        [link, name, network]
    )


def get_account_data(table_name: str) -> List[dict]:
    spreadsh = PostgreSQLTable(table_name=table_name)
    data = spreadsh.get_all_rows()
    return data


def account_exists(name, account_link, network):
    postgres_manager = PostgreSQL()
    exists = False
    if account_link:
        exists = postgres_manager.execute_query_with_results(
            "SELECT EXISTS (SELECT 1 FROM cm_scraping_accounts WHERE link=%s and network=%s)",
            [account_link, network]
        )[0].get('exists')
    elif name:
        exists = postgres_manager.execute_query_with_results(
            "SELECT EXISTS (SELECT 1 FROM cm_scraping_accounts WHERE name = %s and network=%s )",
            [name, network]
        )[0].get('exists')

    return exists


def update_account_link(new_link, name, network):
    postgres_manager = PostgreSQL()

    existing_account = postgres_manager.execute_query_with_results(
        "SELECT id FROM cm_scraping_accounts WHERE name = %s and network= %s",
        [name, network]
    )[0]
    if existing_account:
        postgres_manager.execute_query_with_results(
            """
            UPDATE cm_scraping_accounts
            SET link = %s
            WHERE id = %s
            RETURNING *
            """,
            [new_link, existing_account.get('id')]
        )


def update_account_name_link(account_name, account_link, network):
    postgres_manager = PostgreSQL()

    existing_account = postgres_manager.execute_query_with_results(
        "SELECT id FROM cm_scraping_accounts WHERE (name = %s or link = %s) and network = %s",

        [account_name, account_link, network]
    )[0]
    if existing_account:
        postgres_manager.execute_query_with_results(
            """
            UPDATE cm_scraping_accounts
            SET link = %s,
                name = %s
            WHERE id = %s
            RETURNING *
            """,
            [account_link, account_name, existing_account.get('id')]
        )


def get_account_id_by_name(name, network):
    postgres_manager = PostgreSQL()
    print(f"SELECT id FROM cm_scraping_accounts WHERE name = {name} and network = {network}")
    result = postgres_manager.execute_query_with_results(
        "SELECT id FROM cm_scraping_accounts WHERE name = %s and network = %s",
        [name, network]
    )[0]
    return result.get('id')


def get_accounts(table_suffix):
    postgres_manager = PostgreSQLTable(table_name="cm_social_accounts_" + table_suffix)
    data = postgres_manager.get_all_rows()
    return data


def status_to_blocked(login, table_suffix="facebook"):
    postgres_manager = PostgreSQLTable(table_name="cm_social_accounts_" + table_suffix)
    to_update = [
        {"login": login, "row": "status", "value": "blocked"},
        {"login": login, "row": "status_time", "value": datetime.now()}
    ]
    for el in to_update:
        postgres_manager.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def status_to_not_blocked(login, table_suffix="facebook"):
    postgres_manager = PostgreSQLTable(table_name="cm_social_accounts_" + table_suffix)
    to_update = [
        {"login": login, "row": "status", "value": "not_blocked"},
        {"login": login, "row": "status_time", "value": datetime.now()}
    ]
    for el in to_update:
        postgres_manager.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def delete_blocked(table_suffix):
    postgres_manager = PostgreSQLTable(table_name="cm_social_accounts_" + table_suffix)
    accounts = postgres_manager.get_all_rows()
    for i, el in enumerate(accounts):
        if el['status'] == 'blocked' and el['browser'] != 'DELETED':
            postgres_manager.update_row("id", i + 1, {"browser": "DELETED"})


def status_to_wrong_credentials(username: str, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": "status", "value": "Wrong credentials"},
        {"login": username, "row": "status_time", "value": datetime.utcnow()}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def status_to_approval_needed(username: str, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": "status", "value": "Login approval needed"},
        {"login": username, "row": "status_time", "value": datetime.utcnow()}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def status_to_challenge_needed(username: str, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": "status", "value": "Challenge"},
        {"login": username, "row": "status_time", "value": datetime.utcnow()}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def status_to_no_gologin(username: str, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": "status", "value": "No gologin profile"},
        {"login": username, "row": "status_time", "value": datetime.utcnow()}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def status_to_2fa_needed(username: str, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": "status", "value": "No 2fa code"},
        {"login": username, "row": "status_time", "value": datetime.utcnow()}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})


def make_free(username: str, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    el = {"row": "free", "value": True}
    spreadsh.update_row("login", username, {el.get('row'): el.get('value')})


def make_not_free(username, table_name: str) -> None:
    spreadsh = PostgreSQLTable(table_name=table_name)
    el = {"row": "free", "value": False}
    spreadsh.update_row("login", username, {el.get('row'): el.get('value')})


def get_random_accounts(table_name: str, number:int) -> list[dict]:
    spreadsh = PostgreSQL()
    query = f"""SELECT * FROM {table_name} WHERE status='not_blocked' AND proxy_host IS NOT NULL AND browser is not null and free=true ORDER BY RANDOM() LIMIT {number}"""
    row = spreadsh.execute_query_with_results(query)
    return row

def get_random_country_accounts(table_name: str, number:int, country_alpha3: str) -> list[dict]:
    spreadsh = PostgreSQL()
    query = f"""SELECT * FROM {table_name} WHERE status='not_blocked' AND proxy_host IS NOT NULL AND browser is not null and free=true AND target_country='{country_alpha3}' ORDER BY RANDOM() LIMIT {number}"""
    row = spreadsh.execute_query_with_results(query)
    return row

def get_most_free_accounts(table_name: str, number: int, old_account: str = None) -> list:
    spreadsh = PostgreSQL()
    query = f"""SELECT f.*, max_start_time
        FROM (
            SELECT *
            FROM {table_name}
            WHERE status='not_blocked' AND proxy_host IS NOT NULL  AND browser is not null and free=true
        ) f
        LEFT JOIN (
            SELECT acc, COUNT(1) q
            FROM cm_scraping_tasks
            WHERE status IN ('Delayed','Processing','pending','Pending') AND
                  (end_time IS NULL OR end_time 
                  = NOW() - INTERVAL '10 min')
            GROUP BY 1
        ) t ON t.acc = text(f.login)
        LEFT JOIN (
            SELECT account->>'login' as acc, MAX(start_datetime) as max_start_time
            FROM cm_scraping_tasks
            GROUP BY account->>'login'
        ) s ON s.acc = text(f.login)
    order by q, max_start_time
            limit {number}"""
    rows = spreadsh.execute_query_with_results(query)
    return rows


def update_proxy_info(username, proxy, network):
    table_name = f'cm_social_accounts_{network}'
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": 'proxy_host', 'value': proxy['ip']},
        {"login": username, "row": 'proxy_port', 'value': proxy['port_http']},
        {"login": username, "row": 'proxy_username', 'value': proxy['login']},
        {"login": username, "row": 'proxy_password', 'value': proxy['password']},
        {"login": username, "row": 'proxy_status', 'value': proxy['status']},
        {"login": username, "row": 'proxy_exp_date', 'value': proxy['date_end']}]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})
    time.sleep(5)


def update_browser_info(username, new_profile_id, network):
    table_name = f'cm_social_accounts_{network}'
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": 'browser', 'value': new_profile_id},
        {"login": username, "row": 'status_time', 'value': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})
    time.sleep(5)


def db_get_profile_id(username, network):
    spreadsh = PostgreSQLTable(table_name=f'cm_social_accounts_{network}')
    row = spreadsh.get_row('login', username)
    return row.get('browser')


def browser_to_deleted(browser_id):
    networks = ['facebook', 'twitter']
    for network in networks:
        spreadsh = PostgreSQLTable(table_name=f'cm_social_accounts_{network}')
        el = {"row": "browser", "value": "DELETED"}
        spreadsh.update_row("browser", browser_id, {el.get('row'): el.get('value')})


def update_cookies(network: str, username: str, cookies_value: dict):
    spreadsh = PostgreSQLTable(table_name=f'cm_social_accounts_{network}')
    el = {"row": "cookies", "value": cookies_value}
    spreadsh.update_row("login", username, {el.get('row'): el.get('value')})


def db_get_cookies(network: str, username: str):
    spreadsh = PostgreSQLTable(table_name=f'cm_social_accounts_{network}')
    row = spreadsh.get_row('login', username)
    return row.get('cookies')


def db_get_account_by_login(username, network):
    spreadsh = PostgreSQLTable(table_name=f'cm_social_accounts_{network}')
    row = spreadsh.get_row('login', username)
    return row


def db_get_google_drive_profile_by_profile_id(network, profile_id):
    spreadsh = PostgreSQL()
    row = spreadsh.execute_query_with_results(
        f"""select google_drive_profile from cm_social_accounts_{network} where browser=CAST('{profile_id}' as VARCHAR)""")
    return row[0].get('google_drive_profile')


def db_get_profile_network(profile_id):
    networks = ['facebook', 'twitter']
    for network in networks:
        spreadsh = PostgreSQL()
        row = spreadsh.execute_query_with_results(
            f"""select * from cm_social_accounts_{network} where browser=CAST('{profile_id}' as VARCHAR)""")
        if row:
            return network
    return None


def set_account_field(network, where, field, value):
    table_name = f'cm_social_accounts_{network}'
    spreadsh = PostgreSQLTable(table_name=table_name)
    spreadsh.update_row(where.get('field'), where.get('value'), {field: value})


def get_random_twitter_accounts_for_requests(number:int, country_alpha3:str=None) -> dict:
    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    spreadsh = PostgreSQL()
    query = f"""SELECT * FROM cm_social_accounts_twitter WHERE 
    status='not_blocked' 
    AND proxy_host IS NOT NULL
    AND free=true 
    AND (
        parse_date != '{current_date}' 
        OR count_limit_post < 300
        OR count_limit_post IS NULL
      ) """
    if country_alpha3:
        query = query + f" AND target_country='{country_alpha3}' "
    query = query + f" ORDER BY RANDOM() LIMIT {number}"
    row = spreadsh.execute_query_with_results(query)
    return row


def update_count_limit_post(username: str, count_limit_post: int, network: str):
    table_name = f'cm_social_accounts_{network}'
    spreadsh = PostgreSQLTable(table_name=table_name)
    to_update = [
        {"login": username, "row": 'count_limit_post', 'value': count_limit_post},
        {"login": username, "row": 'parse_date', 'value': datetime.utcnow().strftime("%Y-%m-%d")}
    ]
    for el in to_update:
        spreadsh.update_row("login", el.get('login'), {el.get('row'): el.get('value')})