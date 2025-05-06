import random
from datetime import datetime, timedelta
from postgres_db.core import PostgreSQL


def get_words_to_parse(network: str = 'facebook'):
    words_to_parse = []
    all_words = select_words_all(network)
    random.shuffle(all_words)
    all_words = filter(lambda x: filter_levels(x, network), all_words)
    for word in all_words:
        last_parse_networks = word.get('last_parse_networks') or {}
        last_scraped_str = last_parse_networks.get(network)
        if last_scraped_str:
            try:
                last_scraped = datetime.fromisoformat(last_scraped_str)
            except ValueError:
                last_scraped = datetime.now() - timedelta(days=8)
        else:
            last_scraped = datetime.now() - timedelta(days=8)
        level = int(word.get(f'{network}_level', 0))
        if level == 1 and last_scraped < datetime.now() - timedelta(hours=4):
            words_to_parse.append(word)
        elif level == 2 and last_scraped.date() < datetime.now().date():
            words_to_parse.append(word)
        elif level == 3 and last_scraped.date() < (datetime.now() - timedelta(days=3)).date():
            words_to_parse.append(word)
        elif level == 4 and last_scraped.date() < (datetime.now() - timedelta(days=7)).date():
            words_to_parse.append(word)
    return words_to_parse

def get_tracked_accounts(network: str = 'facebook'):
    accounts_to_parse = []
    all_tracked_accounts = select_tracked_accounts(network)
    random.shuffle(all_tracked_accounts)
    for tracked_account in all_tracked_accounts:
        last_scraped_str = tracked_account.get('last_scraped')
        if last_scraped_str:
            try:
                last_scraped = datetime.fromisoformat(last_scraped_str)
            except ValueError:
                last_scraped = datetime.now() - timedelta(days=10)
        else:
            last_scraped = datetime.now() - timedelta(days=10)
        level = int(tracked_account.get('level'))
        if level == 1:
            # Если с последнего парсинга прошло 5+ дней
            if last_scraped.date() < (datetime.now() - timedelta(days=5)).date():
                accounts_to_parse.append(tracked_account)
        elif level == 2:
            # Сегодня 27-е, и в этом месяце ещё не парсили
            today = datetime.now()
            if today.day == 27 and last_scraped.month != today.month:
                accounts_to_parse.append(tracked_account)
    return accounts_to_parse

def filter_levels(el: dict, network: str):
    if el.get(f'{network}_level') and int(el.get(f'{network}_level')) in [1, 2, 3, 4]:
        return True
    return False

def select_words_all(network):
    postgres_manager = PostgreSQL()
    level = f"{network}_level"
    sql = f"""SELECT * from cm_scraping_words where target_source='monitoring' and {level}>0"""
    result : list[dict] = postgres_manager.execute_query_with_results(sql)
    return [{
        "title": i.get('word'), 
        "parsing_style": i.get("parsing_style"), 
        "tag_id": i.get('id'),
        "target_source": i.get('target_source'), 
        "last_parse_networks": i.get('last_parse_networks'),
        "user_group": i.get('user_group'),
        level: i.get(level),
        "filters": i.get(f"{network}_filters"),
        "frequency": i.get(f"{network}_frequency"),
        'posts_count': i.get('max_posts'), 
        'target_countries': i.get('target_countries')
        } for i in result]

def select_tracked_accounts(network):
    postgres_manager = PostgreSQL()
    sql = """SELECT a.*, tr.user_group
            FROM cm_scraping_accounts a
            JOIN cm_tracked_accounts tr ON tr.acc_id = a.id
            WHERE a.active = true
            AND a.level > 0
            AND a.network = %s"""
    result : list[dict] = postgres_manager.execute_query_with_results(sql, [network])
    return [{
        "link": i.get('link'), 
        "level": i.get("level"), 
        "account_id": i.get('id'), 
        "account_name": i.get('name'),
        "last_scraped": i.get('last_scraped'), 
        "user_group": i.get('user_group'), 
        "posts_count": 30
        } for i in result]


def add_post_to_tag(tag_id, post_id):
    postgres_manager = PostgreSQL()
    postgres_manager.execute_query_with_results(
        """
        INSERT INTO cm_scraping_posts_v2_tags (scrapingpostv2_id, scrapingkeywords_id)
        VALUES (%s, %s) RETURNING *
        """,
        [post_id, tag_id]
    )

def exists_post_tag(tag_id, post_id):
    postgres_manager = PostgreSQL()
    print(
        f"SELECT EXISTS (SELECT 1 FROM cm_scraping_posts_v2_tags WHERE scrapingpostv2_id = {post_id} AND scrapingkeywords_id = {tag_id})")
    result = postgres_manager.execute_query_with_results(
        "SELECT EXISTS (SELECT 1 FROM cm_scraping_posts_v2_tags WHERE scrapingpostv2_id = %s AND scrapingkeywords_id = %s)",
        [post_id, tag_id]
    )[0]
    exists = result.get('exists')
    return exists

def words_last_update(word_id: int, network: str):
    postgres_manager = PostgreSQL()
    last_scarped = datetime.now().isoformat()
    query = """
    UPDATE cm_scraping_words
    SET last_parse_networks = 
        CASE
            WHEN last_parse_networks IS NULL THEN jsonb_build_object(%s, %s)
            ELSE jsonb_set(last_parse_networks, %s, to_jsonb(%s::text), true)
        END
    WHERE id = %s
    RETURNING *;
    """
    json_path = f'{{{network}}}' 
    postgres_manager.execute_query_with_results(query, [network, last_scarped, json_path, last_scarped, word_id])
    return True

def account_last_update(account_id: int):
    postgres_manager = PostgreSQL()
    last_scarped = datetime.now().isoformat()
    query = "UPDATE cm_scraping_accounts SET last_scraped=%s WHERE id=%s"
    postgres_manager.execute_query_with_results(query, [last_scarped, account_id])
    return True