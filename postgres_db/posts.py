from datetime import datetime, timedelta
from postgres_db.accounts import account_exists, cm_add_account, get_account_id_by_name, update_account_name_link
from postgres_db.core import PostgreSQL


def insert_data(data: dict, account_id: int = None):
    print('Account', data['account'])
    postgres_manager = PostgreSQL()
    id_ = int(data['id'])
    print(f"Account: {data['account']}, Account link: {data.get('account_link', '')}, Network: {data['network']}")
    account = data['account']
    account_link = data.get('account_link', '')
    link = data['link']
    network = data['network']
    date = data['date']
    date_added_to_db = datetime.now()
    if date == 'no date':
        date = date_added_to_db
    post_text = data.get('text', '')
    keywords = data.get('keywords', '')
    emotions = data.get('emotions', '')
    defamatory = data.get('defamatory', '')
    score = data.get('score')
    if score is not None:
        score = float(score)
    likes = data.get('likes', '')
    comments = data.get('comments', '')
    shares = data.get('shares', '')
    pic = data.get('pic', '')
    lang = data.get('lang', '')
    acceleration = data.get('acceleration', '')
    topics = data.get('topics', '')
    origin = data.get('origin', '')
    age = data.get('age', '')
    gender = data.get('gender', '')
    opai = data.get('opai', '')
    photo_search = data.get('photo_search', '')
    post_comment = data.get('post_comment', '')
    facebook_group = data.get('facebook_group', '')
    user_group = int(data.get('user_group', 0))
    target_country = data.get('target_country', '')
    if not account_id:
        if not account_exists(account,account_link, network):
            print("Account does not exist")
            cm_add_account(account_link, account, network)
        elif account and account_link:
            print("Account exists")
            update_account_name_link(account, account_link, network)
        social_account_id = get_account_id_by_name(account, network)
    else:
        social_account_id = account_id
    insert_query = f"""INSERT INTO cm_scraping_posts_v2 (id, post_text, link,network, date, keywords,
                        emotions, date_added_to_db, defamatory, score,likes,comments, shares,pic,lang,acceleration,topics,origin,age,gender,openai,search_photo,post_comment,facebook_group,social_account_id,account, group_id,target_country)
                         VALUES (%s,%s,%s,%s,%s,%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s) RETURNING *"""
    postgres_manager.execute_query_with_results(insert_query,
                                                [id_, post_text, link, network, date, keywords, emotions,
                                                 date_added_to_db, defamatory, score,
                                                 likes, comments, shares, pic, lang, acceleration,
                                                 topics,
                                                 origin, age, gender, opai, photo_search, post_comment,
                                                 facebook_group,
                                                 social_account_id, account, user_group, target_country])
    print("Post is inserted")
    profile_data = {
        'account_id': social_account_id,
        'link': account_link,
        'account_name': account,
        'user_group': user_group
    }
    return profile_data

def update_old_post(data, post_link):
    postgres_manager = PostgreSQL()
    account = data['account']
    account_link = data.get('account_link', '')
    network = data['network']
    date = data['date']
    date_added_to_db = datetime.now()
    if date == 'no date':
        date = date_added_to_db
    post_text = data.get('text', '')
    likes = data.get('likes', '')
    comments = data.get('comments', '')
    shares = data.get('shares', '')
    pic = data.get('pic', '')
    user_group = int(data.get('user_group', 0))
    if not account_exists(account, account_link, network):
        cm_add_account(account_link, account, network)
    elif account and account_link:
        update_account_name_link(account, account_link, network)
    social_account_id = get_account_id_by_name(account, network)
    update_query = f"""UPDATE cm_scraping_posts_v2 SET post_text = %s, likes = %s, comments = %s, shares = %s, pic = %s, date = %s, social_account_id = %s, account = %s, group_id = %s WHERE link = %s RETURNING *"""
    postgres_manager.execute_query_with_results(update_query,
                                                [post_text, likes, comments, shares, pic, date, social_account_id,
                                                 account, user_group, post_link])

def select_data():
    """ Select all rows from any table"""
    postgres_manager = PostgreSQL()
    result = postgres_manager.execute_query_with_results(f"SELECT * from cm_scraping_posts_v2")
    for i in result:
        print(i)

def check_post(text, author, link, user_group):
    postgres_manager = PostgreSQL()
    query = f"SELECT link, id FROM cm_scraping_posts_v2 WHERE ((post_text = %s AND account = %s) OR link = %s) and group_id = %s" if author else f"SELECT link FROM cm_scraping_posts_v2 WHERE (post_text = %s OR link = %s) and group_id = %s"
    params = [text, author, link, user_group] if author else [text, link, user_group]
    result = postgres_manager.execute_query_with_results(query, params)
    if result and result[0].get('link') != '#':
        return {"id": result[0].get('id'), "link": result[0].get('link')}
    return []

def get_text(text):
    postgres_manager = PostgreSQL()
    query = f"SELECT post_text_original from cm_scraping_posts_v2 WHERE post_text_original = '%s'"
    result = postgres_manager.execute_query_with_results(query, [text])
    if len(result) > 0:
        return [i.get('post_text_original') for i in result]
    else:
        return []

def fetch_links_from_generic_tracking_posts():
    postgres_manager = PostgreSQL()
    current_datetime = datetime.now()
    threshold_datetime = current_datetime - timedelta(days=2)
    default_date = datetime(2000, 1, 1)
    result = postgres_manager.execute_query_with_results("SELECT post_id, date_processed FROM generic_tracking_posts")
    eligible_post_ids = [
        post_id for post_id, date_processed in result if (date_processed or default_date) < threshold_datetime
    ]
    if eligible_post_ids:
        result = postgres_manager.execute_query_with_results(
            "SELECT link FROM cm_scraping_posts_v2 WHERE id IN %s",
            list(eligible_post_ids))
        links = result
        links = [link.get('ling') for link in links]
    else:
        links = []
    return links

def get_post_by_link(link):
    postgres_manager = PostgreSQL()
    query = f"SELECT * from cm_scraping_posts_v2 WHERE link = %s"
    result = postgres_manager.execute_query_with_results(query, [link])
    return result[0] if result else None