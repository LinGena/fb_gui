import traceback
from datetime import datetime, timedelta, timezone
from psycopg2.errors import UniqueViolation
from postgres_db.posts import insert_data, check_post, update_old_post
from postgres_db.words import add_post_to_tag, exists_post_tag


def update_post(i: dict, user_group: int):
    likes = int(i.get('likes') or 0)
    comments = int(i.get('comments') or 0)
    shares = int(i.get('shares') or 0)
    try:
        acceleration = (likes + comments + shares) / ((datetime.now() - i['date']).days + 1)
    except TypeError:
        acceleration = 0
    date_added_to_db = datetime.now()
    facebook_group = i.get('facebook_group', False)
    if not i.get('date'):
        i['date'] = date_added_to_db
    i.update({
        'keywords': None,
        'emotions': None,
        'lang': None,
        'facebook_group': facebook_group,
        'defamatory': None,
        'user_group': user_group,
        'topics': None,
        'origin': None,
        'age': None,
        'gender': None,
        'opai': None,
        'photo_search': False,
        'post_comment': False,
        'acceleration': acceleration,
        'score': None
    })
    return i

def process_post(i: dict, user_group: int):
    if i['text']:
        i['text'] = str(i['text']).replace("'", "''").replace('"', '""')
    if not i.get('account'):
        i['account'] = ''
    post_db = check_post(i['text'], i['account'], i['link'], int(user_group))
    return post_db

def process_data_item(i: dict, user_group: int, tag_source, tag_id, post_link, account_id):
    profile_data = None
    if post_link:
        print('updating post')
        i = update_post(i, int(user_group))
        update_old_post(i, post_link)
        return profile_data
    try:
        if i['text']:
            i['text'] = str(i['text']).replace("'", "''").replace('"', '""')
        if not i.get('account'):
            i['account'] = ''
        post_db = process_post(i, int(user_group))
        if not post_db and tag_source == "monitoring":
            i = update_post(i, user_group)
            print(f'inserting post {i.get("id")}')
            profile_data = insert_data(i, account_id)
        elif tag_id and post_db.get('id') and not exists_post_tag(tag_id, post_db.get('id')):
            print(f"Post already exists {post_db.get('id')}")
            add_post_to_tag(tag_id, post_db.get('id'))
    except UniqueViolation:
        pass
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    return profile_data

def try_convert_date(date):
    formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"]
    for format_d in formats:
        try:
            return datetime.strptime(date, format_d)
        except ValueError:
            pass
    return False

def is_post_fresh(data):
    if isinstance(data, str):
        return True
    if data.get('date') == "no date" or not data.get('date'):
        return True
    else:
        if isinstance(data.get('date'), str):
            temp = try_convert_date(data.get('date'))
            print(temp)
            if temp:
                data['date'] = temp
        try:
            return data.get('date') > datetime.now(timezone.utc) - timedelta(days=30)
        except TypeError:
            return data.get('date') > datetime.now() - timedelta(days=30)

def process_result(data: dict, target: dict):
    profile_datas = []
    print('Processing data')
    if not data:
        return profile_datas
    tag_source = "monitoring"
    if target.get('tag_id'):
        tag_id = target.get('tag_id')
    else:
        tag_id = None
    user_group = target.get('user_group', '0')
    account_id = target.get('account_id')
    for n, i in enumerate(data):
        profile_data = process_data_item(i, user_group, tag_source, tag_id, target.get('post_link'), account_id)
        if profile_data:
            profile_datas.append(profile_data)
    return profile_datas
