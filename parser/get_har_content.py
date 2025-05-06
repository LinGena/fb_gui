import json
import time
import re
from threading import Thread
from datetime import datetime
from utils.func import load_file
from postgres_db.data_processing import process_result
from postgres_db.words import words_last_update, account_last_update


class GetHarContent():
    def __init__(self, task: dict) -> None:
        self.filename = 'www.facebook.com.har'
        self.posts = []
        self.tag = task

    def run_keywords(self):
        try:
            scr = load_file(self.filename, in_dir=False)
            data = json.loads(scr)
            for entries in data['log']['entries']:
                for i,n in entries.items():
                    if i == 'response':
                        if 'content' in n and 'text' in n['content']:
                            posts_block = n['content']['text']
                            if 'TOP_PUBLIC_POSTS' in str(posts_block):
                                decoder = json.JSONDecoder()
                                idx = 0
                                length = len(posts_block)
                                while idx < length:
                                    try:
                                        deserialized_data, next_idx = decoder.raw_decode(posts_block, idx)
                                        idx = next_idx
                                        self.get_words_post_datas(deserialized_data)
                                    except json.JSONDecodeError as e:
                                        idx += 1
        except Exception as ex:
            print(ex)
        words_last_update(self.tag.get('tag_id'), 'facebook')
        if self.posts:
            self.posts = list(map(self.add_tag_attrs, self.posts))
            Thread(target=process_result, args=(self.posts, self.tag)).start()
    
    def get_words_post_datas(self, data: dict) -> None:
        if 'serpResponse' in data['data']:
            edges = data['data']['serpResponse']['results']['edges']
            for edge in edges:
                try:
                    story = edge['rendering_strategy']['view_model']['click_model']['story']
                    content = story['comet_sections']['content']['story']
                    feedback = story['comet_sections']['feedback']['story']['story_ufi_container']['story']['feedback_context']['feedback_target_with_context']
                    res = {
                        'id': str(time.time()).replace('.', '')[8:].ljust(9, '0'),
                        'pic': self.get_pic(content),
                        'link': content['wwwURL'],
                        'date': datetime.fromtimestamp(story['comet_sections']['timestamp']['story']['creation_time']),
                        'likes': feedback['comet_ufi_summary_and_actions_renderer']['feedback']['reaction_count']['count'],
                        'comments': feedback['comment_rendering_instance']['comments']['total_count'],
                        'shares': feedback['comet_ufi_summary_and_actions_renderer']['feedback']['share_count']['count'],
                        'text': content['message']['text'],
                        'account': content['actors'][0]['name'],
                        'account_link': content['actors'][0]['url'],
                        'network': 'facebook',
                    }
                    self.posts.append(res)
                except Exception as ex:
                    print(ex)

    def run_tracked_accounts(self):
        try:
            scr = load_file(self.filename, in_dir=False)
            data = json.loads(scr)
            for entries in data['log']['entries']:
                for i,n in entries.items():
                    if i == 'response':
                        if 'content' in n and 'text' in n['content']:
                            posts_block = n['content']['text']
                            if '__isFeedUnit' in str(posts_block):
                                json_objects = re.findall(r'{.*?}(?=\s*{|\s*$)', posts_block, re.DOTALL)
                                for obj in json_objects:
                                    try:
                                        deserialized_data = json.loads(obj)
                                        self.get_account_posts(deserialized_data)
                                    except json.JSONDecodeError:
                                        continue
        except Exception as ex:
            print(ex)
        print(len(self.posts))
        account_last_update(self.tag.get('account_id'))
        if self.posts:
            self.posts = list(map(self.add_tag_attrs, self.posts))
            Thread(target=process_result, args=(self.posts, self.tag)).start()

    def get_account_posts(self, data: dict) -> None:
        edges = data.get('data',{}).get('node',{})
        if 'timeline_list_feed_units' in edges:
            edges = edges.get('timeline_list_feed_units',{}).get('edges')
        if not edges:
            return
        if not isinstance(edges, list):
            edges = [edges]
        try:
            for edge in edges:
                if 'node' in edge:
                    edge = edge['node']
                content = edge['comet_sections']['content']['story']
                feedback = edge['comet_sections']['feedback']['story']['story_ufi_container']['story']['feedback_context']['feedback_target_with_context']
                    
                res = {
                    'id': str(time.time()).replace('.', '')[8:].ljust(9, '0'),
                    'pic': self.get_pic(content),
                    'link': content['wwwURL'],
                    'date': datetime.fromtimestamp(edge['comet_sections']['timestamp']['story']['creation_time']),
                    'likes': feedback['comet_ufi_summary_and_actions_renderer']['feedback']['reaction_count']['count'],
                    'comments': feedback['comment_rendering_instance']['comments']['total_count'],
                    'shares': feedback['comet_ufi_summary_and_actions_renderer']['feedback']['share_count']['count'],
                    'text': self.get_text(content),
                    'account': self.tag.get('account_name'),
                    'account_link': self.tag.get('link'),
                    'network': 'facebook',
                }
                self.posts.append(res)
        except Exception as ex:
            print('[get_account_posts]',ex)

    def get_text(self, content) -> str:
        try:
            return content['comet_sections']['message']['story']['message']['text']
        except:
            return ""

    def get_pic(self, content: dict) -> str | None:
        try:
            attachments = content.get('attachments', [])
            if not attachments:
                return None
            attachment = attachments[0]
            styles = attachment.get("styles", {})
            attachment_inner = styles.get("attachment", {})

            subattachments = attachment_inner.get("all_subattachments", {}).get("nodes", [])
            if subattachments:
                media = subattachments[0].get("media", {})
                image = media.get("image", {})
                if "uri" in image:
                    return image["uri"]

            media = attachment_inner.get("media", {})
            placeholder = media.get("placeholder_image", {})
            if "uri" in placeholder:
                return placeholder["uri"]

            if "playable_url" in media:
                return media["playable_url"]
            
            if "thumbnailImage" in media:
                return media['thumbnailImage']['uri']
            
            if "style_infos" in attachment_inner:
                reel = attachment_inner['style_infos'][0]['fb_shorts_story']['attachments'][0]
                return reel['media']['thumbnailImage']['uri']
            
            if "large_share_image" in media:
                return media['large_share_image']['uri']
        except Exception as e:
            print(f"[get_pic error] {e}")
        return None
    
    def add_tag_attrs(self, el):
        el['tag'] = self.tag.get('title')
        el['tag_id'] = self.tag.get('tag_id')
        el['target_country']= self.tag.get('target_country')
        return el