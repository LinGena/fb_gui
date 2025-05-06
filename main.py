import pyautogui
import random
from parser.devtools_btn import ClickButtons
from postgres_db.words import get_words_to_parse, get_tracked_accounts
from parser.get_har_content import GetHarContent


class Facebook(ClickButtons):
    def __init__(self):
        super().__init__()
        pyautogui.PAUSE = 1

    def run(self):
        self.fill_filter_devtools()
        while True:
            try:
                task = self.get_task()
                self.posts_count = task['task']['posts_count']
                if not task:
                    print('No task')
                    pyautogui.sleep(60)
                    continue
                url = self.get_url(task)
                self.press_clear_btn()
                self.open_url_in_browser(url)
                self.run_scan() 
                self.get_har_file()
                if task['type_task'] == 'keywords': 
                    GetHarContent(task['task']).run_keywords()
                elif task['type_task'] == 'tracked_accounts':
                    GetHarContent(task['task']).run_tracked_accounts()
            except Exception as ex:
                self.exception_while(ex)  

    def get_url(self, task: dict) -> str:
        if task['type_task'] == 'keywords':
            search_tag = str(task['task']['title']).replace('"', '').strip()
            url = f'https://www.facebook.com/search/posts?q={search_tag}"'
            if task['task']['parsing_style'] == "recent":
                recent_filter = '&filters=eyJyZWNlbnRfcG9zdHM6MCI6IntcIm5hbWVcIjpcInJlY2VudF9wb3N0c1wiLFwiYXJnc1wiOlwiXCJ9In0%3D'
                url += recent_filter
            return url
        elif task['type_task'] == 'tracked_accounts':
            url = task['task']['link']
            if 'facebook' not in url:
                raise Exception('[info] It is not a facebook link')
            return url
        raise Exception('[info] No such type_task')

    def get_task(self, reget_task: int = 0) -> dict:
        task = None
        try:
            if reget_task > 0:
                if reget_task == 1:
                    type_task = 2
                else:
                    type_task = 1
            else:
                type_task = random.randint(1,2)
            if type_task == 1:
                words = get_words_to_parse()
                if words:
                    task = {'type_task':'keywords', 'task':words[0]}
            elif type_task == 2:
                tracked_accounts = get_tracked_accounts()
                if tracked_accounts:
                    task = {'type_task':'tracked_accounts', 'task':tracked_accounts[0]}
            if not task and reget_task == 0:
                return self.get_task(type_task)
        except Exception as ex:
            print(f"[get_task]: {ex}")
        return task

    def exception_while(self, ex):
        if 'critical' in str(ex):
            btn = self.is_image_on_screen('img/reload.png')
            if not btn:
                raise Exception(ex)
            self._click(btn.x, btn.y)
            self.count_try_reload += 1
            if self.count_try_reload >= 2:
                raise Exception(f'[count_try_reload = 2] {ex}')
        elif '[info]' in str(ex):
            print(ex)
        else:
            raise Exception(ex)

    def run_scan(self):
        pyautogui.sleep(2)
        pyautogui.moveTo(x=660, y=485, duration=0.5)
        count_end = 0
        for i in range(self.posts_count):
            previous_screenshot = pyautogui.screenshot()
            pyautogui.hscroll(-1000)
            current_screenshot = pyautogui.screenshot()
            if current_screenshot == previous_screenshot:
                count_end += 1
                if count_end > 4:
                    self.end_page = True
                    break
            else:
                count_end = 0


if __name__ == '__main__':
    Facebook().run()