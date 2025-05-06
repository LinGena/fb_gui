import pyautogui
import platform
import pyperclip


class ClickButtons():

    def fill_filter_devtools(self):
        pyautogui.sleep(5)
        if not self.is_image_on_screen('img/fetch_on.png'):
            btn = self.is_image_on_screen('img/fetch_off.png')
            if not btn:
                self.press_clear_btn()
                print('Откройте страницу facebook и DevTools на вкладке Networks')
                raise Exception('critical')
            self._click(btn.x, btn.y)
        btn = self.is_image_on_screen('img/filter_off.png')
        if btn:
            self._click(btn.x, btn.y)
            pyautogui.typewrite('graphql/', interval=0.1)

    def press_clear_btn(self, count_try: int = 0):
        btn = self.is_image_on_screen('img/clear.png')
        if btn:
            self._click(btn.x, btn.y)
            pyautogui.moveTo(btn.x - 60, btn.y, duration=0.5)
            return
        if not self.is_reconnect_devtools():
            if count_try > 3:
                print('Не вижу кнопку очистки! Откройте Dev Tools на вкладке Networks')
                raise Exception('Something wrong with DevTools')
            return self.press_clear_btn(count_try+1)
        if count_try > 3:
            raise Exception('Here and what ?')
        return self.press_clear_btn(count_try+1)
    
    def is_image_on_screen(self, img_name) -> bool:
        try:
            point = pyautogui.locateCenterOnScreen(img_name, confidence=0.8)
            if point is None:
                return False
            if platform.system() == 'Darwin':
                point = pyautogui.Point(point.x / 2, point.y / 2)
            return point
        except pyautogui.ImageNotFoundException:
            return False
        
    def is_reconnect_devtools(self) -> bool:
        btn = self.is_image_on_screen('img/reconnect.png')
        if btn:
            self._click(btn.x, btn.y)
            return True
        return False
    
    def _click(self, x, y):
        pyautogui.moveTo(x, y, duration=0.5)
        pyautogui.leftClick()

    def open_url_in_browser(self, url):
        pyautogui.sleep(5)
        btn = self.is_image_on_screen('img/reload.png')
        self._click(btn.x + 100, btn.y)
        pyautogui.sleep(0.5)
        pyautogui.hotkey('command' if platform.system() == 'Darwin' else 'ctrl', 'a')
        pyautogui.press('del')
        pyautogui.sleep(0.5)
        pyperclip.copy(url)
        pyautogui.hotkey('command' if platform.system() == 'Darwin' else 'ctrl', 'v')
        pyautogui.press('enter')

    def is_btn_appeared(self, img_filename: str, btn_name: str, retries: int = 10, wait_time: int = 5):
        for attempt in range(retries):
            btn = self.is_image_on_screen(img_filename)
            if not btn:
                if attempt < retries - 1:
                    pyautogui.sleep(wait_time)
                else:
                    print(f'Кнопка {btn_name} не появилась.')
                    raise Exception('critical')
            else:
                return btn
            
    def get_har_file(self):
        pyautogui.sleep(1)
        btn = self.is_image_on_screen('img/download_har.png')
        if not btn:
            print('Откройте Dev Tools на вкладке Networks')
            raise Exception('critical')
        self._click(btn.x, btn.y)

        btn = self.is_btn_appeared('img/save.png', 'Save')
        self._click(btn.x, btn.y)

        btn = self.is_btn_appeared('img/yes.png', 'Yes', 5, 2)
        self._click(btn.x, btn.y)