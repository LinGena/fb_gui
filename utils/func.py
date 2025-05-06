import json
import os
import time

def _get_filename(filename) -> str:
    dir_name = 'static'
    os.makedirs(dir_name, exist_ok=True)
    return dir_name + '/' + filename

def write_to_file(filename, src, in_dir:bool=True):
    if in_dir:
        filename = _get_filename(filename)
    with open(filename,"w", encoding='utf8') as file:
        file.write(src)

def load_file(filename, in_dir: bool=True, retries: int = 10, wait_time: int = 2):
    if in_dir:
        filename = _get_filename(filename)

    for attempt in range(retries):
        try:
            with open(filename, "r", encoding='utf8') as file:
                src = file.read()
            return src
        except PermissionError:
            if attempt < retries - 1:
                time.sleep(wait_time)  # Ждем перед повторной попыткой
            else:
                raise Exception('File did not load')

def write_to_file_json(filename, src, in_dir:bool=True):
    if in_dir:
        filename = _get_filename(filename)
    with open(filename,"w",encoding='utf8') as file:
        json.dump(src, file, indent=4, ensure_ascii=False)

def load_from_file_json(filename, in_dir:bool=True):
    if in_dir:
        filename = _get_filename(filename)
    with open(filename, encoding='utf8') as file:
        src=json.load(file)
    return src

def func_chunk_array(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]