import os
import re
import requests
from concurrent import futures


def download():
    pattern = re.compile(r'\]\((https://github[^\)]*)\)')
    matches = set()
    directory_path = 'result/user'
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='UTF-8') as file:
                for line in file:
                    found_matches = pattern.findall(line)
                    matches.update(found_matches)
    return matches


def process_url(url):
    response = requests.get(url)
    file_name = url.split('/')[-2] + '_' + url.split('/')[-1]
    with open(fr'result/user_data/{file_name}', 'wb') as file:
        file.write(response.content)
    print(url)


def process():
    urls = download()
    with futures.ThreadPoolExecutor() as executor:
        executor.map(process_url, urls)


def replace():
    directory_path = 'result/user'

    replace_string = '../user_data/'

    # pattern = re.compile(r'https://github.com/LayerZero-Labs/sybil-report/assets/([a-f0-9\-]+)/([a-f0-9\-]+)')
    pattern = re.compile(r'https://github.com/LayerZero-Labs/sybil-report/files/([^/]+)/([^/]+)')

    for filename in os.listdir(directory_path):
        print(filename)
        if filename.startswith('995'):
            print()
        file_path = os.path.join(directory_path, filename)

        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()

            new_content = re.sub(pattern, replace_string + r'\1_\2', content)

            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(new_content)
