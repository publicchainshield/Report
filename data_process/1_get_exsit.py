import os
import re


def get_init_set():
    filename = 'result/initialList.txt'
    init_set = set()

    with open(filename, 'r') as file:
        for line in file:
            stripped_line = line.strip()
            if stripped_line:
                init_set.add(stripped_line)
    return init_set


def get_user_set():
    directory_path = 'result/user'
    matches = set()
    pattern = re.compile(r'0x[0-9A-Fa-f]+')
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='UTF-8') as file:
                for line in file:
                    found_matches = pattern.findall(line)
                    matches.update(found_matches)
    return matches


def get_user_link():
    directory_path = 'result/user_link'
    matches = set()
    pattern = re.compile(r'0x[0-9A-Fa-f]+')
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='UTF-8') as file:
                for line in file:
                    found_matches = pattern.findall(line)
                    matches.update(found_matches)
    return matches


def get_user_data():
    directory_path = 'result/user_data'
    matches = set()
    pattern = re.compile(r'0x[0-9A-Fa-f]+')
    for filename in os.listdir(directory_path):
        if filename.endswith('.txt') is False and filename.endswith('.md') is False and filename.endswith(
                '.csv') is False:
            continue
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='UTF-8') as file:
                for line in file:
                    found_matches = pattern.findall(line)
                    matches.update(found_matches)
    return matches

# user_data = get_user_data()
# init_set = get_init_set()
# user_set = get_user_set()
# user_link = get_user_link()
# print()

#
# union = user_data.union(init_set).union(user_set).union(user_link)
# with open('source/sybil_address', 'a') as file:
#     for item in union:
#         if len(item) == 42:
#             file.write(f"{item}\n")


# def check_exist(address):
#     if address in init_set or address in user_set or address in user_link:
#         return True
#     else:
#         return False


