def get_sybil_set():
    input_file = '../source/sybil_address'

    lines_set = set()

    with open(input_file, 'r') as file:
        for line in file:
            #  strip() 
            lines_set.add(line.strip())
    return lines_set


def check_exist(address):
    sybil_set = get_sybil_set()
    if address in sybil_set:
        return True
    else:
        return False


