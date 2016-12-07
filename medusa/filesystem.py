import simplejson as json


def update_json_file(path, step):
    """
    update json file data. The key is the json field, and the value
    is the new content.
    """
    content = read_data(path)
    content = json.loads(content[0])
    content["step"] = step

    write_data(path, json.dumps(content))


def write_data(path, line):
    with open(path, 'w') as out_file:
        out_file.write(line)


def append_data(path, line):
    with open(path, 'a') as out_file:
        out_file.write(line)


def read_data(path):
    with open(path, 'r') as in_file:
        content = in_file.readlines()

    return content


def read_data_oneline(path):
    with open(path, 'r') as in_file:
        content = in_file.readlines()

    return "".join(content)
