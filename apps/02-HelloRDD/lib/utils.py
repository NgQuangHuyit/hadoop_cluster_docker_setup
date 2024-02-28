from os import getcwd
from os.path import join


def get_local_dir(data_file):
    return f"file://{join(getcwd(),data_file)}"