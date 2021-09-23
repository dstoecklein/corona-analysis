# Author: Daniel Stöcklein

import os


def get_population_path():
    path = '/files/population/'
    if not os.path.exists(path):
        path = '../files/population/'
    if not os.path.exists(path):
        path = '../../files/population/'

    return path


def get_covid19_ger_path():
    path = '/files/covid19/GER/'
    if not os.path.exists(path):
        path = '../files/covid19/GER/'
    if not os.path.exists(path):
        path = '../../files/covid19/GER/'

    return path


def get_utils_path():
    path = '/files/various/'
    if not os.path.exists(path):
        path = '../files/various/'
    if not os.path.exists(path):
        path = '../../files/various/'

    return path