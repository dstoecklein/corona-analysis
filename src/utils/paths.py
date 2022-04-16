import os


def get_population_path():
    path = "files/population/"
    if not os.path.exists(path):
        path = "../files/population/"
    if not os.path.exists(path):
        path = "../../files/population/"
    if not os.path.exists(path):
        path = "../../../files/population/"
    if not os.path.exists(path):
        path = "../../../../files/population/"
    return path


def get_covid_path():
    path = "files/covid/"
    if not os.path.exists(path):
        path = "../files/covid/"
    if not os.path.exists(path):
        path = "../../files/covid/"
    if not os.path.exists(path):
        path = "../../../files/covid/"
    if not os.path.exists(path):
        path = "../../../../files/covid/"
    return path


def get_hospitals_path():
    path = "files/hospitals/"
    if not os.path.exists(path):
        path = "../files/hospitals/"
    if not os.path.exists(path):
        path = "../../files/hospitals/"
    if not os.path.exists(path):
        path = "../../../files/hospitals/"
    if not os.path.exists(path):
        path = "../../../../files/hospitals/"
    return path


def get_tests_path():
    path = "files/tests/"
    if not os.path.exists(path):
        path = "../files/tests/"
    if not os.path.exists(path):
        path = "../../files/tests/"
    if not os.path.exists(path):
        path = "../../../files/tests/"
    if not os.path.exists(path):
        path = "../../../../files/tests/"
    return path


def get_vaccinations_path():
    path = "files/vaccinations/"
    if not os.path.exists(path):
        path = "../files/vaccinations/"
    if not os.path.exists(path):
        path = "../../files/vaccinations/"
    if not os.path.exists(path):
        path = "../../../files/vaccinations/"
    if not os.path.exists(path):
        path = "../../../../files/vaccinations/"
    return path


def get_mortality_path():
    path = "files/mortality/"
    if not os.path.exists(path):
        path = "../files/mortality/"
    if not os.path.exists(path):
        path = "../../files/mortality/"
    if not os.path.exists(path):
        path = "../../../files/mortality/"
    if not os.path.exists(path):
        path = "../../../../files/mortality/"
    return path


def get_utils_path():
    path = "files/various/"
    if not os.path.exists(path):
        path = "../files/various/"
    if not os.path.exists(path):
        path = "../../files/various/"

    return path
