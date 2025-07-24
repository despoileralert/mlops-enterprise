from infrastructure.mysql_manager import *
from utils.config_loader import *
import yaml


def func1():
    c = ConfigLoader()
    s = MySQLManager(c.get_database_config())
    test = s.get_pool_status()
    return test

print(func1())    