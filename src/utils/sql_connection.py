from argparse import ArgumentParser
from utils.common_utils import read_params, CustomHandler
import pyodbc
import logging

def logs():
    args = ArgumentParser()
    args.add_argument("--config_path", "-c", default='params.yaml') 
    parsed_args = args.parse_args() # read the values of parameter/arguments passed in above line
    configs = read_params(parsed_args.config_path)

    server = configs['sql']['server']
    database = configs['sql']['database']
    username = configs['sql']['username']
    password = configs['sql']['password']
    driver = '{ODBC Driver 18 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(connection_string)
    
    cursor = conn.cursor()
    logger = logging.getLogger(database)
    logger.setLevel(logging.DEBUG)
    azure_handler = CustomHandler(cursor, conn)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    azure_handler.setFormatter(formatter)
    logger.addHandler(azure_handler)
    return logger

