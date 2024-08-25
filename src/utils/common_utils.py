import yaml
import os
import zipfile
import logging

class CustomHandler(logging.Handler):
    def __init__(self, cursor, conn):
        logging.Handler.__init__(self)
        self.cursor = cursor
        self.conn = conn

    def emit(self, record):
        sql = "INSERT INTO test_logs (LevelName, Message, DateCreated) VALUES (?, ?, GetDate())"
        self.cursor.execute(sql, (record.levelname, record.msg))
        self.conn.commit()
        
def clean_dir(dir_path:str)-> None:
    #Removes all the files in a directory. 
    if len(os.listdir(dir_path)) == 0:
        print(f'directory {dir_path} already empty')
    else:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
        print(f'directory {dir_path} cleaned')

def create_dir(path:str) -> None:
    if os.path.exists(path):
        print(f'directory {path} already exists')
    else:
        os.mkdir(path)
        print(f'directory {path} created')

def unzip_file(zip_filepath, extract_to):
    if not os.listdir(extract_to):
        #Folder Empty
        try:
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            print(f'Zip File Extracted to path {extract_to}')
        except:
            print(f'Zip File Could Not Be Extracted')
    else:
        #Folder not empty
        print('Directory was not empty. Deleted Directory')
        clean_dir(extract_to)
        create_dir(extract_to)
        try:
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            print(f'Zip File Extracted to path {extract_to}')
        except:
            print(f'Zip File Could Not Be Extracted')

def read_params(config_path:str) -> dict:
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config