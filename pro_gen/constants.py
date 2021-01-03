import psycopg2
import os


# Database Class
# Change the table values as you like
class DB:
    '''
    Connect to database here in constants.py so that db can be accessed inside spider too
    '''

    def __init__(self):
        self.conn = psycopg2.connect(
            host='localhost',
            database='oilwells',
            user='postgres',
            password='123456'
        )
        self.cur = self.conn.cursor()


# db object

DATABASE = DB()

# Set file storage path
STORAGE_PATH = os.path.join(os.getcwd(), 'docs')

# Add all county values to this tuple to be scraped
COUNTY = (3,)
# 62+5

# crawler name
CRAWLER_NAME = {
    'main': 'wells',
    'dst': 'dst',
    'wh': 'wh',
    'tops' : 'tops',
    'production' : 'production'
}

DOWNLOAD = {
    'pdf' : False,
    'oilProduction' : False,
    'sources' : True,
    'dst' : False
}
