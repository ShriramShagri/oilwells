import psycopg2

# Database Class
class db:
    '''
    Connect to database here in constants.py so that db can be accessed inside spider too
    '''
    def __init__(self):
        self.conn = psycopg2.connect(
            host=' localhost',
            database= 'oilwells',
            user= 'postgres',
            password= '123456'
        )
        self.cur = self.conn.cursor()

# db object

DATABASE = db()

# Add all county values to this tuple to be scraped
COUNTY = (3, 7, 11)

# names of pdf to be downloaded

TODOWNLOAD = ['Intent To Drill Well', 'Well Completion Report']

# Columns for ip table to be stored(Change db columns if added or removed any)

IPCOLUMNS = ['Producing Method: ', '\xa0\xa0\xa0\xa0Oil: ', '\xa0\xa0\xa0\xa0Water: ', '\xa0\xa0\xa0\xa0Gas: ', 'Disposition of Gas: ', '\xa0\xa0\xa0\xa0Size: ', 
            '\xa0\xa0\xa0\xa0Set at: ', '\xa0\xa0\xa0\xa0Packer at: ', 'Production intervals: ', "THIS SHOULD BE THERE"]


# Columns for wh table to be stored(Change db columns if added or removed any)

WHCOLUMS = ['API: ', 'KID: ', 'Lease:', 'Well:', 'Original operator:', 'Current operator:', 'Field:', 'Location1', 'Location2', 'Location3', 'NAD27 Longitude: ',
            'NAD27 Latitude: ', 'NAD83 Longitude: ', 'NAD83 Latitude: ', 'County: ', 'Permit Date: ', 'Spud Date: ', 'Completion Date: ', 'Plugging Date: ', 'Well Type: ', 
            'Status: ', 'Total Depth: ', 'Elevation: ', 'Producing Formation: ', 'IP Oil (bbl): ', 'IP Water (bbl): ', 'IP GAS (MCF): ', 'KDOR code for Oil:', 
            'KCC Permit No.: ', "THIS SHOULD BE THERE"]


# crawler name
CRAWLER_NAME = 'genx'