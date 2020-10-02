# Database
HOST = 'localhost'
USER = 'postgres'
PASSWORD = '123456'
DATABASE = 'oilwells'

# Counties

COUNTY = (3, 7, 11)


TODOWNLOAD = ['Intent To Drill Well', 'Well Completion Report']

IPCOLUMNS = ['Producing Method: ', '\xa0\xa0\xa0\xa0Oil: ', '\xa0\xa0\xa0\xa0Water: ', '\xa0\xa0\xa0\xa0Gas: ', 'Disposition of Gas: ', '\xa0\xa0\xa0\xa0Size: ', 
            '\xa0\xa0\xa0\xa0Set at: ', '\xa0\xa0\xa0\xa0Packer at: ', 'Production intervals: ', "THIS SHOULD BE THERE"]

WHCOLUMS = ['API: ', 'KID: ', 'Lease:', 'Well:', 'Original operator:', 'Current operator:', 'Field:', 'Location1', 'Location2', 'Location3', 'NAD27 Longitude: ',
            'NAD27 Latitude: ', 'NAD83 Longitude: ', 'NAD83 Latitude: ', 'County: ', 'Permit Date: ', 'Spud Date: ', 'Completion Date: ', 'Plugging Date: ', 'Well Type: ', 
            'Status: ', 'Total Depth: ', 'Elevation: ', 'Producing Formation: ', 'IP Oil (bbl): ', 'IP Water (bbl): ', 'IP GAS (MCF): ', 'KDOR code for Oil:', 
            'KCC Permit No.: ', "THIS SHOULD BE THERE"]

# crawler name

CRAWLER_NAME = 'genx'