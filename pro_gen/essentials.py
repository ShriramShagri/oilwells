DOWNLOAD_CHECK = []

SOURCES = 'Digital Wellbore information for this horizontal well is available.'
OIL = ['This well has been linked to an oil lease.', 'This well has been linked to a gas lease.']

# names of pdf to be downloaded



TODOWNLOAD = (
    'Intent To Drill Well', 'Well Completion Report', 'Drill Stem Test', 'DST Report', 'Directional Drilling Report',
    'Well Map', 'Download scan(s) (ZIP archive of TIF images)')

# Columns for ip table to be stored(Change db columns if added or removed any)

IPCOLUMNS = ('Producing Method: ', '\xa0\xa0\xa0\xa0Oil: ', '\xa0\xa0\xa0\xa0Water: ', '\xa0\xa0\xa0\xa0Gas: ',
             'Disposition of Gas: ', '\xa0\xa0\xa0\xa0Size: ',
             '\xa0\xa0\xa0\xa0Set at: ', '\xa0\xa0\xa0\xa0Packer at: ', 'Production intervals: ',
             "THIS SHOULD BE THERE")

# Columns for wh table to be stored(Change db columns if added or removed any)

WHCOLUMS = (
    'API: ', 'KID: ', 'Lease:', 'Well:', 'Original operator:', 'Current operator:', 'Field:', 'Location1', 'Location2',
    'Location3', 'NAD27 Longitude: ',
    'NAD27 Latitude: ', 'NAD83 Longitude: ', 'NAD83 Latitude: ', 'County: ', 'Permit Date: ', 'Spud Date: ',
    'Completion Date: ', 'Plugging Date: ', 'Well Type: ',
    'Status: ', 'Total Depth: ', 'Elevation: ', 'Producing Formation: ', 'IP Oil (bbl): ', 'IP Water (bbl): ',
    'IP GAS (MCF): ', 'KDOR code for Oil:',
    'KCC Permit No.: ', "THIS SHOULD BE THERE")

DST_Extensions = ('zip', 'pdf')

# DST Table Cleaners:
# Dont Change the below
MAIN_SET1 = (
    '<td><b>Initial Hydro Pressure</b>: ',
    '<b>First Initial Flow Pressure</b>: ',
    '<b>First Final Flow Pressure</b>: ',
    '<b>Initial Shut-in Pressure</b>: ',
    '<b>Second Initial Flow Pressure</b>: ',
    '<b>Second Final Flow Pressure</b>: ',
    '<b>Final Shut-in Pressure</b>: ',
    '<b>Final Hydro Pressure</b>: '
)

MAIN_SET2 = (
    'Hole Size: ',
    'Drill Collar ID: ',
    'Drill Pipe ID: ',
    'Drill Collar Length: ',
    'Drill Pipe Length: ',
)

MAIN_SET4 = (
    '<td colspan="2">Bottom Hole Temperature: ',
    'Percent Porosity: ',
    'Samples: ',
    'Gas Recovery: ',
    'Comments: '
)

DOWNLOAD_CHECK.append(SOURCES)
DOWNLOAD_CHECK.extend(OIL)
DOWNLOAD_CHECK.extend(TODOWNLOAD)
