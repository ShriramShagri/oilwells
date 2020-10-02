# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
from .constants import *


class ProGenPipeline:
    def __init__(self):
        self.conn = psycopg2.connect(
                            host=HOST,
                            database=DATABASE,
                            user=USER,
                            password=PASSWORD
                            )
        self.cur = self.conn.cursor()


    def process_item(self, item, spider):

    # if wh table is present, pass proper param

        whRawData = [i.replace('\n' ,"") for i in item['wh']]
        whFilteredData = []
        extra = False

        whFilteredData.append(whRawData[whRawData.index('API: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('KID: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Lease:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Well:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Original operator:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Current operator:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Field:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Location: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Location: ') + 2])
        if whRawData[whRawData.index('Location: ') + 2] != '':
            whFilteredData.append(whRawData[whRawData.index('Location: ') + 3])
            extra = True
        whFilteredData.append(whRawData[whRawData.index('NAD27 Longitude: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('NAD27 Latitude: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('NAD83 Longitude: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('NAD83 Latitude: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('County: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Permit Date: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Spud Date: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Completion Date: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Plugging Date: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Well Type: ') + 1])
        try:
            whFilteredData.append(whRawData[whRawData.index('Status: ') + 1])
        except:
            whFilteredData.append("")
        whFilteredData.append(whRawData[whRawData.index('Total Depth: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Elevation: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Producing Formation: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('IP Oil (bbl): ') + 1])
        whFilteredData.append(whRawData[whRawData.index('IP Water (bbl): ') + 1])
        whFilteredData.append(whRawData[whRawData.index('IP GAS (MCF): ') + 1])
        whFilteredData.append(whRawData[whRawData.index('KCC Permit No.: ') + 1])

        self.store_wh(whFilteredData, extra)
            

        essentials = [whFilteredData[0], whFilteredData[1]]


        # if casing table is present, pass proper param

        try:
            if 'Purpose of String' in item['casing']:
                casingRawData = item['casing'][item['casing'].index('Additives')+1:]
                casingFilteredData = []
                if len(casingRawData) % 7 == 0:
                    for i in range(len(casingRawData)//7):
                        casingFilteredData.append(essentials + casingRawData[i*7:(i+1)*7] + [""])
                    self.store_casing(casingFilteredData)
                elif len(casingRawData) % 8 == 0:
                    for i in range(len(casingRawData)//8):
                        casingFilteredData.append(essentials + casingRawData[i*8:(i+1)*8])
                    self.store_casing(casingFilteredData)
        except:
            pass
        

        try:
            if 'Perforation record' in item['pf']:
                pfRawData = item['pf'][item['pf'].index('Depth')+1:]
                pfFilteredData = []
                try:
                    checklist = [int(pfRawData[i*4])for i in range(len(pfRawData)//4)]
                    repeat = 4
                    print("Zone 4")
                    addition = []
                except:
                    repeat = 3
                    print("Zone 3")
                    addition = [""]

                for i in range(len(pfRawData)//repeat):
                    pfFilteredData.append(essentials + pfRawData[i*repeat:(i+1)*repeat] + addition)
                self.store_pf(pfFilteredData)

        except:
            pass

        return item


    def store_wh(self, item, extra):
        if extra:
            self.cur.execute("INSERT INTO WH VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" , tuple(item))
            self.conn.commit()
        else:
            sql = '''
            INSERT INTO WH 
            (API,
            KID,
            Lease,
            Well,
            Original_operator,
            Current_operator,
            Field,
            Location1,
            Location2,
            NAD27_Longitude,
            NAD27_Latitude,
            NAD83_Longitude,
            NAD83_Latitude,
            County,
            Permit_Date,
            Spud_Date,
            Completion_Date,
            Plugging_Date,
            Well_Type,
            Status,
            Total_Depth,
            Elevation,
            Producing_Formation,
            IP_Oil,
            IP_Water,
            IP_GAS,
            KCC_Permit_No)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            self.cur.execute(sql, tuple(item))
            self.conn.commit()
    
    def store_ip(self, item):
        sql = '''
        INSERT INTO IP
        (API,
        KID,
        Producing_Method,
        Oil,
        Water,
        Gas,
        Disposition of Gas,
        Size,
        Set_at,
        Packer_at,
        Production_intervals)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        self.cur.execute(sql, tuple(item))
        self.conn.commit()
    

    def store_cutting(self, item, kid):
        sql = '''
        INSERT INTO IP
        KID,
        Box_Number_1,
        Starting_Depth_1,
        Ending_Depth_1,
        Box_Number_2,
        Starting_Depth_2,
        Ending_Depth_2 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        self.cur.execute(sql, tuple(kid) + tuple(item))
        self.conn.commit()
    
    def store_casing(self, item):
        args_str = b','.join(self.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8") 
        self.cur.execute("INSERT INTO casing VALUES " + args_str) 
        self.conn.commit()
    
    def store_pf(self, item):
        args_str = b','.join(self.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8") 
        self.cur.execute("INSERT INTO Perforation VALUES " + args_str) 
        self.conn.commit()
