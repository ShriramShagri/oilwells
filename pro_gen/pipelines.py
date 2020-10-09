# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .constants import *
import csv
import os


class ProGenPipeline(db):
    '''
    Handle Data Like Pro
    '''
    def process_item(self, item, spider):
        '''
        Check for all the data sent and handle them with care 
        '''
        def checklist(s1, s2, l):
            '''
            Just a repeated set of lines
            '''
            try:
                if l[l.index(s1)+1] != s2:
                    return True
                return False
            except:
                return False

        # if wh table is present, gather relevant data and add to table
        try:
            whRawData = [i.replace('\n', "") for i in item['wh']]
            whFilteredData = []
            for clms in range(len(WHCOLUMS)-1):
                if(WHCOLUMS[clms] == 'Location1'):
                    if checklist('Location: ', 'NAD27 Longitude: ', whRawData):
                        whFilteredData.append(whRawData[whRawData.index('Location: ') + 1])
                    else:
                        whFilteredData.append("") 
                elif(WHCOLUMS[clms] == 'Location2'):
                    if checklist('Location: ', 'NAD27 Longitude: ', whRawData):
                        whFilteredData.append(whRawData[whRawData.index('Location: ') + 2])
                    else:
                        whFilteredData.append("") 
                elif(WHCOLUMS[clms] == 'Location3'):
                    if checklist('Location: ', 'NAD27 Longitude: ', whRawData):
                        whFilteredData.append(whRawData[whRawData.index('Location: ') + 3])
                    else:
                        whFilteredData.append("") 
                elif checklist(WHCOLUMS[clms], WHCOLUMS[clms + 1], whRawData):
                    whFilteredData.append(whRawData[whRawData.index(WHCOLUMS[clms]) + 1])
                else:
                    whFilteredData.append("") 

            # Add to table :)

            self.store_wh(whFilteredData)
            essentials = [whFilteredData[0], whFilteredData[1]]
        except :
            essentials = [item['api'], item['kid']]
        


         # if casing table is present, pass proper param

        try:
            if item['ip']:
                ipRawData = [i.replace('\n', "") for i in item['ip']]
                ipFilteredData_temp = []

                for clms in range(len(IPCOLUMNS)-1):
                    if checklist(IPCOLUMNS[clms], IPCOLUMNS[clms + 1], ipRawData):
                        ipFilteredData_temp.append(ipRawData[ipRawData.index(IPCOLUMNS[clms]) + 1])
                    else:
                        ipFilteredData_temp.append("") 
                
                ipFilteredData = list(essentials)
                ipFilteredData.extend(ipFilteredData_temp)

                # Add to table :)

                self.store_ip(ipFilteredData)
        except:
            pass

        # if casing table is present, pass proper param

        try:
            if item['casing']:
                casingRawData = item['casing'][item['casing'].index('Additives') + 3:]
                casingFilteredData = []

                # Let's hope the casing data repeats itself after 17 elements 
                for i in range(len(casingRawData) // 17):
                    temp = list()
                    temp.extend(essentials)
                    temp.extend(casingRawData[i * 17 + 1:(i + 1) * 17:2])
                    casingFilteredData.append(temp)
                
                # Add to table :)

                self.store_casing(casingFilteredData)

        except:
            pass

        # if perforation table is present, pass proper param

        try:
            if item['pf']:
                pfRawData = item['pf'][item['pf'].index('Depth') + 3:]
                pfRawData = [i.replace('\n', "") for i in pfRawData]
                pfFilteredData = []

                for i in range(len(pfRawData) // 9):
                    temp = list()
                    temp.extend(essentials)
                    temp.extend(pfRawData[i * 9 + 1:(i + 1) * 9:2])
                    pfFilteredData.append(temp)
                
                # Remove empty rows
                toRemove = []
                for row in pfFilteredData:
                    if row.count("") >= 3:
                        toRemove.append(row)
                for items in toRemove:
                    pfFilteredData.remove(pfFilteredData.index(items))
                
                # Add to table :)

                self.store_pf(pfFilteredData)

        except:
            pass

        # if cuttings table is present, pass proper param

        try:
            if item['cutting']:
                # I can't explain these :p
                cuttingRawData = []
                cuttingRawDatatemp = item['cutting'][2:]
                cuttingRawDatatemp = [i.replace('\n', "") for i in cuttingRawDatatemp]
                filterString = '$_%&^%'.join(cuttingRawDatatemp)
                filterList = filter(None, filterString.split("Box Number: "))
                for sts in filterList:
                    cuttingRawData.append(list(filter(None, sts.split('$_%&^%'))))
                cuttingFilteredData = []

                for j in cuttingRawData:
                    temp = list()
                    temp.extend(essentials)
                    temp.extend(j[::2])
                    cuttingFilteredData.append(temp)

                # Add to table :)

                self.store_cutting(cuttingFilteredData)

        except:
            pass

        return item

    def store_wh(self, item):
        '''
        Store WH to table
        '''
        try:
            DATABASE.cur.execute(
                "INSERT INTO WH VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                tuple(item))
        except:
            DATABASE.conn.rollback()
            try:
                DATABASE.cur.execute(
                    "INSERT INTO WH VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    tuple(item))
            except:
                DATABASE.conn.rollback()
        else:
            DATABASE.conn.commit()

    def store_ip(self, item):
        '''
        Store IP to table
        '''
        try:
            DATABASE.cur.execute(
                "INSERT INTO IP VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                tuple(item))
        except:
            DATABASE.conn.rollback()
            try:
                DATABASE.cur.execute(
                    "INSERT INTO IP VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    tuple(item))
            except:
                DATABASE.conn.rollback()
        else:
            DATABASE.conn.commit()

    def store_cutting(self, item):
        '''
        Store Cuttings to table
        '''
        # Check if 'skips' is present
        newitem = []
        for t in item:
            while len(t) < 6:
                t.append('')
            newitem.append(t)

        try:
            args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in newitem).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO cutting VALUES " + args_str)
        except:
            DATABASE.conn.rollback()
            try:
                args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in newitem).decode("utf-8")
                DATABASE.cur.execute("INSERT INTO cutting VALUES " + args_str)
            except:
                DATABASE.conn.rollback()
        else:
            DATABASE.conn.commit()

    def store_casing(self, item):
        '''
        Store casing to table
        '''
        try:
            args_str = b','.join(
                DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO casing VALUES " + args_str)
        except:
            DATABASE.conn.rollback()
            try:
                args_str = b','.join(
                    DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
                DATABASE.cur.execute("INSERT INTO casing VALUES " + args_str)
            except:
                DATABASE.conn.rollback()
        else:
            DATABASE.conn.commit()

    def store_pf(self, item):
        '''
        Store Perforation to table
        '''
        try:
            args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO Perforation VALUES " + args_str)
        except:
            DATABASE.conn.rollback()
            try:
                args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
                DATABASE.cur.execute("INSERT INTO Perforation VALUES " + args_str)
            except:
                DATABASE.conn.rollback()
        else:
            DATABASE.conn.commit()
