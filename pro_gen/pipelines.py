# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .constants import *

class DSTPipeline():
    '''
    Handle Data Like Pro
    '''
    def process_item(self, item, spider):
        '''
        Check for all the data sent and handle them with care 
        '''
        crudeData = []
        cleanData = []
        for i in range(len(item['table'])// 13):
            crudeData.append(item['table'][i*13:(i+1)*13])
        
        for html in crudeData:
            temparr = [item['kid'],]
            # TestNumber
            temparr.append(html[0].replace('\n', '').replace('</td>', '').replace('<td width="50%"><b>Test Number:</b> ', ''))
            # Data Source
            temparr.append(html[1].replace('\n', '').replace('</td>', '').replace('<td width="50%"><b>Data Source:</b> ', ''))
            # Interval, FormationTested
            temp = html[2].replace('\n', '').replace('</td>', '').split('<br>')
            temparr.append(temp[0].replace('<td>Interval: ', ''))
            temparr.append(temp[1].replace('Formation tested: ', ''))
            # Datetime
            temparr.append(html[3].replace('\n', '').replace('</td>', '').replace('<td>Date, Time: ', ''))
            # Main data set 1
            temp = html[4].replace('\n', '').replace('</td>', '').split('<br>')
            extracted = []
            for crude, junk in zip(temp, MAIN_SET1):
                extracted.append(crude.replace(junk, ''))
            temparr.extend(extracted)

            # Tool Data
            temp = html[5].replace('\n', '').replace('</td>', '').split('<br>')
            temp.pop(0)
            extracted = []
            for crude, junk in zip(temp, MAIN_SET2):
                extracted.append(crude.replace(junk, ''))
            temparr.extend(extracted)

            # Initial Flow
            temp = html[6].replace('\n', '').replace('</td>', '').split('<br>')
            extracted = []
            for crude, junk in zip(temp, MAIN_SET3):
                extracted.append(crude.replace(junk, ''))
            temparr.extend(extracted)

            # Bottom Hole Temperature
            temp = html[7].replace('\n', '').replace('</td>', '').split('<br>')
            extracted = []
            for crude, junk in zip(temp, MAIN_SET4):
                extracted.append(crude.replace(junk, ''))
            temparr.extend(extracted)

            # Recovery
            temparr.append(';'.join(list(filter(None, html[8].replace('\n', '').replace('</td>', '').replace('<td colspan="2"><b>Recovery</b><br>', '').split('<br>')))))
            cleanData.append(temparr)
        
        self.store_dst(cleanData, item['kid'])

        return item
      
    def store_dst(self, item, kid):
        '''
        Store casing to table
        '''
        try:
            args_str = b','.join(
                DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO dst VALUES " + args_str)
        except Exception as e:
            DATABASE.conn.rollback()
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, ('', kid, str(e), "dst"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

class ProGenPipeline():
    '''
    Handle Data Like Pro
    '''
    def process_item(self, item, spider):
        '''
        Check for all the data sent and handle them with care 
        '''
        keysInItem = item.keys()
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
        if 'wh' in keysInItem:
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
        else:
            essentials = [item['api'], item['kid']]
        


         # if casing table is present, pass proper param

        if 'ip' in keysInItem:
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

            self.store_ip(ipFilteredData, essentials)

        # if casing table is present, pass proper param

        if 'casing' in keysInItem:
            casingRawData = item['casing'][item['casing'].index('Additives') + 3:]
            casingRawData = [i.replace('\n', "") for i in casingRawData]
            casingFilteredData = []

            # Let's hope the casing data repeats itself after 17 elements 
            offset = 0
            for i in range(len(casingRawData) // 17):
                temp = list()
                temp.extend(essentials)
                if casingRawData[i * 17 + 1 + offset] == '':
                    offset += 1
                if (i + 1) * 17 + offset <= len(casingRawData):
                    temp.extend(casingRawData[i * 17 + 1 + offset:(i + 1) * 17 + offset:2])
                    casingFilteredData.append(temp)
            
            # Add to table :)

            self.store_casing(casingFilteredData, essentials)

        # if perforation table is present, pass proper param
        if 'pf' in keysInItem:
            arrlen = 0
            if len(item['pf']) > 2:
                pfRawData = []
                for i in item['pf']:
                    pfRawData.append(i.replace('<td>', '').replace('</td>', ''))
                pfFilteredData = []
                if len(item['pfHeaders']) % 4 == 0:
                    arrlen = 4
                    for i in range(len(pfRawData) // 4):
                        temp = list()
                        temp.extend(essentials)
                        temp.extend(pfRawData[i * 4 :(i + 1) * 4])
                        pfFilteredData.append(temp)

                elif len(item['pfHeaders']) % 6 == 0:
                    arrlen = 6
                    for i in range(len(pfRawData) // 6):
                        temp = list()
                        temp.extend(essentials)
                        temp.extend(pfRawData[i * 6 :(i + 1) * 6])
                        pfFilteredData.append(temp)
        
        # Remove empty rows
                # if len(pfFilteredData) > 0:
                #     toRemove = []
                #     for row in pfFilteredData:
                #         if row.count("") >= 3:
                #             toRemove.append(row)
                #     if len(toRemove) > 0:
                #         for items in toRemove:
                #             pfFilteredData.remove(pfFilteredData.index(items))
        
        # Add to table :)
                self.store_pf(pfFilteredData, essentials, arrlen)

        # if cuttings table is present, pass proper param

        if 'cutting' in keysInItem:
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

            self.store_cutting(cuttingFilteredData, essentials)

        return item

    def store_wh(self, item):
        '''
        Store WH to table
        '''
        try:
            DATABASE.cur.execute(
                "INSERT INTO WH VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                tuple(item))
        except Exception as e:
            DATABASE.conn.rollback()
            api, kid = item[0], item[1]
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "WH"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

    def store_ip(self, item, ess):
        '''
        Store IP to table
        '''
        try:
            DATABASE.cur.execute(
            "INSERT INTO IP VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(item))
        except Exception as e:
            DATABASE.conn.rollback()
            api, kid = ess
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "IP"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

    def store_cutting(self, item, ess):
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
        except Exception as e:
            DATABASE.conn.rollback()
            api, kid = ess
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "cutting"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

    def store_casing(self, item, ess):
        '''
        Store casing to table
        '''
        try:
            args_str = b','.join(
                DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO casing VALUES " + args_str)
        except Exception as e:
            DATABASE.conn.rollback()
            api, kid = ess
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "casing"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

    def store_pf(self, item, ess, arrlen):
        '''
        Store Perforation to table
        '''
        try:
            if arrlen == 4:
                args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
                DATABASE.cur.execute("INSERT INTO Perforation VALUES " + args_str)
            elif arrlen == 6:
                args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
                DATABASE.cur.execute("INSERT INTO pf VALUES " + args_str)
        except Exception as e:
            DATABASE.conn.rollback()
            api, kid = ess
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), f"Perforation{arrlen}"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()
