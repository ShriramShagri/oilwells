# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re
from .constants import *
from .essentials import *

class ProductionPipeline():
    def process_item(self, item, spider):
        print('In')

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
            try:
                ipRawData = [i.replace('\n', "") for i in item['ip']]
                ipFilteredData_temp = []

                for clms in range(len(IPCOLUMNS)-1):
                    if checklist(IPCOLUMNS[clms], IPCOLUMNS[clms + 1], ipRawData):
                        ipFilteredData_temp.append(ipRawData[ipRawData.index(IPCOLUMNS[clms]) + 1])
                    else:
                        ipFilteredData_temp.append("") 
                
                ipFilteredData = list(essentials)
                ipFilteredData.extend(ipFilteredData_temp)
            
            except Exception as err:
                self.error(essentials[0], essentials[1], str(err), 'pf')
                            
            else:
                # Add to table :)
                self.store_ip(ipFilteredData, essentials)

        # if casing table is present, pass proper param

        if 'casing' in keysInItem:
            try:
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
            except Exception as err:
                self.error(essentials[0], essentials[1], str(err), 'casing')
                            
            else:
                # Add to table :)
                if len(casingFilteredData) > 0:
                    self.store_casing(casingFilteredData, essentials)

        # if perforation table is present, pass proper param
        if 'pf' in keysInItem:
            try:
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
                    if len(pfFilteredData) > 0:
                        toRemove = []
                        for row in pfFilteredData:
                            if row.count("") >= 3:
                                toRemove.append(row)
                        if len(toRemove) > 0:
                            for items in toRemove:
                                pfFilteredData.pop(pfFilteredData.index(items))
            
            # Add to table :)
                    if pfFilteredData:
                        self.store_pf(pfFilteredData, essentials, arrlen)
            except Exception as err:
                self.error(essentials[0], essentials[1], str(err), 'pf')

        # if cuttings table is present, pass proper param

        if 'cutting' in keysInItem:
            # Raw html to datalist
            try:
                remove = ['<table border="1" align="center">', '</table>', '\n', '<br>', '</td>', '</tr>', '<td>']
                rep = {i : '' for i in remove}
                rep = dict((re.escape(k), v) for k, v in rep.items()) 
                pattern = re.compile("|".join(rep.keys()))
                cuttingRawData = list(filter(None, pattern.sub(lambda m: rep[re.escape(m.group(0))], item['cutting'][-1]).split('<tr>')))
                cuttingFilteredData = []
                for index, i in enumerate(cuttingRawData):
                    cuttingFilteredData.append(list())
                    cuttingFilteredData[index].extend(essentials)
                    temp = list(filter(None, i.split('<strong>')))
                    for j in temp:
                        dataPair = j.split('</strong>')
                        if 'Confidential until: ' not in dataPair[0]:
                            cuttingFilteredData[index].append(dataPair[-1])
                    if len(temp) == 3:
                        cuttingFilteredData[index].append('')
            except Exception as err:
                self.error(essentials[0], essentials[1], str(err), 'cutting')
            # Add to table :)
            else:
                self.store_cutting(cuttingFilteredData, essentials)
        
        if 'tops' in keysInItem:
            try:
                remove = ['<table border="1" align="center">', '</table>', '\n', '</tr>', '</td>', '\xa0']
                rep = {i : '' for i in remove}
                rep = dict((re.escape(k), v) for k, v in rep.items()) 
                pattern = re.compile("|".join(rep.keys()))
                topsRawData = list(filter(None, pattern.sub(lambda m: rep[re.escape(m.group(0))], item['tops']).split('</th>')[-1].split('<tr>')))
                topsFilteredData = []
                for index, i in enumerate(topsRawData):
                    topsFilteredData.append([])
                    topsFilteredData[index].extend(essentials)
                    topsFilteredData[index].extend(i.split('<td>')[1:])
            except Exception as err:
                self.error(essentials[0], essentials[1], str(err), 'tops')
            
            else:
                self.store_tops(topsFilteredData, essentials)
        
        if 'dst' in keysInItem:
            try:
                crudeData = []
                cleanData = []
                track = -1
                for i in item['dst']:
                    if 'Test Number:' in i:
                        crudeData.append([])
                        track += 1
                    if track >= 0:
                        crudeData[track].append(i)

                
                for html in crudeData:
                    try:
                        temparr = []
                        temparr.extend(essentials)
                        # TestNumber
                        temparr.append(html[0].replace('\n', '').replace('</td>', '').replace('<td width="50%"><b>Test Number:</b> ', ''))

                        # Data Source
                        temparr.append(html[1].replace('\n', '').replace('</td>', '').replace('<td width="50%"><b>Data Source:</b> ', ''))

                        # Interval, FormationTested
                        temp = html[2].replace('\n', '').replace('</td>', '').split('<br>')
                        if len(temp) == 2:
                            temparr.append(temp[0].replace('<td>Interval: ', ''))
                            temparr.append(temp[1].replace('Formation tested: ', ''))
                        elif len(temp) == 1:
                            if '<td>Interval: ' in temp:
                                temparr.extend([temp[0].replace('<td>Interval: ', ''), ''])
                            elif 'Formation tested: ' in temp:
                                temparr.extend(['', temp[0].replace('Formation tested: ', '')])
                            else:
                                temparr.extend(['', ''])
                        else:
                            temparr.extend(['', ''])

                        # Datetime
                        if 'Date, Time:' in html[3]:
                            temparr.append(html[3].replace('\n', '').replace('</td>', '').replace('<td>Date, Time: ', ''))
                        else:
                            temparr.append('')

                        # Main data set 1
                        temp = html[4].replace('\n', '').replace('</td>', '').split('<br>')
                        if len(temp) == len(MAIN_SET1):
                            extracted = []
                            for crude, junk in zip(temp, MAIN_SET1):
                                extracted.append(crude.replace(junk, ''))
                            temparr.extend(extracted)
                        else:
                            temparr.extend(['' for _ in range(MAIN_SET1)])

                        # Tool Data
                        temp = html[5].replace('\n', '').replace('</td>', '').split('<br>')
                        temp.pop(0)
                        if len(temp) == len(MAIN_SET2):
                            extracted = []
                            for crude, junk in zip(temp, MAIN_SET2):
                                extracted.append(crude.replace(junk, ''))
                            temparr.extend(extracted)
                        else:
                            temparr.extend(['' for _ in range(MAIN_SET2)])

                        # Initial Flow
                        if 'Bottom Hole Temperature' in html[6]:
                            temparr.append('')
                            html.insert(6, '')
                        else:
                            temparr.append(';'.join(list(filter(None, html[6].replace('\n', '').replace('<td colspan="2">','').replace('</td>', '').split('<br>')))))

                        # Bottom Hole Temperature
                        if 'Bottom Hole Temperature' in html[6]:
                            temp = html[7].replace('\n', '').replace('</td>', '').split('<br>')
                            extracted = []
                            for crude, junk in zip(temp, MAIN_SET4):
                                extracted.append(crude.replace(junk, ''))
                            temparr.extend(extracted)
                        else:
                            temparr.extend(['' for _ in range(len(MAIN_SET4))])

                        # Recovery
                        if 'Recovery' in html[8]:
                            temparr.append(';'.join(list(filter(None, html[8].replace('\n', '').replace('</td>', '').replace('<td colspan="2"><b>Recovery</b><br>', '').split('<br>')))))
                        else:
                            temparr.append('')
                        cleanData.append(temparr)
                    except Exception as err:
                        self.error(essentials[0], essentials[1], str(err), 'dst')
                        continue
            except Exception as err:
                self.error(essentials[0], essentials[1], str(err), 'dst')
            else:
                self.store_dst(cleanData, essentials)
        
        if 'production' in keysInItem:
            datacol1 = item['production'][0]
            filtereddata = []
            for i in datacol1:
                if len(i) < 150 or 'https://chasm.kgs.ku.edu/ords/oil.ogl5.SelectLeases?' not in i:
                    i = i[80:-6].replace('</a>', '').replace('<br>', '').replace('\n', ' ')
                    filtereddata.append([i,])
            
            datacol2 = item['production'][1]
            for index, i in enumerate(datacol2):
                filtereddata[index].append(i.split('\n')[1])
            
            datacol3 = item['production'][2]
            for index, i in enumerate(datacol3):
                filtereddata[index].append(i)

            datacol4 = item['production'][3]
            finaldata = []
            for index, i in enumerate(datacol4[1:]):
                i = i.replace('\n', '').replace('<td>', '').replace("</td>", '').split('<br>')
                row = filtereddata[index]
                for j in i:
                    if len(j) > 94:
                        j = j[89:-4]
                    finaldata.append(row+[j])
            print(len(finaldata))
            self.store_production(finaldata)
        return item
    

    def replacer(self, text, remove, s=None):
        rep = {i : '' for i in remove}
        rep = dict((re.escape(k), v) for k, v in rep.items()) 
        pattern = re.compile("|".join(rep.keys()))
        if s:
            return list(filter(None, pattern.sub(lambda m: rep[re.escape(m.group(0))], text).split(s)))
        else:
            return pattern.sub(lambda m: rep[re.escape(m.group(0))], text)

    def store_production(self, item):
        '''
        Store production to table
        '''
        try:
            args_str = b','.join(DATABASE.cur.mogrify("(%s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO production VALUES " + args_str)
        except Exception as e:
            DATABASE.conn.rollback()
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, ('NA', 'NA', str(e), "WH"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()

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
    
    def store_tops(self, item, ess):
        '''
        Store casing to table
        '''
        try:
            args_str = b','.join(
                DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode(
                "utf-8")
            DATABASE.cur.execute("INSERT INTO tops VALUES " + args_str)
        except Exception as e:
            api, kid = ess
            DATABASE.conn.rollback()
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "Tops"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()
    
    def store_dst(self, item, ess):
        '''
        Store casing to table
        '''
        try:
            args_str = b','.join(
                DATABASE.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
            DATABASE.cur.execute("INSERT INTO dst VALUES " + args_str)
        except Exception as e:
            DATABASE.conn.rollback()
            api, kid = ess
            sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
            DATABASE.cur.execute(sql, (api, kid, str(e), "dst"))
            DATABASE.conn.commit()
        else:
            DATABASE.conn.commit()
    
    def error(self, api, kid, e, table):
        sql = "INSERT INTO errors VALUES (%s, %s, %s, %s)"
        DATABASE.cur.execute(sql, (api, kid, e, table))
        DATABASE.conn.commit()
