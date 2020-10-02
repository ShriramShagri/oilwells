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

        whRawData = [i.replace('\n', "") for i in item['wh']]
        whFilteredData = []

        whFilteredData.append(whRawData[whRawData.index('API: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('KID: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Lease:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Well:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Original operator:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Current operator:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Field:') + 1])
        whFilteredData.append(whRawData[whRawData.index('Location: ') + 1])
        whFilteredData.append(whRawData[whRawData.index('Location: ') + 2])
        if whRawData[whRawData.index('Location: ') + 2] != 'NAD27 Longitude: ':
            whFilteredData.append(whRawData[whRawData.index('Location: ') + 3])
        else:
             whFilteredData.append("")
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
        try:
            whFilteredData.append(whRawData[whRawData.index('KDOR code for Oil:') + 1])
        except:
            whFilteredData.append("")
        whFilteredData.append(whRawData[whRawData.index('KCC Permit No.: ') + 1])

        # self.store_wh(whFilteredData)

        essentials = [whFilteredData[0], whFilteredData[1]]


         # if casing table is present, pass proper param

        # try:
        if item['ip']:
            ipRawData = [i.replace('\n', "") for i in item['ip']]
            ipFilteredData = []

            ipFilteredData.append(ipRawData[ipRawData.index('Producing Method: ') + 1])
            if ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Oil: ') + 1] != '\xa0\xa0\xa0\xa0Water: ':
                ipFilteredData.append(ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Oil: ') + 1])
            else:
                ipFilteredData.append('')
            if ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Water: ') + 1] != '\xa0\xa0\xa0\xa0Gas: ':
                ipFilteredData.append(ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Water: ') + 1])
            else:
                ipFilteredData.append('')
            if ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Gas: ') + 1] != 'Disposition of Gas: ':
                ipFilteredData.append(ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Gas: ') + 1])
            else:
                ipFilteredData.append('')
            if ipRawData[ipRawData.index('Disposition of Gas: ') + 1] != '\xa0\xa0\xa0\xa0Size: ':
                ipFilteredData.append(ipRawData[ipRawData.index('Disposition of Gas: ') + 1])
            else:
                ipFilteredData.append('')
            if ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Size: ') + 1] != '\xa0\xa0\xa0\xa0Set at: ':
                ipFilteredData.append(ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Size: ') + 1])
            else:
                ipFilteredData.append('')
            if ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Set at: ') + 1] != '\xa0\xa0\xa0\xa0Packer at: ':
                ipFilteredData.append(ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Set at: ') + 1])
            else:
                ipFilteredData.append('')
            if ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Packer at: ') + 1] != 'Production intervals: ':
                ipFilteredData.append(ipRawData[ipRawData.index('\xa0\xa0\xa0\xa0Packer at: ') + 1])
            else:
                ipFilteredData.append('')
            ipFilteredData.append(ipRawData[ipRawData.index('Production intervals: ') + 1])
            
            tem = list(essentials)
            tem.extend(ipFilteredData)
            self.store_ip(tem)
        # except:
        #     pass

        # if casing table is present, pass proper param

        try:
            if item['casing']:
                casingRawData = item['casing'][item['casing'].index('Additives') + 3:]
                casingFilteredData = []
                for i in range(len(casingRawData) // 17):
                    temp = list()
                    temp.extend(essentials)
                    temp.extend(casingRawData[i * 17 + 1:(i + 1) * 17:2])
                    casingFilteredData.append(temp)
                # self.store_casing(casingFilteredData)
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
                # self.store_pf(pfFilteredData)

        except:
            pass

        # if cuttings table is present, pass proper param

        try:
            if item['cutting']:
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
                # self.store_cutting(cuttingFilteredData)

        except:
            pass

        return item

    def store_wh(self, item):
        self.cur.execute(
            "INSERT INTO WH VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(item))
        self.conn.commit()

    def store_ip(self, item):
        self.cur.execute(
            "INSERT INTO IP VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(item))
        self.conn.commit()

    def store_cutting(self, item):
        newitem = []
        for t in item:
            while len(t) < 6:
                t.append('')
            newitem.append(t)

        args_str = b','.join(self.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in newitem).decode("utf-8")
        self.cur.execute("INSERT INTO cutting VALUES " + args_str)
        self.conn.commit()

    def store_casing(self, item):
        args_str = b','.join(
            self.cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
        self.cur.execute("INSERT INTO casing VALUES " + args_str)
        self.conn.commit()

    def store_pf(self, item):
        args_str = b','.join(self.cur.mogrify("(%s, %s, %s, %s, %s, %s)", tuple(x)) for x in item).decode("utf-8")
        self.cur.execute("INSERT INTO Perforation VALUES " + args_str)
        self.conn.commit()
