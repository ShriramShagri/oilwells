# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2


class ProGenPipeline:
    def __init__(self):
        self.conn = psycopg2.connect(
                            host="localhost",
                            database="oilWells",
                            user="postgres",
                            password="lynx1729"
                            )
        self.cur = self.conn.cursor()


    def process_item(self, item, spider):
        # if wh table is present, pass proper param

        templist = [i.replace('\n' ,"") for i in item['wh']]

        if templist[18] == '\n':
            tryout = templist[1:14:2] + templist[15:17] + templist[18:25:2] +templist[27::2]
            self.store_wh(tryout, False)
            
        else:
            tryout = templist[1:14:2] + templist[15:18] + templist[19:26:2] +templist[28::2]
            self.store_wh(tryout, True)

        API, KID = tryout[0], tryout[1]

        # if wh table is present, pass proper param

        return item

    def store_wh(self, item, extra):
        if extra:
            while len(item)>28:
                del item[-2]
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
            Location3,
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            self.cur.execute(sql, tuple(item))
            self.conn.commit()
        else:
            while len(item)>=28:
                del item[-2]
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
    
    def store_ip(self, item, kid):
        sql = '''
        INSERT INTO IP
        KID,
        Producing_Method,
        Oil,
        Water,
        Gas,
        Disposition of Gas,
        Size,
        Set_at,
        Packer_at,
        Production_intervals 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        self.cur.execute(sql, tuple(kid) + tuple(item))
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
    
    def store_casing(self, item, kid):
        sql = '''
        INSERT INTO IP
        KID,
        Purpose_Of_String,
        Size_Hole_Drilled
        Size_Casing_Set
        Weight
        Setting_Depth,
        Type_Of_Cement,
        Sacks_Used
        Type_And_Percent_Additives 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        self.cur.execute(sql, tuple(kid) + tuple(item))
        self.conn.commit()
    
    def store_perforation(self, item, kid):
        sql = '''
        INSERT INTO IP
        KID,
        Shots_Per_Foot,
        Perforation_Record,
        Material_Record,
        Depth 
        VALUES (%s, %s, %s, %s, %s)
        '''
        self.cur.execute(sql, tuple(kid) + tuple(item))
        self.conn.commit()
