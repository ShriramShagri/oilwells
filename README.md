# Web Scraper

> Install scrapy and postgres

```bash
    pip install scrapy
    pip install psycopg2
    pip install dateutil
    pip install xlrd
```

> Go inside pro_gen folder

```bash
    cd pro_gen
```

> Run scrapy

```bash
    scrapy crawl spider_name
```

- Create database and specify the credentials in [constants.py](pro_gen/constants.py) file. Also create tables, queries are in [tables.sql](tables.sql) file.

- spider_name can be changed of found inside [constants.py](pro_gen/constants.py) folder.
- Storage path for downloaded files and counties can be changed inside the same file.


### Master List of Oil and Gas Wells: 
http://www.kgs.ku.edu/Magellan/Qualified/index.html


### County:
---------
https://chasm.kgs.ku.edu/ords/qualified.ogw5.FileSave?f_t=&f_r=&ew=W&f_s=&f_l=&f_op=&f_st=15&f_c=23&f_api=&f_ws=ALL
1) ogwell9911.txt

2) File containing tops data (for wells with tops): tops9911.txt


### Files to Download on API Details apge:
--------------------------------------
Well Completion Report
Intent to Drill Well
Drill Stem Test
Directional Survey Date --> CSV/Excel/PDF
Production: https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1031882285
Cuttings: https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1002883832



### Engineering Inforation: (Database Table - Identifier - API) 
------------------------
Casing
Performation
Initial Potential
Formation


https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1037989902
### Casing: 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayEng?f_kid=1044091564



---
Oil Production 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1041231775


---
Tops Data in new Page
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1002884009

---

DST Report
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1002883840

---

Drill Stem Test
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1044998943

---

Tops Data: 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1031431227

---
Tops Data with new page: 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1006067479

---
Cuttings Data wtih 6 fields: 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1042194696
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1043933653

---
Cuttings data with 3 fields: 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1006067447

---
Messed Up pf
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayEng?f_kid=1044998943

---
Well with WH and Production data: 
https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1025721792

---

Directional Suyvey: https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1002880481

---

# Errors

> No elements in pf table

https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayEng?f_kid=1044564317

> No engineering data even having link

https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1046936239

> Extra columns in perforation table

https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayEng?f_kid=1046070165

> Data missing should include?

https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayEng?f_kid=1044596288




> New column in cuttings?!

https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1051306869


> Error 

https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid=1006084758

https://chasm.kgs.ku.edu/ords/dst.dst2.DisplayDST?f_kid=1002876162