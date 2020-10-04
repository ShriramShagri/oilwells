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

# Table Queries

>WH Table

```sql
CREATE TABLE WH(
   API TEXT,
   KID TEXT,
   Lease TEXT,
   Well TEXT,
   Original_operator TEXT,
   Current_operator TEXT,
   Field TEXT,
   Location1 TEXT,
   Location2 TEXT,
   Location3 TEXT,
   NAD27_Longitude TEXT,
   NAD27_Latitude TEXT,
   NAD83_Longitude TEXT,
   NAD83_Latitude TEXT,
   County TEXT,
   Permit_Date TEXT,
   Spud_Date TEXT,
   Completion_Date TEXT,
   Plugging_Date TEXT,
   Well_Type TEXT,
   Status TEXT,
   Total_Depth TEXT,
   Elevation TEXT,
   Producing_Formation TEXT,
   IP_Oil TEXT,
   IP_Water TEXT,
   IP_GAS TEXT,
   KDOR_code TEXT,
   KCC_Permit_No TEXT
);
```

>Cuttings Table

```sql
CREATE TABLE Cutting(
   API TEXT,
   KID TEXT,
   Box_Number TEXT,
   Starting_Depth TEXT,
   Ending_Depth TEXT,
   Skips TEXT
);

```

>Casing Table

```sql
CREATE TABLE Casing(
   API TEXT,
   KID TEXT,
   Purpose_Of_String TEXT,
   Size_Hole_Drilled TEXT
   Size_Casing_Set TEXT
   Weight TEXT
   Setting_Depth TEXT,
   Type_Of_Cement TEXT,
   Sacks_Used TEXT
   Type_And_Percent_Additives TEXT
);

```

>Perforation Table

```sql
CREATE TABLE Perforation(
   API TEXT,
   KID TEXT,
   Shots_Per_Foot TEXT,
   Perforation_Record TEXT,
   Material_Record TEXT,
   Depth TEXT
);

```

>IP Table

```sql
CREATE TABLE IP(
   API TEXT,
   KID TEXT,
   Producing_Method TEXT,
   Oil TEXT,
   Water TEXT,
   Gas TEXT,
   Disposition_of_Gas TEXT,
   Size TEXT,
   Set_at TEXT,
   Packer_at TEXT,
   Production_intervals TEXT
);

```

>oilProduction Table

```sql
CREATE TABLE oilProduction (
   API TEXT,
   KID TEXT,
   LEASE_KID TEXT,
   LEASE TEXT,
   DOR_CODE TEXT,
   API_NUMBER TEXT,
   FIELD TEXT,
   PRODUCING_ZONE TEXT,
   OPERATOR TEXT,
   COUNTY TEXT,
   TOWNSHIP TEXT,
   TWN_DIR TEXT,
   RANGE TEXT,
   RANGE_DIR TEXT,
   SECTION TEXT,
   SPOT TEXT,
   LATITUDE TEXT,
   LONGITUDE TEXT,
   MONTH_YEAR TEXT,
   PRODUCT TEXT,
   WELLS TEXT,
   PRODUCTION TEXT
);

```

>IP Table

```sql
CREATE TABLE Tops(
   API TEXT,
   KID TEXT,
   FORMATION TEXT,
   TOP TEXT,
   BASE TEXT,
   SOURCE TEXT,
   UPDATED TEXT
);

```