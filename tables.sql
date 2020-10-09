-- Table Queries
-- Create database before this

DROP TABLE IF EXISTS WH;
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

DROP TABLE IF EXISTS Cutting;
CREATE TABLE Cutting(
   API TEXT,
   KID TEXT,
   Box_Number TEXT,
   Starting_Depth TEXT,
   Ending_Depth TEXT,
   Skips TEXT
);

DROP TABLE IF EXISTS Casing;
CREATE TABLE Casing(
   API TEXT,
   KID TEXT,
   Purpose_Of_String TEXT,
   Size_Hole_Drilled TEXT,
   Size_Casing_Set TEXT,
   Weight TEXT,
   Setting_Depth TEXT,
   Type_Of_Cement TEXT,
   Sacks_Used TEXT,
   Type_And_Percent_Additives TEXT
);

DROP TABLE IF EXISTS Perforation;
CREATE TABLE Perforation(
   API TEXT,
   KID TEXT,
   Shots_Per_Foot TEXT,
   Perforation_Record TEXT,
   Material_Record TEXT,
   Depth TEXT
);

DROP TABLE IF EXISTS IP;
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

DROP TABLE IF EXISTS oilProduction;
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

DROP TABLE IF EXISTS Tops;
CREATE TABLE Tops(
   API TEXT,
   KID TEXT,
   FORMATION TEXT,
   Top_ TEXT,
   BASE TEXT,
   SOURCE TEXT,
   UPDATED TEXT
);