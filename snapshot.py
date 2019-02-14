#PURPOSE
#   Takes snapshots--in file-geodatabase format--of all or a subset of a given geodatabase's
#   feature datasets, feature classes, view-based feature-classes, tables, view-based tables,
#   and raster datasets. Snapshots are taken on a specified interval.

#README NOTES
#   This script logs its activity to a log file (snapshot.log) which is written into the
#   script's directory at execution time. Periodically truncate or delete the log file to
#   preserve disk space.
#
#   Feature datasets aren't copied at the feature-dataset level. Instead, a feature dataset
#   with the same name is created in the snapshot geodatabase and then feature classes are
#   copied individually into that feature dataset. Other feature-dataset objects, such as
#   topologies and geometric networks, aren't copied.

#HOW TO USE
#   Write a script that calls this script and passes arguments per arguments described in
#   script's section that is commented w/
#   #***** GET ARGUMENTS *****.
#   Then run or schedule the calling script.

#HISTORY
#   DATE         ORGANIZATION     PROGRAMMER          NOTES
#   01/17/2019   VCGI             Ivan Brown          First stable release. Built w/ ArcGIS
#                                                     Desktop (arcpy) 10.6.1 and Python 2.7.14.
#                                                     Tested w/ ArcGIS Server 10.5.1.
#
#   02/13/2019   VCGI             Ivan Brown          Added include-list option. Also,
#                                                     changed major-variable settings so that
#                                                     they are obtained via
#                                                     arcpy.GetParameterAsText(); this
#                                                     allows a script to call this script
#                                                     multiple times, once for each of a set
#                                                     of geodatabases.

#PSEUDO CODE
#   Set major variables (via script arguments), including:
#      -source geodatabase
#      -path of folder in which snapshot geodatabases (.gdb) are created
#      -list of data objects to be included from snapshots
#      -list of data objects to be excluded from snapshots
#      -boolean to indicate if raster datasets are to be included
#      -tempo in which snapshots are taken (in days)
#
#   Get today's date. Get date of last snapshot (based on names of snapshots in snapshot folder).
#   If it is time for a snapshot:
#      Create a snapshot geodatabase w/ name that includes today's date (YYYYMMDD). Naming pattern:
#         SNAPSHOT_<geodatabase nickname>_YYYYMMDD.gdb
#      For each feature dataset of source geodatabase:
#         If (include list has items and it is in include list and it isn't in exclude list) or (include list doesn't have items and it isn't in exclude list):
#            Create a feature dataset w/ same name in snapshot geodatabase
#            For each feature class in that feature dataset:
#               If (include list has items and it is in include list and it isn't in exclude list) or (include list doesn't have items and it isn't in exclude list):
#                  Copy it to snapshot geodatabase (within a same-name feature-dataet)
#      For each stand-alone feature-class:
#         If (include list has items and it is in include list and it isn't in exclude list) or (include list doesn't have items and it isn't in exclude list):
#            Copy it to snapshot geodatabase
#      For each non-spatial table:
#         If (include list has items and it is in include list and it isn't in exclude list) or (include list doesn't have items and it isn't in exclude list):
#            Copy it to snapshot geodatabase
#      If raster datasets are included in process:
#         For each raster dataset:
#            If (include list has items and it is in include list and it isn't in exclude list) or (include list doesn't have items and it isn't in exclude list):
#               Copy it to snapshot geodatabase
#
#   Send email report on script activity. Report includes:
#      -include list
#      -exclude list
#      -snapshot tempo
#      -date of last snapshot-geodatabase, if exists
#      -if a new snapshot-geodatabase was made, name of new snapshot-geodatabase
#      -list of data objects that were copied or exported
#      -list of all snapshot-geodatabases (and their approximate sizes) in snapshot-geodatabase folder

#IMPORT MODULES
print "IMPORTING MODULES..."
import time, datetime, calendar, sys, os, os.path, smtplib, arcpy

print "SETTING MAJOR VARIABLES BY READING ARGUMENTS..."
#***** GET ARGUMENTS *****
#
#      PASS ARGUMENTS TO SCRIPT IN SAME ORDER AS PRESENTED HERE, WHICH IS:
#
#         <source_gdb> <snapshot_folder> <include_list> <exclude_list> <include_rasters> <tempo> <gdb_nickname> <email_server> <email_port> <email_from> <to_list>
#
#Set "source_gdb" argument to path of an .sde file or path of .gdb from which snapshot is made.
#
#   If setting to an .sde file (connection to enterprise geodatabase), make sure that:
#      login (read-only) and password are hardwired to the connection (if script will run as
#      automated-scheduled task).
#
#   Otherwise, make sure that the script has read-access to the .gdb.
source_gdb = arcpy.GetParameterAsText(0)
#
#Set "snapshot_folder" argument to full path of folder in which snapshots are made.
#
snapshot_folder = arcpy.GetParameterAsText(1)
#
#Set "include_list" argument to a string of comma-separated names to indicate names of feature datasets,
#   feature classes, non-spatial tables, and raster datasets to be explicitly included in the
#   snapshots unless they are prohibited via the exclude_list argument and/or the include_rasters
#   argument.
#
#   Set to an empty string ("") to pass in an empty include_list. In this case, data objects are
#   included in the snapshots unless prohibited via the exclude_list argument and/or the include_rasters
#   argument.
#
#   If include_list isn't empty, only its items are considered for inclusion in snapshots. Only
#   data objects in include_list that aren't prohibited via the exclude_list argument and/or the
#   include_rasters argument are included in snapshots.
#
#   Don't include schema prefixes (DATABASE.OWNER.). The script handles them on its own.
#
#   This argument isn't case-sensitive.
#
#   Format names with this pattern:
#      {fds:}<name>
#
#      If the item is a feature dataset, begin the string w/ "fds:".
#
#      Examples:
#         A feature dataset:
#            fds:SchoolDistricts
#
#         A stand-alone featureclass:
#            Parks
#
#         A non-spatial table:
#            PINcrosswalk
#
#         A raster dataset:
#            DEM2007
#
#         A feature dataset and a stand-alone feature-class:
#            fds:PoliceDistricts,MileMarkers
#
include_list = arcpy.GetParameterAsText(2)
if include_list == "":
   include_list = []
else:
   include_list = include_list.split(",")
   i = 0
   while i < len(include_list):
      include_list[i] = include_list[i].strip()
      i += 1
#
#Set "exclude_list" argument to a string of comma-separated names to indicate names of feature datasets,
#   feature classes, non-spatial tables, and raster datasets to be excluded from the snapshot.
#
#   Set to empty string ("") to pass in an empty exclude_list.
#
#   Don't include schema prefixes (DATABASE.OWNER.). The script handles them on its own.
#
#   This argument isn't case-sensitive.
#
#   Format names with this pattern:
#      {fds:}<name>
#
#      If the item is a feature dataset, begin the string w/ "fds:".
#
#      Examples:
#         A feature dataset:
#            fds:VehicularTransportation
#
#         A stand-alone featureclass:
#            CityBoundary
#
#         A non-spatial table:
#            ServiceRequests
#
#         A raster dataset:
#            LandCover2012
#
#         A stand-alone feature-class and a raster dataset:
#            Hydrants,TreeCanopy
#
exclude_list = arcpy.GetParameterAsText(3)
if exclude_list == "":
   exclude_list = []
else:
   exclude_list = exclude_list.split(",")
   i = 0
   while i < len(exclude_list):
      exclude_list[i] = exclude_list[i].strip()
      i += 1
#
#Set "include_rasters" boolean argument to indicate if raster datasets are copied.
#
#   Set to True if raster datasets are copied (if include_list has items and raster included in include_list and
#   raster isn't in exclude_list) OR (if include_list doesn't have items and raster isn't in exclude_list).
#
#   Set to False if no raster datasets will be copied whatsoever (raster datasets in
#   include_list and exclude_list arguments are ignored when this argument is set to False, as all are excluded).
#
include_rasters = arcpy.GetParameterAsText(4)
if include_rasters == "True":
   include_rasters = True
else:
   include_rasters = False
#
#Set "tempo" argument to an integer that specifies number of days that lapse until time to take
#   a new snapshot.
#
#   For example, if a snapshot is to be taken every 7 days, set to 7.
#
#   Days are counted as if counting cells on a wall calendar--days aren't measured from the hour
#   level. For example, the tempo argument is set to 7 (snapshot every 7 days). The script runs
#   at 5:00 AM on January 1. The script then runs at 3:00 AM on January 8. Because January 8 is
#   7 days after January 1, the script takes a new snapshot--even though 3:00 AM is 2 hours
#   before the exact 7-day mark.
#
tempo = int(arcpy.GetParameterAsText(5))
#
#Set "gdb_nickname" to a string that identifies the source geodatabase/data in snapshot-geodatabase
#   counterparts, email, and log file. Snapshot-geodatabase names are based on this string
#   (snapshot_ + gdb_nickname + _YYYYMMDD.gdb)
#   Don't include any spaces or special characters; underscores are okay.
#
#   For example:
#      GDB_BigCity_ParcelData
#
gdb_nickname = arcpy.GetParameterAsText(6)
#
#email_server
#   Set to the host name of the SMTP router to be used for sending automated email.
#
#   For example:
#      BigCityEmailServer
#
email_server = arcpy.GetParameterAsText(7)
#
#email_port
#   The port number of the SMTP router to be used for sending email.  Set to a string.
#
#   For example:
#      999
#
email_port = arcpy.GetParameterAsText(8)
#
#email_from
#   The sender email address to be used with email notifications (must be in
#   name@domain format). An email account that is used for automated notifications in
#   your organization can be used.
#
#   For example:
#      name@domain
#
email_from = arcpy.GetParameterAsText(9)
#
#to_list
#   This setting is used to store email addresses of email recipients (must be in
#   name@domain format). Set to a string of comma-separated email addresses.
#
#   For example:
#      name1@domain1,name2@domain2
#
to_list = arcpy.GetParameterAsText(10).split(",")
i = 0
while i < len(to_list):
   to_list[i] = to_list[i].strip()
   i += 1
#***** END OF SECTION FOR GETTING ARGUMENTS *****

#***** OTHER VARIABLES
#email_content
#   A GLOBAL VARIABLE THAT STORES CONTENT TO BE WRITTEN TO AUTOMATED EMAIL.
email_content = ""

#FUNCTIONS

#THIS FUNCTION SIMPLY CAPTURES THE CURRENT DATE AND TIME AND
#   RETURNS IN A PRESENTABLE TEXT FORMAT YYYYMMDD-HHMM
#   FOR EXAMPLE:
#      20171201-1433
def tell_the_time():
   s = time.localtime()
   the_year = str(s.tm_year)
   the_month = str(s.tm_mon)
   the_day = str(s.tm_mday)
   the_hour = str(s.tm_hour)
   the_minute = str(s.tm_min)
   #FORMAT THE MONTH TO HAVE 2 CHARACTERS
   while len(the_month) < 2:
      the_month = "0" + the_month
   #FORMAT THE DAY TO HAVE 2 CHARACTERS
   while len(the_day) < 2:
      the_day = "0" + the_day
   #FORMAT THE HOUR TO HAVE 2 CHARACTERS
   while len(the_hour) < 2:
      the_hour = "0" + the_hour
   #FORMAT THE MINUTE TO HAVE 2 CHARACTERS
   while len(the_minute) < 2:
      the_minute = "0" + the_minute
   the_output = the_year + the_month + the_day + "-" + the_hour + the_minute
   return the_output

#THIS FUNCTION SIMPLY TAKES A STRING ARGUMENT AND THEN
#   WRITES THE GIVEN STRING INTO THE SCRIPT'S LOG FILE.
#   SET FIRST ARGUMENT TO THE STRING. SET THE SECOND
#   ARGUMENT (BOOLEAN) TO True OR False TO INDICATE IF
#   STRING SHOULD ALSO BE PRINTED. SET THE THIRD
#   ARGUMENT (BOOLEAN) TO True OR False TO INDICATE IF
#   STRING SHOULD ALSO BE INCLUDED IN EMAIL NOTIFICATION.
#   ADDS CURRENT TIME TO BEGINNING OF FIRST PARAMETER.
#   ADDS A \n TO FIRST PARAMETER (FOR HARD RETURNS).
def make_note(the_note, print_it = False, email_it = False):
   the_note = tell_the_time() + "  " + the_note
   the_note += "\n"
   log_file = open(sys.path[0] + "\\snapshot.log", "a")
   log_file.write(the_note)
   log_file.close()
   if print_it == True:
      print the_note
   if email_it == True:
      global email_content
      email_content += the_note

#THIS FUNCTION TAKES A GIVEN DATA-OBJECT NAME AND RETURNS ITS NAME W/O SCHEMA PREFIX
def get_name(obj_name):
   i = obj_name.rfind(".")
   if i == -1:
      return obj_name
   else:
      return obj_name[i + 1:len(obj_name)]
   
#THIS FUNCTION SENDS A GIVEN MESSAGE (W/ STRING "SECURE" IN SUBJECT) TO AN EMAIL DISTRIBUTION-LIST
#   THE FIRST ARGUMENT IS THE EMAIL'S SUBJECT STRING
#   THE SECOND ARGUMENT IS THE EMAIL'S MESSAGE-CONTENT STRING
def send_email(the_subject = "", the_message = ""):
   the_header = 'From:  "Python" <' + email_from + '>\n'
   the_header += "To:  Snapshot Watchers\n"
   the_header += "Subject:  [SECURE] " + the_subject + "\n"
   #INSTANTIATE AN SMTP OBJECT
   smtp_serv = smtplib.SMTP(email_server + ":" + email_port)
   #SEND THE EMAIL
   smtp_serv.sendmail(email_from, to_list, the_header + the_message)
   #QUIT THE SERVER CONNECTION
   smtp_serv.quit()

#THIS FUNCTION RETURNS AN ESTIMATED SIZE, IN MB (INTEGER), OF A GIVEN FILE-GEODATABASE
#   ITS ARGUMENT IS FULL PATH OF THE FILE GEODATABASE
def get_gdb_size(the_path):
   file_list = os.listdir(the_path)
   total_mb = 0
   for i in file_list:
      total_mb = total_mb + ((os.path.getsize(os.path.join(the_path, i)) / 1000) / 1000)
   return total_mb

try:
   #GET CURRENT DATE, TO BE CONSIDERED TO BE CURRENT DATE THROUGHOUT SCRIPT EXECUTION
   t = time.localtime()

   #VERIFY THAT SOURCE GEODATABASE EXISTS
   if arcpy.Exists(source_gdb) == False:
      make_note("Couldn't connect to source geodatabase " + source_gdb + ". Script terminated.", True, True)
      sys.exit()

   #VERIFY THAT SNAPSHOT FOLDER EXISTS
   if arcpy.Exists(snapshot_folder) == False:
      make_note("Snapshot folder " + snapshot_folder + " doesn't exist. Script terminated.", True, True)
      sys.exit()

   #CAPTURE TODAY'S DAY AND 8-CHARACTER DATE-REPRESENTATION
   y2 = t.tm_year
   m2 = t.tm_mon
   d2 = t.tm_mday
   n2 = t.tm_yday
   today8 = str(y2)
   the_string = str(m2)
   while len(the_string) < 2:
      the_string = "0" + the_string
   today8 += the_string
   the_string = str(d2)
   while len(the_string) < 2:
      the_string = "0" + the_string
   today8 += the_string
   make_note("Today is day " + str(n2) + " of the year.", True)
   make_note("Today's date--in YYYYMMDD pattern--is " + today8, True)

   #GET LIST OF SNAPSHOTS IN SNAPSHOT FOLDER, W/ PATTERN YYYYMMDD,<full snapshot name>,<.gdb size, in MB>
   arcpy.env.workspace = snapshot_folder
   the_list = arcpy.ListWorkspaces("*", "FileGDB")
   snapshots = []
   for i in the_list:
      j = i.upper()
      the_date = j[len(j) - 12:len(j) - 4]
      try:
         x = int(the_date)
         gdb_size = str(get_gdb_size(os.path.join(arcpy.env.workspace,i)))
         snapshots.append(the_date + "," + j + "," + gdb_size)
         make_note("Found snapshot from " + the_date + " (" + i + ").", True, True)
      except:
         make_note("Geodatabase " + i + " isn't named according to how this script names snapshots. Excluding it from list of pre-exising snapshots.", True, True)
   snapshots.sort()
   snapshots.reverse()

   #MAKE SURE SNAPSHOT W/ TODAY'S DATE DOESN'T ALREADY EXIST
   for i in snapshots:
      if i[0:8] == today8:
         make_note("A snapshot (" + i[9:i.rfind(",")] + ") w/ today's date already exists. Script terminated.", True, True)
         sys.exit()

   #GET DAY OF LAST SNAPSHOT (PER SNAPSHOTS IN SNAPSHOT FOLDER)
   if len(snapshots) > 0:
      latest8 = snapshots[0][0:8]
      make_note("Day of last snapshot (per snapshot-folder contents) is " + latest8 + ".", True, True)
   else:
      latest8 = None
      make_note("Snapshot folder doesn't have pre-existing snapshots.", True, True)
      
   #DETERMINE IF IT IS TIME FOR A SNAPSHOT
   #(IF A SNAPSHOT EXISTS, GET NUMBER OF DAYS SINCE)
   if latest8:
      y1 = int(latest8[0:4])
      m1 = int(latest8[4:6])
      d1 = int(latest8[6:8])
      n1 = datetime.date(y1,m1,d1).timetuple()[7]
      day_count = n2
      #IF YEAR OF LAST SNAPSHOT IS BEFORE THIS YEAR, COUNT DAYS THROUGH YEARS BACK TO LAST SNAPSHOT
      if y1 < y2:
         i = y2 - 1
         while i >= y1:
            if calendar.isleap(i):
               if i > y1:
                  day_count += 366
               else:
                  day_count = day_count + 366 - n1
            else:
               if i > y1:
                  day_count += 365
               else:
                  day_count = day_count + 365 - n1
            i = i - 1
      else:
         day_count = n2 - n1
   else:
      day_count = 0
   if day_count >= tempo or latest8 == None:
      time_for_snapshot = True
   else:
      time_for_snapshot = False
   make_note(str(day_count) + " days have passed since last snapshot. Snapshot tempo is " + str(tempo) + " days.", True, True)

   #IF IT IS TIME FOR A SNAPSHOT, PROCEED
   if time_for_snapshot == True:
      #CREATE SNAPSHOT GEODATABASE
      snapshot_gdb_name = "SNAPSHOT_" + gdb_nickname + "_" + today8 + ".gdb"
      arcpy.CreateFileGDB_management(snapshot_folder, snapshot_gdb_name)
      snapshot_gdb_path = snapshot_folder + "\\" + snapshot_gdb_name
      make_note("Created snapshot geodatabase " + snapshot_gdb_name + ".", True, True)

      #CAPTURE include_list INTO LOG/REPORT
      if len(include_list) > 0:
         make_note("DATA OBJECTS TO BE INCLUDED IN SNAPSHOT (VIA EXPLICIT INCLUDE-LIST):", True, True)
         for i in include_list:
            make_note("     " + i, True, True)

      #CAPTURE exclude_list INTO LOG/REPORT
      make_note("DATA OBJECTS TO BE EXCLUDED FROM SNAPSHOT:", True, True)
      for i in exclude_list:
         make_note("     " + i, True, True)         

      #DERIVE OBJECT-TYPE-SPECIFIC INCLUDE-LISTS FROM include_list (UPPER-CASED)
      #(FEATURE DATASETS)
      if len(include_list) > 0:
         included_fds = []
         for i in include_list:
            if i[0:4].upper() == "FDS:":
               included_fds.append(i[4:len(i)].upper())
         #(OTHER DATA-OBJECTS)
         included_other = []
         for i in include_list:
            if i[0:4].upper() != "FDS:":
               included_other.append(i.upper())
      else:
         included_fds = []
         included_other = []
         
      #DERIVE OBJECT-TYPE-SPECIFIC EXCLUDE-LISTS FROM exclude_list (UPPER-CASED)
      #(FEATURE DATASETS)
      excluded_fds = []
      for i in exclude_list:
         if i[0:4].upper() == "FDS:":
            excluded_fds.append(i[4:len(i)].upper())
      #(OTHER DATA-OBJECTS)
      excluded_other = []
      for i in exclude_list:
         if i[0:4].upper() != "FDS:":
            excluded_other.append(i.upper())

      #SET WORKSPACE
      arcpy.env.workspace = source_gdb

      #WORK FEATURE DATASETS
      fds_list = arcpy.ListDatasets("*","Feature")
      for i in fds_list:
         j = get_name(i)
         if (len(include_list) > 0 and j.upper() in included_fds and j.upper() not in excluded_fds) or (len(include_list) == 0 and j.upper() not in excluded_fds):
            arcpy.CreateFeatureDataset_management(snapshot_gdb_path, j, i)
            fc_list = arcpy.ListFeatureClasses("*", "All", i)
            for k in fc_list:
               l = get_name(k)
               if l.upper() not in excluded_other:
                  arcpy.Copy_management(i + "\\" + k, snapshot_gdb_path + "\\" + j + "\\" + l)
                  make_note("Copied feature-class " + i + "\\" + k + " to snapshot geodatabase.", True, True)

      #WORK STAND-ALONE FEATURE-CLASSES
      fc_list = arcpy.ListFeatureClasses()
      for i in fc_list:
         j = get_name(i)
         if (len(include_list) > 0 and j.upper() in included_other and j.upper() not in excluded_other) or (len(include_list) == 0 and j.upper() not in excluded_other):
            arcpy.Copy_management(i, snapshot_gdb_path + "\\" + j)
            make_note("Copied feature-class " + i + " to snapshot geodatbase.", True, True)

      #WORK TABLES
      table_list = arcpy.ListTables()
      for i in table_list:
         j = get_name(i)
         if (len(include_list) > 0 and j.upper() in included_other and j.upper() not in excluded_other) or (len(include_list) == 0 and j.upper() not in excluded_other):
            arcpy.Copy_management(i, snapshot_gdb_path + "\\" + j)
            make_note("Copied table " + i + " to snapshot geodatbase.", True, True)

      #WORK RASTER DATASETS
      if include_rasters == True:
         raster_list = arcpy.ListRasters()
         for i in raster_list:
            j = get_name(i)
            if (len(include_list) > 0 and j.upper() in included_other and j.upper() not in excluded_other) or (len(include_list) == 0 and j.upper() not in excluded_other):
               arcpy.Copy_management(i, snapshot_gdb_path + "\\" + j)
               make_note("Copied raster " + i + " to snapshot geodatbase.", True, True)

      #GET SNAPSHOT-GEODATABASE'S 8-CHARACTER DATE AND SIZE INTO snapshots LIST
      gdb_size = str(get_gdb_size(snapshot_gdb_path))
      snapshots.append(today8 + "," + snapshot_gdb_path + "," + gdb_size)
      snapshots.sort()

   #OTHERWISE, SIMPLY REPORT
   else:
      make_note("Snapshot geodatabase not made.", True, True)
      
   #SCRIPT COMPLETED, EMAIL REPORT (INDLUDING SNAPSHOT-GEODATABASE SIZES, IF APPLICABLE)
   if len(snapshots) > 0:
      make_note("ESTIMATED SIZES OF SNAPSHOT GEODATABASES, IN MB:", True, True)
      for i in snapshots:
         make_note("     " + i[9:i.rfind(",")] + ":  " + i[i.rfind(",") + 1:len(i)], True, True)
   make_note("Script completed.\n\n", True)
   send_email("snapshot.py - " + gdb_nickname + " - REPORT", email_content)

except:
   make_note("Either a snapshot is not due at this time or something went wrong.", True, True)
   make_note("arcpy MESSAGES: " + arcpy.GetMessages(), True, True)
   send_email("snapshot.py - " + gdb_nickname + " - ERROR CONDITION", email_content)
