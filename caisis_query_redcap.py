'''
    author@esilgard April 2016 Update May 2017
    query caisis for clinical data based configurations in json file
    extract to csv for redcap dataloading and specimen/assay linkage
'''

import pyodbc
import json
import os
import csv
import sys

# json config file holds details of disease, patients, and database
config = json.load(open(sys.argv[1],"r"))
mrns = [x.strip() for x in open(config["patients"]["patient_id_file"],"r").readlines()]
metadata = json.load(open(config["disease"]["metadata"], "r"))

## pyodbc connection string details
connStr = ("DRIVER=" + config["database"]["driver"] + \
           ";SERVER=" + config["database"]["server_name"]  + \
           ";DATABASE=" + config["database"]["database_name"]  + \
           ";Trusted_Connection=yes")   
conn = pyodbc.connect(connStr)
cur = conn.cursor()

# this caisis_id query will get the caisis id for any MRN found in caisis
# even if it is not the primary MRN listed in the patients table
# store in dictionary of key = caisis_id to value = MRN from input list
caisis_id_mapping = dict((x[1],x[0]) for x in cur.execute("SELECT distinct \
    PtMRN, PatientId FROM vPatientIdMRN WHERE PtMRN in ('" + "','".join(mrns) + \
    "')").fetchall())
# string used for SQL queries
caisis_id_string = "('" + "','".join([str(c) for c in caisis_id_mapping.keys()]) + "')"

# output mapping file to ensure data can be mapped back to the MRN PROVIDED
with open(config["output"]["directory"] + os.path.sep + "pt_id_mapping.csv", "w") as mapping_out:
    csv_writer = csv.writer(mapping_out, lineterminator = '\n')
    csv_writer.writerow(['mrn','patientid'])
    for k,v in caisis_id_mapping.items():
        csv_writer.writerow([v,k])
    
    
# reality check print statement
print "querying " +  config["database"]["database_name"]  + " for " +  \
    config["disease"]["name"] + ": " + str(len(caisis_id_mapping.keys())) + \
    " patients found in caisis out of " + str(len(mrns)) + " mrns provided"

for tables in [x for x in metadata["tables"]]:    
    table_name = tables['table']                          
    fields = tables['fields']
    query_list = [table_name + '.' + f for f in tables['fields']]
    
    # RedCap header requirements - all lowercase, no periods, first column = pt_id
    # for all tables where PatientId may be repeated, also include 
    # redcap_repeat_instrument = the caisis patientid 
    # redcap_repeat_instance = the primary key for the table
    output_headers = ['pt_id']
    if table_name!= 'Patients':
        output_headers += ['redcap_repeat_instrument','redcap_repeat_instance']
    output_headers += [x.lower().replace('.','_') for x in query_list]
    
    # query by patient id unless a single join is necessary (dictated by metadata file)
    if tables['patientIdInTable'] == "True":
        # make sure patientid is in every table
        query_string = "SELECT " + table_name + ".PatientId as pt_id, "
        if table_name != 'Patients':
            query_string += "'" + table_name + "' as a, " + table_name + "." + \
                tables['primaryKey'] + ", "
        # add to query string based on metadata (varies by disease group)
        query_string  += ", ".join(query_list) + " FROM "+ table_name
        query_string += " WHERE " + table_name + ".PatientId in "+ caisis_id_string
        
    else:
        joining_table = tables['joinOn'][0]
        joining_key = tables['joinOn'][1]
        # make sure patientid is in every table
        query_string = "SELECT " + joining_table + ".PatientId as pt_id, '" + \
            table_name + "' as a, " + table_name + "." +  tables['primaryKey'] + ", "
        query_string  += ", ".join(query_list) + " FROM " + table_name
        query_string += " INNER JOIN " + joining_table + " ON " + joining_table + \
            "." + joining_key + "=" + tables['table'] +"." + joining_key + \
            " WHERE " + joining_table + ".PatientId in " + caisis_id_string 

    rows =  cur.execute(query_string).fetchall()
    # if there are results - write to file
    if rows:
        with open (config["output"]["directory"] + os.path.sep + table_name + ".csv", "w") as out: 
            csv_writer = csv.writer(out, lineterminator = '\n')
            csv_writer.writerow(output_headers)
            for r in rows:
                csv_writer.writerow(r)
    else:
        print "no records returned in ", table_name
