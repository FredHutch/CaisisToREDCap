# CaisisToREDCap
a script to pull clinical data from Caisis and output in a format that can be ingested into REDCap

 - query caisis for clinical data based configurations in json file
 - extract to csv for redcap dataloading and specimen/assay linkage

run from query_caisis_redcap.py with a single argument; the path to the config.json file

*config.json* contains input and output directories, path to *metadata.json* file and database connection details

*metadata.json* contains tables and fields needed to build the query depending on the caisis disease group
