# This script is to make modifications to the csv prior to running other transformations. 
# This is for data modifications that are easier applied in CSV format
# Replacements made with Amber for download instructions see: https://github.com/dalance/amber 

###
# MAKE A COPY FIRST
cp ripa_stops_datasd.csv edited_ripa_stops_datasd.csv

# Search and Replace date entries in Time_Stop
ambr "1900-01-01" "" ripa_stops_datasd.csv
ambr "1899-12-31" "" ripa_stops_datasd.csv

####
