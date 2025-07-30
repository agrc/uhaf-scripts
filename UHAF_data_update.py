# -*- coding: utf-8 -*-
"""
Created on Tue Apr 01 10:25:27 2025
@author: eneemann

"""

import arcpy
from arcpy import env
import os
import time
import numpy as np
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor

#: Set variables
today = time.strftime("%Y%m%d")
update_csv = r"M:\Shared drives\UGRC Projects\UHAF\UHAF_Monthly_Reports\GIS Monthly Report - 7.1.2025.csv"
df = pd.read_csv(update_csv)

empty_zipcode_shapefile = r"M:\Shared drives\UGRC Projects\UHAF\ZipCodes\ZipCodes.shp"
# output_shapefile = r"M:\Shared drives\UGRC Projects\UHAF\ZipCodes\UHAF_ZipCodes_test_4.shp"
# output_shapefile = r"C:\Temp\UHAF_ZipCodes_test_20250508.shp"
merged_csv = rf"M:\Shared drives\UGRC Projects\UHAF\UHAF_Monthly_Reports\Updated_files\merged_table_{today}.csv"
# output_filename = r"M:\Shared drives\UGRC Projects\UHAF\UHAF_Monthly_Reports\Updated_files\UHAF_updated_ZipCodes.shp"
output_filename = r"C:\Temp\UHAF_updated_ZipCodes.shp"


#: Rename zip code column to ZIP5
columns_renames={"Applicant Zip/Postal Code": "ZIP5"}
df.rename(columns=columns_renames, inplace=True)

#: Replace Race and Ethnicity values with correct values
replacements = {'Hispanic or Latino/a': 'Hispanic', 'Not Hispanic or Latino/a': 'Not Hispanic',
                'Prefer not to answer': 'Chose Not to Respond', 'Other multiple race': 'Other - Multiple Race'}
df_replaced = df.replace(replacements)

#: Get counts based on zip code value
aggregated_df = df_replaced.groupby('ZIP5').agg(
    **{'TotalHousholds' : pd.NamedAgg(column='ZIP5', aggfunc='count'),
    'TotalAssistance' : pd.NamedAgg(column='UHAF Assistance Amount', aggfunc='sum'),
    'AverageAssistance' : pd.NamedAgg(column='UHAF Assistance Amount', aggfunc='mean'),
    'AvgPercentAMIGross' : pd.NamedAgg(column='AMI Gross %', aggfunc='mean'),
    }
)

#: Races
#: Create dataframe with White count in 'Race' column
white_race = df_replaced[df_replaced['Race'] == 'White']
white_race_zip_count = white_race.groupby('ZIP5').agg(
    **{'White' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Create dataframe Asian count in 'Race' column
asian_race = df_replaced[df_replaced['Race'] == 'Asian']
asian_race_zip_count = asian_race.groupby('ZIP5').agg(
    **{'Asian' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Create dataframe Black or African American count in 'Race' column
black_race = df_replaced[df_replaced['Race'] == 'Black or African American']
black_race_zip_count = black_race.groupby('ZIP5').agg(
    **{'BlackAfric' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Create dataframe Native Hawaiian or Pacific Islander count in 'Race' column
hawaiianpacific_race = df_replaced[df_replaced['Race'] == 'Native Hawaiian or Pacific Islander']
hawaiianpacific_race_zip_count = hawaiianpacific_race.groupby('ZIP5').agg(
    **{'Native_Haw' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Create dataframe American Indian or Alaska Native count in 'Race' column
indiannative_race = df_replaced[df_replaced['Race'] == 'American Indian or Alaska Native']
indiannative_race_zip_count = indiannative_race.groupby('ZIP5').agg(
    **{'AmIndianAK' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Create dataframe Other - Multiple Race count in 'Race' column
other_race = df_replaced[df_replaced['Race'] == 'Other - Multiple Race']
other_race_zip_count = other_race.groupby('ZIP5').agg(
    **{'Other' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Create dataframe Chose Not to Respond count in 'Race' column
cnr_race = df_replaced[df_replaced['Race'] == 'Chose Not to Respond']
cnr_race_zip_count = cnr_race.groupby('ZIP5').agg(
    **{'CNR' : pd.NamedAgg(column='Race', aggfunc='count')
    }
)

#: Ethnicity
#: Create dataframe with Hispanic count in 'Ethnicity' column
hispanic_eth = df_replaced[df_replaced['Ethnicity'] == 'Hispanic']
hispanic_eth_zip_count = hispanic_eth.groupby('ZIP5').agg(
    **{'Hispanic' : pd.NamedAgg(column='Ethnicity', aggfunc='count')
    }
)

#: Create dataframe with Not Hispanic count in 'Ethnicity' column
not_hispanic_eth = df_replaced[df_replaced['Ethnicity'] == 'Not Hispanic']
not_hispanic_eth_zip_count = not_hispanic_eth.groupby('ZIP5').agg(
    **{'NotHisp' : pd.NamedAgg(column='Ethnicity', aggfunc='count')
    }
)

#: Create dataframe with Chose Not to Respond count in 'Ethnicity' column
ecnr_eth = df_replaced[df_replaced['Ethnicity'] == 'Chose Not to Respond']
ecnr_eth_zip_count = ecnr_eth.groupby('ZIP5').agg(
    **{'EthnicityC' : pd.NamedAgg(column='Ethnicity', aggfunc='count')
    }
)

#: Join all dataframes together, one at a time to the aggregated_df dataframe
#: Join each Race one at a time
merged_df = aggregated_df.merge(white_race_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(asian_race_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(black_race_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(hawaiianpacific_race_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(indiannative_race_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(other_race_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(cnr_race_zip_count, how='left', on='ZIP5')

#: Join each Ethnicity one at a time
merged_df = merged_df.merge(hispanic_eth_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(not_hispanic_eth_zip_count, how='left', on='ZIP5')
merged_df = merged_df.merge(ecnr_eth_zip_count, how='left', on='ZIP5')

#: Replace nulls with zeros
merged_df = merged_df.fillna(0)

#: Save as CSV
merged_df.to_csv(merged_csv)

# Use arcpy to join table to shapefile and copy to new shapefile
temp_table = "in_memory\\table_to_join"
arcpy.conversion.TableToTable(merged_csv, "in_memory", "table_to_join")

env.qualifiedFieldNames = False #: preserve original field names
env.overwriteOutput = True #: overwrite existing shapefile
joined_shapefile = arcpy.management.AddJoin(empty_zipcode_shapefile, "ZIP5", temp_table, "ZIP5", "KEEP_ALL")

#: Delete existing shapefile
if arcpy.Exists(output_filename):
        arcpy.Delete_management(output_filename)

#: Copy to new shapefile
arcpy.management.CopyFeatures(joined_shapefile, output_filename)

#: Delete extra fields
arcpy.management.DeleteField(output_filename, ["OID_", "ZIP5_1"])


# #: Read in empty zip code shapefile as spatial dataframe
# sdf = pd.DataFrame.spatial.from_featureclass(empty_zipcode_shapefile)

# #: Join merged_df with our attributes to the empty zip code polygons
# #: Need to use ZIP in sdf and ZIP5 in merged_df to ensure join uses number fields
# final_sdf = sdf.merge(merged_df, how='left', left_on='ZIP', right_on='ZIP5')

# #: Replace null values with 0s
# columns = final_sdf.columns.to_list()
# items_to_remove = ['SHAPE', 'FID', 'COUNTYNBR', 'NAME', 'SYMBOL', 'ZIP', 'ZIP5']
# keep_columns = [item for item in columns if item not in items_to_remove]
# column_dictionary = {col_name: 0 for col_name in keep_columns}

# final_clean_sdf = final_sdf.fillna(column_dictionary).copy()
# final_clean_sdf.ZIP = final_clean_sdf.ZIP.astype(int)

# fix_cols = final_clean_sdf.select_dtypes(include='float64').columns
# final_clean_sdf[fix_cols] = final_clean_sdf[fix_cols].astype(np.float32)

# #: Write out spatial dataframe to shapefile to load into AGOL
# if arcpy.Exists('C:\Temp\temp.gdb'):
#     continue
# else:
#     arcpy.management.CreateFileGDB('C:\Temp', 'temp.gdb')

# final_clean_sdf.spatial.to_featureclass(location=output_shapefile)
# final_clean_sdf.spatial.to_featureclass(location='C:\Temp\temp.gdb\test_fc')


# final_clean_sdf.Index.astype(np.int64)
# gdf = gpd.GeoDataFrame(final_clean_sdf, crs="EPSG:26912", geometry='SHAPE')
# gdf.to_file(output_shapefile)  