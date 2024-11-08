import os
import math
import sqlite3


#####SOME DEPENDENCIES
#sqlite3

#fiji
##divide by zero set to 0

##python:
#pandas
#openpyxl

batch_folders_list = [
#"path/to/already_analyzed/folder",
"/path/to/first/new/folder",
"/path/to/second/new/folder",
]

#laptop
imagej_path = "/path/to/fiji-linux64/Fiji.app/ImageJ-linux64"

#sqlite_path = "/path/to/sqlite-tools-linux-x86-3410200/sqlite3"

## This is from when I was auto-generating spreadsheets from python pandas dataframes
spreadsheet_prog_path = "soffice"

#spreadsheet_path = "start excel"


#How many threads?
max_threads = (os.cpu_count() - 2)
#max_threads = 15
#max_threads = 1

#Get the rolling ball size
rolling_ball_size = 50
#rolling_ball_size = int(input("Please enter the rolling ball size: "))

min_particle_px = 20

############   RIGHT NOW THIS LIST IS JUST A REFERENCE AND DOESN'T DO ANYTHING AUTOMATICALLY; THE "measurements_request_command" BELOW IS CURRENTLY WHAT IS ACTUALLY USED BY THE MACROS ##############

measure_area = True
measure_mean = True
measure_standard = True
measure_modal = False

#note "min" in the command
measure_minmax = True

measure_centroid = False
measure_center = False
measure_perimeter = False
measure_bounding = False
measure_fit = False
measure_shape= False

#note "feret's" in the command
measure_ferets = False

measure_integrated = False
measure_median = True
measure_skewness = False
measure_kurtosis = False

#note this is called by "area_fraction but comes out as "%area" in csv export, which is a hassle. Should rename column in macro if used
measure_area_fraction = True

measure_stack = False
measure_limit = False
measure_display = False
measure_invert = False
measure_scientific = False
measure_add = False
measure_nan = False
measure_redirect = "None"
measure_decimal=3

##############################################

####### Initializing some variables so we can define Fiji measurement processing commands dynamically ########
# pair_root = ''
# filename_before_well = ''
# well = ''
# position = int()
# channel = ''
# step = ''
# triplet = ''
# raw_folder_id_int = ''

# initialized_var_list = [pair_root, filename_before_well, well, position, channel, step, triplet, raw_folder_id_int]

##Once per thread commands

measurements_request_command = """
run("Set Measurements...", "area area_fraction mean standard min redirect=None decimal=3");
"""

results_io_command = """
run("Input/Output...", "jpeg=85 gif=-1 file=.csv use_file save_column");
"""

##Used in each macro commands


#!!!!!The post-measure, pre-export commands have to be modified at the top of the main script file because python global variables are not global and this is a sad.

# post_measure_pre_export_commands = """
# setResult("pair_root", 0, """ + pair_root + """);
# setResult("raw_folder_id_int, 0, """ + raw_folder_id_int + """);
# setResult("filename_before_well",0, """ + filename_before_well + """);
# setResult("well",0, """ + well + """);
# setResult("position",0, """ + str(position) + """);
# setResult("triplet",0, """ + triplet + """);
# """



ch00_pre_process_commands = """
run("16-bit");
run("Subtract Background...", "rolling=rolling_ball_size");
"""


ch01_pre_process_commands = """
run("16-bit");
run("Subtract Background...", "rolling=rolling_ball_size");
"""

ratio_image_create_commands = """
imageCalculator("Divide create 32-bit stack", ch01_title, ch00_title);
"""

ratio_image_process_commands = """
setAutoThreshold("Huang dark");
//run("Threashold", )
run("Create Selection");
//resetThreshold();
"""


######### SQLITE AWESOMENESS #################
### THIS WAS SUPPLANTED IN THE V07 RUNS BY A WORKFLOW MOSTLY RUN THROUGH A HIERARCHICAL TABLE/VIEW AND UPDATE SYSTEM WITHIN SQLITE.
### CODE RELATING TO THAT IS PROVIDED IN SEPARATE SQL FILES.
### THE LINES BELOW, INCLUDING UNCOMMENTED ONES, ARE NOT RELEVANT TO THE REFERENCE WORKFLOW FOR THE INITIAL PAPER SUBMISSION.


# # Connect to the SQLite database
# conn = sqlite3.connect('your_database.db')
# c = conn.cursor()
# 
# # Get a list of all distinct step values
# steps = [row[0] for row in c.execute('SELECT DISTINCT step FROM table')]
# 
# # Initialize an empty list to hold the select statements
# select_statements = []
# 
# # Loop through each step value and generate a separate SELECT statement for it
# for step in steps:
#     select_statement = f"SELECT * FROM table WHERE step = {step}"
#     select_statements.append(select_statement)
# 
# # Join the SELECT statements using the common column
# joined_select = "SELECT * FROM " + " JOIN ".join(select_statements) + " ON common_column"
# 
# # Execute the final joined SELECT statement
# results = c.execute(joined_select).fetchall()
# 
# # Close the database connection
# conn.close()


# This tells us, for each well and raw or pre_processed, the average of the quotient given by: mean pixel value of the whole ch01 image / mean pixel value of the ch00 image


#This gives us some statistics aggregated over each well



#What do we want?

#  For each exp_branch in position:
#   (raw, bs50, gb(n), (method)fromch00thresh, (method)fromch01thresh
#     ch00 mean_pixval, 
#     stdev
#     ch01 mean_pixval, 
#     stdev
#     ratio mean pixval, 
#     stdev
#     
#     for each well from positions:
#       position_equal, area_weighted
#     ch00 mean_pixval, 
#     inter-postition stdev
#     ch01 mean_pixval, 
#     inter-postition stdev
#     ratio mean pixval, 
#     inter-postition stdev
# 
#     for each triplet, from wells:
#       well_equal from position_equal, well_equal FROM pos_area_weighted, well_area_weighted THROUGH
#     ch00_mean_pixval,
#     inter-postition stdev
#     ch01 mean_pixval, 
#     inter-postition stdev
#     ratio mean pixval, 
#     inter-postition stdev
# 



pos_raw_wiqs_sql = ("""
CREATE VIEW pos_raw_wiqs AS
    SELECT ch00.filename_before_well,
           ch00.well,
           ch00.position,
           ch00.step,
           ch00.Area AS raw_ch00_area,
           ch01.Area AS raw_ch01_area,
           (ch01.pixval_mean / ch00.pixval_mean) AS pair_raw_wiq
      FROM batch_supertable AS ch00
           JOIN
           batch_supertable AS ch01 ON ch00.filename_before_well = ch01.filename_before_well AND 
                                       ch00.well = ch01.well AND 
                                       ch00.position = ch01.position AND 
                                       ch00.step = ch01.step
     WHERE ch00.channel = 'ch00' AND 
           ch01.channel = 'ch01' AND 
           ch00.step = 'raw'
     GROUP BY ch00.filename_before_well,
              ch00.well,
              ch00.position,
              ch00.step;

""")


pos_pp_wiqs_sql = ("""
CREATE VIEW pos_pp_wiqs AS
    SELECT ch00.filename_before_well,
           ch00.well,
           ch00.position,
           ch00.step,
           ch00.Area AS pp_ch00_area,
           ch01.Area AS pp_ch01_area,
           (ch01.pixval_mean / ch00.pixval_mean) AS pair_pp_wiq
      FROM batch_supertable AS ch00
           JOIN
           batch_supertable AS ch01 ON ch00.filename_before_well = ch01.filename_before_well AND 
                                       ch00.well = ch01.well AND 
                                       ch00.position = ch01.position AND 
                                       ch00.step = ch01.step
     WHERE ch00.channel = 'ch00' AND 
           ch01.channel = 'ch01' AND 
           ch00.step = 'pre_processed'
     GROUP BY ch00.filename_before_well,
              ch00.well,
              ch00.position,
              ch00.step;
""")


well_wiqs_sql = ("""
CREATE VIEW well_wiqs AS
    SELECT a.filename_before_well,
           a.well,
           avg(a.pair_raw_wiq) AS well_raw_wiq_mean,
           avg(b.pair_pp_wiq) AS well_pp_wiq_mean,
           sum(b.pair_pp_wiq * b.pp_ch00_area) / sum(b.pp_ch00_area) AS well_pp_wiq_ch00area_weighted_mean,
           sum(b.pair_pp_wiq * b.pp_ch01_area) / sum(b.pp_ch01_area) AS well_pp_wiq_ch01area_weighted_mean
      FROM pos_raw_wiqs AS a
           JOIN
           pos_pp_wiqs AS b ON a.filename_before_well = b.filename_before_well AND 
                               a.well = b.well
     GROUP BY a.filename_before_well,
              a.well
     ORDER BY a.filename_before_well,
              a.well;
""")


well_stats_long_sql = ("""
CREATE VIEW well_stats_long AS
    SELECT a.filename_before_well,
           a.well,
           a.channel,
           a.step,
           avg(a.pixval_mean) AS well_pixval_unweighted_mean,
           agg_select.sum_area AS well_pix_area_sum,
           sum(a.pixval_mean * a.Area / agg_select.sum_area) AS well_pixval_area_weighted_mean,
           sum(a.StdDev / count_pos) AS well_pixval_stdev,
           a.channel || '_' || a.step AS channel_step
      FROM batch_supertable AS a
           JOIN
           (
               SELECT filename_before_well,
                      well,
                      channel,
                      step,
                      sum(Area) AS sum_area,
                      count( * ) AS count_pos
                 FROM batch_supertable
                GROUP BY filename_before_well,
                         well,
                         channel,
                         step
           )
           AS agg_select ON agg_select.filename_before_well = a.filename_before_well AND 
                            agg_select.well = a.well AND 
                            agg_select.channel = a.channel AND 
                            agg_select.step = a.step
     GROUP BY a.filename_before_well,
              a.well,
              a.channel,
              a.step
     ORDER BY a.filename_before_well,
              a.well,
              a.channel,
              a.step;
""")


well_stats_wide_sql = ("""
CREATE VIEW well_stats_wide AS
    SELECT a.filename_before_well,
           a.well,
           ch00_raw.well_pixval_unweighted_mean AS ch00_raw_well_pixval_unweighted_mean,
           ch00_raw.well_pixval_stdev AS ch00_raw_well_pixval_stdev,
           ch00_pp.well_pixval_unweighted_mean AS ch00_pp_well_pixval_unweighted_mean,
           ch00_pp.well_pixval_area_weighted_mean AS ch00_pp_well_pixval_area_weighted_mean,
           ch00_pp.well_pixval_stdev AS ch00_pp_well_pixval_stdev,
           ch01_raw.well_pixval_unweighted_mean AS ch01_raw_well_pixval_unweighted_mean,
           ch01_raw.well_pixval_stdev AS ch01_raw_well_pixval_stdev,
           ch01_pp.well_pixval_unweighted_mean AS ch01_pp_well_pixval_unweighted_mean,
           ch01_pp.well_pixval_area_weighted_mean AS ch01_pp_well_pixval_area_weighted_mean,
           ch01_pp.well_pixval_stdev AS ch01_pp_well_pixval_stdev/*  */,
           ratio_img.well_pixval_unweighted_mean AS ratio_img_well_pixval_unweighted_mean,
           ratio_img.well_pixval_area_weighted_mean AS ratio_img_well_pixval_area_weighted_mean,
           ratio_img.well_pixval_stdev AS ratio_img_well_pixval_stdev/*  */,
           ratio_img_processed.well_pixval_unweighted_mean AS ratio_img_processed_well_pixval_unweighted_mean,
           ratio_img_processed.well_pixval_area_weighted_mean AS ratio_img_processed_well_pixval_area_weighted_mean,
           ratio_img_processed.well_pixval_stdev AS ratio_img_processed_well_pixval_stdev-- 
      FROM well_stats_long AS a
           JOIN
           (
               SELECT *
                 FROM well_stats_long
                WHERE channel = 'ch00' AND 
                      step = 'raw'
           )
           AS ch00_raw ON ch00_raw.filename_before_well = a.filename_before_well AND 
                          ch00_raw.well = a.well
           JOIN
           (
               SELECT *
                 FROM well_stats_long
                WHERE channel = 'ch00' AND 
                      step = 'pre_processed'
           )
           AS ch00_pp ON ch00_pp.filename_before_well = a.filename_before_well AND 
                         ch00_pp.well = a.well
           JOIN
           (
               SELECT *
                 FROM well_stats_long
                WHERE channel = 'ch01' AND 
                      step = 'raw'
           )
           AS ch01_raw ON ch01_raw.filename_before_well = a.filename_before_well AND 
                          ch01_raw.well = a.well
           JOIN
           (
               SELECT *
                 FROM well_stats_long
                WHERE channel = 'ch01' AND 
                      step = 'pre_processed'
           )
           AS ch01_pp ON ch01_pp.filename_before_well = a.filename_before_well AND 
                         ch01_pp.well = a.well
           JOIN
           (
               SELECT *
                 FROM well_stats_long
                WHERE channel = 'both' AND 
                      step = 'ratio_img'
           )
           AS ratio_img ON ratio_img.filename_before_well = a.filename_before_well AND 
                           ratio_img.well = a.well
           JOIN
           (
               SELECT *
                 FROM well_stats_long
                WHERE channel = 'both' AND 
                      step = 'ratio_img_processed'
           )
           AS ratio_img_processed ON ratio_img_processed.filename_before_well = a.filename_before_well AND 
                                     ratio_img_processed.well = a.well
     GROUP BY a.filename_before_well,
              a.well;
""")

exp_stats_direct_from_pos_long_sql = ("""
CREATE VIEW exp_stats_direct_from_pos_long AS
    SELECT a.filename_before_well,
           a.channel,
           a.step,
           avg(a.pixval_mean) AS exp_pixval_unweighted_mean,
           sum(a.pixval_mean * a.Area / agg_select.sum_area) AS exp_pixval_area_weighted_mean,
           sum(a.StdDev / count_pos) AS exp_pixval_stdev,
           a.channel || '_' || a.step AS channel_step
      FROM batch_supertable AS a
           JOIN
           (
               SELECT filename_before_well,
                      channel,
                      step,
                      sum(Area) AS sum_area,
                      count( * ) AS count_pos
                 FROM batch_supertable
                GROUP BY filename_before_well,
                         channel,
                         step
           )
           AS agg_select ON agg_select.filename_before_well = a.filename_before_well AND 
                            agg_select.step = a.step
     GROUP BY a.filename_before_well,
              a.channel,
              a.step
     ORDER BY a.filename_before_well,
              a.channel,
              a.step;
""")


exp_stats_direct_from_pos_wide_sql = ("""
 CREATE VIEW exp_stats_direct_from_pos_wide AS
    SELECT a.filename_before_well,
           ch00_raw.exp_pixval_unweighted_mean AS ch00_raw_exp_pixval_unweighted_mean,
           ch00_raw.exp_pixval_stdev AS ch00_raw_exp_pixval_stdev,
           ch00_pp.exp_pixval_unweighted_mean AS ch00_pp_exp_pixval_unweighted_mean,
           ch00_pp.exp_pixval_area_weighted_mean AS ch00_pp_exp_pixval_area_weighted_mean,
           ch00_pp.exp_pixval_stdev AS ch00_pp_exp_pixval_stdev,
           ch01_raw.exp_pixval_unweighted_mean AS ch01_raw_exp_pixval_unweighted_mean,
           ch01_raw.exp_pixval_stdev AS ch01_raw_exp_pixval_stdev,
           ch01_pp.exp_pixval_unweighted_mean AS ch01_pp_exp_pixval_unweighted_mean,
           ch01_pp.exp_pixval_area_weighted_mean AS ch01_pp_exp_pixval_area_weighted_mean,
           ch01_pp.exp_pixval_stdev AS ch01_pp_exp_pixval_stdev/*  */,
           ratio_img.exp_pixval_unweighted_mean AS ratio_img_exp_pixval_unweighted_mean,
           ratio_img.exp_pixval_area_weighted_mean AS ratio_img_exp_pixval_area_weighted_mean,
           ratio_img.exp_pixval_stdev AS ratio_img_exp_pixval_stdev/*  */,
           ratio_img_processed.exp_pixval_unweighted_mean AS ratio_img_processed_exp_pixval_unweighted_mean,
           ratio_img_processed.exp_pixval_area_weighted_mean AS ratio_img_processed_exp_pixval_area_weighted_mean,
           ratio_img_processed.exp_pixval_stdev AS ratio_img_processed_exp_pixval_stdev-- 
      FROM exp_stats_direct_from_pos_long AS a
           JOIN
           (
               SELECT *
                 FROM exp_stats_direct_from_pos_long
                WHERE channel = 'ch00' AND 
                      step = 'raw'
           )
           AS ch00_raw ON ch00_raw.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_direct_from_pos_long
                WHERE channel = 'ch00' AND 
                      step = 'pre_processed'
           )
           AS ch00_pp ON ch00_pp.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_direct_from_pos_long
                WHERE channel = 'ch01' AND 
                      step = 'raw'
           )
           AS ch01_raw ON ch01_raw.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_direct_from_pos_long
                WHERE channel = 'ch01' AND 
                      step = 'pre_processed'
           )
           AS ch01_pp ON ch01_pp.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_direct_from_pos_long
                WHERE channel = 'both' AND 
                      step = 'ratio_img'
           )
           AS ratio_img ON ratio_img.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_direct_from_pos_long
                WHERE channel = 'both' AND 
                      step = 'ratio_img_processed'
           )
           AS ratio_img_processed ON ratio_img_processed.filename_before_well = a.filename_before_well
     GROUP BY a.filename_before_well;

""")

exp_stats_from_wells_long_sql = ("""
CREATE VIEW exp_stats_from_wells_long AS
    SELECT wl.filename_before_well,
           wl.channel,
           wl.step,
           avg(wl.well_pixval_unweighted_mean) AS exp_wells_pixval_unweighted_mean,
           avg(wl.well_pixval_area_weighted_mean) AS exp_unweighted_avg_of_wells_area_weighted_means,
           sum(wl.well_pix_area_sum) AS exp_pix_area_sum,
           sum(wl.well_pixval_area_weighted_mean * wl.well_pix_area_sum / agg_select.sum_area) AS exp_wells_pixval_area_weighted_mean,
           sum(wl.well_pixval_stdev / count_pos) AS exp_stdev_of_wells_pixvals
      FROM well_stats_long AS wl
           JOIN
           (
               SELECT filename_before_well,
                      channel,
                      step,
                      sum(Area) AS sum_area,
                      count( * ) AS count_pos
                 FROM batch_supertable
                GROUP BY filename_before_well,
                         channel,
                         step
           )
           AS agg_select ON agg_select.filename_before_well = wl.filename_before_well AND 
                            agg_select.channel = wl.channel AND 
                            agg_select.step = wl.step
     GROUP BY wl.filename_before_well,
              wl.channel,
              wl.step;
""")

exp_stats_from_wells_wide_sql = ("""
CREATE VIEW exp_stats_from_wells_wide AS
    SELECT a.filename_before_well,
           ch00_raw.exp_wells_pixval_unweighted_mean AS ch00_raw_exp_pixval_unweighted_mean,
           ch00_raw.exp_stdev_of_wells_pixvals AS ch00_exp_stdev_of_wells_pixvals,
           ch00_pp.exp_wells_pixval_unweighted_mean AS ch00_pp_exp_wells_pixval_unweighted_mean,
           ch00_pp.exp_wells_pixval_area_weighted_mean AS ch00_pp_exp_wells_pixval_area_weighted_mean,
           ch00_pp.exp_unweighted_avg_of_wells_area_weighted_means AS ch00_pp_exp_unweighted_avg_of_wells_area_weighted_means,
           ch00_pp.exp_stdev_of_wells_pixvals AS ch00_pp_exp_stdev_of_wells_pixvals,
           ch00_pp.exp_pix_area_sum AS ch00_pp_exp_pix_area_sum,
           ch01_raw.exp_wells_pixval_unweighted_mean AS ch01_raw_exp_pixval_unweighted_mean,
           ch01_raw.exp_stdev_of_wells_pixvals AS ch01_raw_exp_stdev_of_wells_pixvals,
           ch01_pp.exp_wells_pixval_unweighted_mean AS ch01_pp_exp_wells_pixval_unweighted_mean,
           ch01_pp.exp_wells_pixval_area_weighted_mean AS ch01_pp_exp_pexp_wells_pixval_area_weighted_mean,
           ch01_pp.exp_unweighted_avg_of_wells_area_weighted_means AS ch01_pp_exp_unweighted_avg_of_wells_area_weighted_means,
           ch01_pp.exp_stdev_of_wells_pixvals AS ch01_pp_exp_stdev_of_wells_pixvals,
           ch01_pp.exp_pix_area_sum AS ch01_pp_exp_pix_area_sum,
           ratio_img.exp_wells_pixval_unweighted_mean AS ratio_img_exp_wells_pixval_unweighted_mean,
           ratio_img.exp_wells_pixval_area_weighted_mean AS ratio_img_exp_wells_pixval_area_weighted_mean,
           ratio_img.exp_unweighted_avg_of_wells_area_weighted_means AS ratio_img_exp_unweighted_avg_of_wells_area_weighted_means,
           ratio_img.exp_stdev_of_wells_pixvals AS ratio_img_exp_stdev_of_wells_pixvals,
           ratio_img.exp_pix_area_sum AS ratio_img_exp_pix_area_sum-- 
      /* -- */FROM exp_stats_from_wells_long AS a
           JOIN
           (
               SELECT *
                 FROM exp_stats_from_wells_long
                WHERE channel = 'ch00' AND 
                      step = 'raw'
           )
           AS ch00_raw ON ch00_raw.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_from_wells_long
                WHERE channel = 'ch00' AND 
                      step = 'pre_processed'
           )
           AS ch00_pp ON ch00_pp.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_from_wells_long
                WHERE channel = 'ch01' AND 
                      step = 'raw'
           )
           AS ch01_raw ON ch01_raw.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_from_wells_long
                WHERE channel = 'ch01' AND 
                      step = 'pre_processed'
           )
           AS ch01_pp ON ch01_pp.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_from_wells_long
                WHERE channel = 'both' AND 
                      step = 'ratio_img'
           )
           AS ratio_img ON ratio_img.filename_before_well = a.filename_before_well
           JOIN
           (
               SELECT *
                 FROM exp_stats_from_wells_long
                WHERE channel = 'both' AND 
                      step = 'ratio_img_processed'
           )
           AS ratio_img_processed ON ratio_img_processed.filename_before_well = a.filename_before_well
     GROUP BY a.filename_before_well;
-- , ratio_img_processed.exp_wells_pixval_unweighted_mean as ratio_img_processed_exp_wells_pixval_unweighted_mean-- , ratio_img_processed.exp_pixval_area_weighted_mean as ratio_img_processed_exp_pixval_area_weighted_mean-- , ratio_img_processed.exp_unweighted_avg_of_wells_area_weighted_means as ratio_img_processed_exp_unweighted_avg_of_wells_area_weighted_means-- , ratio_img_processed.exp_stdev_of_wells_pixvals as ratio_img_processed_exp_stdev_of_wells_pixvals-- , ratio_img_processed.exp_pix_area_sum as ratio_img_processed_exp_pix_area_sum-- -- -- 

""")


postcreate_sql_list = [pos_raw_wiqs_sql, pos_pp_wiqs_sql, well_wiqs_sql, well_stats_long_sql, well_stats_wide_sql, exp_stats_direct_from_pos_long_sql, exp_stats_direct_from_pos_wide_sql, exp_stats_from_wells_long_sql, exp_stats_from_wells_wide_sql]


export_items = ['batch_supertable', 'well_stats_long', 'well_stats_wide', 'exp_stats_direct_from_pos_long', 'exp_stats_direct_from_pos_wide', 'exp_stats_from_wells_wide', 'exp_stats_from_wells_long']


