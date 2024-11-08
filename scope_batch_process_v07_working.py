## TO DO
# 
# Fix the Threshold call method
# Config: list of threshold methods to try
# Config: Measurements desired, custom fields?

# QC the CSV stats; explicitly close results pages?

# Verify image pairs in raw folder before processing
# Walk over a filetree to multiple raw folders
# Console output save, including sub-tees in the thread script
# Fiji info save
# Open sample images after a run
# Create mosaics


####### Initializing some variables so we can define Fiji measurement processing commands dynamically ########
pair_root = ''
filename_before_well = ''
well = ''
position = int()
channel = ''
step = ''
channel_step = ''
triplet = ''
raw_folder_id_int = ''




import sys
import os
import time
from datetime import datetime
import threading
import subprocess
import math
import csv
import itertools
import shutil
import argparse
import importlib

import sqlite3
import pandas
#import sqlalchemy

import default_config_pointer
#import csvkit
from default_config_pointer import default_config_file
#import scope_process_config
#from scope_process_config import *


#def copy_this

#def batch_folder_setup ()

#def batch_folder_teardown ()

#Get a timestamp that will be appended to lots of folders and such
timestamp_string = datetime.now().strftime('%Y%m%d_%H%M%S')

#Copy this script for later archiving before any other variables get imported that might mess with "__file__"
temp_script_copy_file = __file__.replace('.py','') + '_'+timestamp_string + '.py'
archived_script_filename = os.path.basename(temp_script_copy_file)
shutil.copy(__file__, temp_script_copy_file)


#Set up a folder to manage items that don't belong to just one individual raw images folder

batch_superfolder = os.path.join(os.path.dirname(__file__), 'batch_superfolder_' + timestamp_string)
os.mkdir(batch_superfolder)
batch_super_parameters = os.path.join(batch_superfolder, 'batch_super_parameters')
os.mkdir(batch_super_parameters)
batch_super_stats = os.path.join(batch_superfolder, 'batch_super_stats')
os.mkdir(batch_super_stats)
batch_super_macros = os.path.join(batch_super_parameters, 'batch_super_macros')
os.mkdir(batch_super_macros)
batch_bigcsv_subfolder = os.path.join(batch_super_stats, 'big_csvs')
os.mkdir(batch_bigcsv_subfolder)
batch_super_logs = os.path.join(batch_super_parameters, 'batch_super_logs')
os.mkdir(batch_super_logs)
batch_super_samples = os.path.join(batch_superfolder, 'batch_super_samples')
os.mkdir(batch_super_samples)

batch_super_csv = os.path.join(batch_super_stats, 'batch_super_csv_'+timestamp_string + '.csv')
batch_super_csv_exp = os.path.join(batch_super_stats, 'batch_super_csv_exp'+timestamp_string + '.csv')
batch_super_db = os.path.join(batch_super_stats, 'batch_super_db_'+timestamp_string + '.db')
batch_xlsx = os.path.join(batch_super_stats, 'batch_sheet_results_' + timestamp_string + '.xlsx')
batch_super_sr_folderlist = os.path.join(batch_super_parameters, 'batch_script_run_folderlist.py')
batch_super_rawfiles_list = os.path.join(batch_super_parameters, 'batch_super_rawfiles_list')
pngify_macro = os.path.join(batch_super_parameters, 'pngify.ijm')



#print('DCF is '+default_config_file)
parser = argparse.ArgumentParser(description='Personalize the config.')
parser.add_argument('-c', '--config_file',  default= default_config_file)
parser.add_argument('-r', '--raw_folder',  type=os.path.abspath, default='config_file_raw_folder')

args = parser.parse_args()
config_file_string = args.config_file.replace('.py','')
config_file_name = config_file_string




### To do: accept fullpath to a config file if it is outside the script directory
#config_file_base = os.path.basename(config_file_string)
print('Config file in use is ' + config_file_name)
try:
    #with open(config_file_name + ".py", "r") as f:
        #module_code = f.read()
    #module_globals = {}
    #exec(module_code, module_globals)
    #globals().update(module_globals)
    config_file_import = importlib.import_module(config_file_name, package=None)
    globals().update(vars(config_file_import))
    #config_file_import = importlib.import_module(scope_process_config)
    #config_module = getattr(sys.modules[__name__], config_file_name)
    #print(config_module)
    #from config_file_import import *
    #globals().update(vars(config_file_import))
    #from config_file_import import *
except:
    print('Tried to load config file '+ config_file_name + ' in the same folder as the script, but failed. If \'-c\' or \'--config_file\' was used as an option when running the script, then it did not point to a valid config file. Otherwise, the entry in the \'default_config_pointer\' file (found in the same folder as the script) does not name a valid config file.')
        
    exit(1)    
    


# Get the raw folder list as either the folder entered in the command line or the list of folders in the config file
raw_folder_arg = args.raw_folder
#print('raw folder arg is ' + raw_folder_arg)
if os.path.basename(raw_folder_arg) == 'config_file_raw_folder':
    print('No command line option given for raw images location. Using location list in the config file: ' + repr(batch_folders_list))
    if not os.path.exists(batch_folders_list[0]):
        print('Config filw raw folder option does not point to a valid folder. Exiting.')
        os.remove(temp_script_copy_file)
        sys.exit(1)
else:   
    if os.path.exists(raw_folder_arg):
        batch_folders_list = []
        batch_folders_list.append(raw_folder_arg)
        print('Processing images in command line option location: ' + raw_folder_arg)
    else:
        print('Command line option gave an invalid path to a raw images folder. Exiting.')
        os.remove(temp_script_copy_file)
        sys.exit(1)

#Quick function to reconstruct macro filenames from pair roots
def pair_macro_file_from_pair_root(pair_root):
    macro_name = pair_root + '_process_macro.ijm'
    macro_fullpath = os.path.join(macros_folder, macro_name)
    return(macro_fullpath)


############START OF BLOCK WRITING LOOPING THROUGH THE BATCH FOLDERS##############
#scropt_output_folders = []

batch_pair_roots = []
batchpaths_macro_dirs = []
batchpaths_sub_csv_dirs = []
batchpaths_bigcsv_files = []
batchpaths_bigcsv_and_subdir = []
batchpaths_sample_dirs = []
batchpaths_param_dirs = []
batchpaths_outimage_dirs = []
batchpaths_log_dirs = []
batch_script_copy_dirs = []

pair_macro_file_dict = {}

script_output_folders_list = []

print(repr(batch_folders_list))

raw_folder_id_int = 0
for raw_folder_id_int, raw_images_folder in enumerate(batch_folders_list):
    raw_images_parent = os.path.split(raw_images_folder)[0]
    script_output_folder = os.path.join (raw_images_parent, 'script_run_' + timestamp_string)
    script_output_folders_list.append(script_output_folder)
    #script_output_folders.appent(script_output_folder)
    #print(raw_images_parent)
    output_images_folder = os.path.join(script_output_folder, 'images')
    batchpaths_outimage_dirs.append(output_images_folder)
    
    pre_processed_images_folder = os.path.join(output_images_folder, 'pre_processed')
    ratio_images_folder = os.path.join(output_images_folder, 'ratios')
    ratio_images_processed_folder = os.path.join(output_images_folder, 'ratios_processed')
    sum_images_folder = os.path.join(output_images_folder, 'sums')
    
    
    stats_folder = os.path.join(script_output_folder, 'stats')
    
    sub_csv_folder = os.path.join(stats_folder, 'measure_outputs')
    batchpaths_sub_csv_dirs.append(sub_csv_folder)
    
    big_csv_fullpath = os.path.join(stats_folder, 'all_measure_outputs.csv')
    batchpaths_bigcsv_files.append(big_csv_fullpath)
    
    batchpaths_bigcsv_and_subdir.append([big_csv_fullpath, sub_csv_folder])
    
    parameters_folder = os.path.join(script_output_folder, 'parameters')
    batchpaths_param_dirs.append(batchpaths_param_dirs)
    
    macros_folder = os.path.join(parameters_folder, 'macros')
    batchpaths_macro_dirs.append(macros_folder)
    
    logs_folder = os.path.join(parameters_folder, 'logs')
    batchpaths_log_dirs.append(logs_folder)


    this_script_copy_fullpath = os.path.join(parameters_folder, archived_script_filename)
    batch_script_copy_dirs.append(this_script_copy_fullpath)
    
    #print('copy this script to ' + this_script_copy_fullpath)
    config_file_with_ext = config_file_string + '.py'
    config_origin_fullpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_file_with_ext)
    config_copy_fullpath = os.path.join(parameters_folder, config_file_string + timestamp_string + '.py')
    console_output_fullpath = os.path.join(parameters_folder, 'console_output.txt')


    with open(batch_super_sr_folderlist, 'a') as b:
        b.write(script_output_folder + '\n')
    b.close()

    #Define a list to hold the names of the pairs of files
    pair_roots = []


    #Get filenames minus the channel part at the end
    #Remove duplicates from the list of pair roots


    with open(batch_super_rawfiles_list, 'a') as r:
    
        for filename in os.listdir(raw_images_folder):
            if filename.endswith('tif'):
                try:
                    pair_root = filename.split('_ch')[0]
                    position = int(pair_root.split("Position")[1])
                    pair_exp_well = pair_root.split(" Position")[0]
                    pair_roots.append(pair_root)
                    r.write(filename + '\n')
                except:
                    print('In folder ' + raw_images_folder + ' , ' + filename + ' had an unexpected format and was excluded') 
            else:
                print('In folder ' + raw_images_folder + ' , ' + filename + ' had an unexpected format and was excluded') 
    r.close()


    
    pair_roots = list(dict.fromkeys(pair_roots))


    #Create the output folders if they don't already exist
    #print('setting up output directories')
    for output_folder in [
    script_output_folder, 
    output_images_folder,
    ratio_images_folder, 
    pre_processed_images_folder,
    ratio_images_processed_folder,
    parameters_folder,
    macros_folder,
    stats_folder,
    sub_csv_folder,
    logs_folder
    ]:
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)

    #copy this file into the output folder
    shutil.copy(temp_script_copy_file, this_script_copy_fullpath)

    #copy the config file into the output folder
    shutil.copy(config_origin_fullpath, config_copy_fullpath)

        

    #print('pair_roots_list: ' + repr(pair_roots))
    #print(pair_roots)

    #Divide the pair roots into a number of sub-lists corresponding to the max number of threads
    #print(
    #pair_roots_threaded = list(itertools.zip_longest(*[iter(pair_roots)] * min(len(pair_roots),(len(pair_roots)//(os.cpu_count() + 2)))))


    #Start the clock
    start_time = time.time()

    #Define a list to hold the threads
    threads = []
    sampled_triplets = []
    #Initialize the csvs and write the macros for individual image pairs
    for pair_root in sorted(pair_roots):
        sample_flag = "false"
        #print('pair_root: ' + pair_root + '\n')
        batch_pair_roots.append(pair_root)
        #print(pair_root)
        position = int(pair_root.split("Position")[1])
        pair_exp_well = pair_root.split(" Position")[0]
        well = pair_exp_well.rsplit('_',1)[1]
        #print('well: ' + well + '\n')
        exp_maybe = pair_exp_well.rsplit('_',1)[0]
        #print('exp_maybe: ' + exp_maybe + '\n')
        if well == exp_maybe.rsplit('_',1)[1]:
	        filename_before_well = exp_maybe.rsplit('_',1)[0]
        else:
	        filename_before_well = exp_maybe
        plate_grid_let = [*well][0]
        plate_grid_num = int([*well][1])
        
        triplet = ''
        
        if plate_grid_let in ['B','C','D']:
            triplet = 'BCD' + str(plate_grid_num)
        
        if plate_grid_let in ['E','F','G']:
            triplet = 'EFG' + str(plate_grid_num)
            
        if triplet == '':
            print('unexpected plate location for ' + pair_root + ' . Data processing may fail.')
        
        if triplet in sampled_triplets:
            sample_flag = "false"
        else:
            sample_flag = "true"
            sampled_triplets.append(triplet);

            
        ch00_raw_filename = pair_root + '_ch00.tif'
        ch01_raw_filename = pair_root + '_ch01.tif'
        
        ch00_raw_fullpath = os.path.join(raw_images_folder, ch00_raw_filename)
        ch01_raw_fullpath = os.path.join(raw_images_folder, ch01_raw_filename)
        
        ch00_raw_filename_noext = pair_root + '_ch00'
        ch01_raw_filename_noext = pair_root + '_ch01'
        
        #ch00_pre_processed_fullpath = os.path.join(pre_processed_images_folder, pair_root +'pp_ch00.tif')
        #ch01_pre_processed_fullpath = os.path.join(pre_processed_images_folder, pair_root + 'pp_ch01.tif')
        ch00_prep_filename = pair_root + '_ch00_prep.tif'
        ch01_prep_filename = pair_root + '_ch01_prep.tif'
        
        ch00_prep_fullpath = ratio_fullpath = os.path.join(pre_processed_images_folder, ch00_prep_filename)
        ch01_prep_fullpath = ratio_fullpath = os.path.join(pre_processed_images_folder, ch01_prep_filename)



        #Create a name for the ratio image output
        ratio_raw_filename = pair_root + '_ratio_raw.tif'
        sum_raw_filename = pair_root + '_sum_raw.tif'
        
        ratio_raw_fullpath = os.path.join(ratio_images_folder, ratio_raw_filename)
        sum_raw_fullpath = os.path.join(sum_images_folder, sum_raw_filename)
        
        ratio_prep_filename = pair_root + '_ratio_prep.tif'
        sum_prep_filename = pair_root + '_sum_prep.tif'

        ratio_prep_fullpath = os.path.join(ratio_images_folder, ratio_prep_filename)
        sum_prep_fullpath = os.path.join(sum_images_folder, sum_prep_filename)        
        
        #ratio_processed_fullpath = os.path.join(ratio_images_processed_folder, ratio_processed_filename)
        #print('RPF = ' + ratio_processed_fullpath)
        pair_process_macro_fullpath = pair_macro_file_from_pair_root(pair_root)
        pair_macro_file_dict[pair_root] = pair_process_macro_fullpath
        pair_csv_base_fullpath = os.path.join(sub_csv_folder, pair_root + '_stats')
        
        #This is for trying separate macros to do the different steps for a pair. Takes too much time / is unnecessary
        #raw_process_macro = pair_root + '_process_raw.ijm'
        #ratio_process_macro = pair_root + '_process_ratio.ijm'
        #pair_process_macro_name = pair_root + '_process_macro.ijm'
        #pair_process_macro_fullpath = os.path.join(macros_folder, pair_process_macro_name)
        

        
        #This is for initializing a csv for all the results outputs from each pair macro to go in together, which is hilariously hard to do in imageJ
        #pair_csv_fullpath_generic = os.path.join(sub_csv_folder, pair_root + '_stats.csv')
        #Initialize CSV
        #print('initializing csv at' + pair_csv_fullpath_generic)
        #with open(pair_csv_fullpath, 'w', newline='') as c:
        #    c.write("""ij_row,area,mean,min,max,pair_root,channel,step,""")
        #    c.write("\n")
        #c.close()
        
        #This is the imageJ macro that will be written and executed to process the particular pair of images the loop is working on.
#         post_measure_pre_export_commands = """
# setResult("pair_root", 0, \"""" + pair_root + """\");
# setResult("raw_folder_id_int", 0, \"""" + str(raw_folder_id_int) + """\");
# setResult("filename_before_well",0, \"""" + filename_before_well + """\");
# setResult("well",0, \"""" + well + """\");
# setResult("position",0, \"""" + str(position) + """\");
# setResult("triplet",0, \"""" + triplet + """\");
# """

        with open(pair_process_macro_fullpath, 'w') as f:
            f.write("""
//Turn python script variables into imagej macro variables

            
//Some terrible GPT3 suggestions to get division to work properly
//import ij;
//import ij.ImagePlus;
//import ij.plugin.ImageCalculator;
//ic = new imageCalculator();
//ic.setDivideZero(0.0);
            
setBatchMode(true);

//Convert python script variable values into macro variable values

pair_root = \"""" + pair_root + """\";
sample_flag = \"""" + sample_flag + """\";
position = \"""" + str(position) + """\";
exp_well = \"""" + pair_exp_well + """\";
triplet = \"""" + triplet + """\";
filename_before_well = \"""" + filename_before_well + """\";
well = \"""" + well + """\";
pair_csv_base_fullpath = \"""" + pair_csv_base_fullpath + """\";
ch00_raw_fullpath = \"""" + ch00_raw_fullpath + """\";
ch01_raw_fullpath = \"""" + ch01_raw_fullpath + """\";
ch00_raw_filename_noext = \"""" + ch00_raw_filename_noext + """\";
ch01_raw_filename_noext = \"""" + ch01_raw_filename_noext + """\";


ch00_prep_fullpath = \"""" + ch00_prep_fullpath + """\";
ch01_prep_fullpath = \"""" + ch01_prep_fullpath + """\";
ratio_raw_fullpath = \"""" + ratio_raw_fullpath + """\";
ratio_prep_fullpath = \"""" + ratio_prep_fullpath + """\";
sum_raw_fullpath = \"""" + sum_raw_fullpath + """\";
sum_prep_fullpath = \"""" + sum_prep_fullpath + """\";


rolling_ball_size = \"""" + str(rolling_ball_size) + """\";
min_particle_px = \"""" + str(min_particle_px) + """\";

thr_methods = newArray("Otsu","Huang");
gb_values = newArray(1,2,3);
preparray = newArray("raw", "prep");

print("Working on image pair " + pair_root);
            
            
var meas_count= 0;
var process_level = 0;
var targ_channel="init";
var targ_prep="init";
var targ_blur="init";
var targ_roi_type="init";
var roisrc_channel="init";
var roisrc_prep="init";
var roisrc_blur="init";
var roisrc_tmethod="init";
var roisrc_particle="init";


function clearTargInfo() {
    targ_channel="none";
    targ_prep="none";
    targ_blur="none";
    targ_roi_type="none";
}


function clearRoisrcInfo() {
    roisrc_channel="none";
    roisrc_prep="none";
    roisrc_blur="none";
    roisrc_tmethod="none";
    roisrc_particle="none";
}

//These concat variables are set by the function that follows and don't need to be handled manually
var targ_str = "init";
var roisrc_str = "init";
var meas_str = "init";
var meas_str_no_blurs = "init";
var meas_str_no_tmethod = "init";
var meas_str_no_targ_channel = "init";
var meas_str_no_roisrc_channel = "init";
var meas_str_no_preps = "init";

function setMeasStrs() {
targ_str = targ_channel + "_" + targ_prep  + "_" + targ_blur + "_" + targ_roi_type;

roisrc_str = roisrc_channel + "_" + roisrc_prep + "_" + roisrc_blur + "_" + roisrc_tmethod + "_" + roisrc_particle;

meas_str = "TARG_" + targ_str + "_ROISRC_" + roisrc_str;

meas_str_no_blurs = "TARG_" + targ_channel + "_" + targ_prep + "_" + targ_roi_type + "_ROISRC_" + roisrc_channel + "_" + roisrc_prep + "_" + roisrc_tmethod + "_" + roisrc_particle;

meas_str_no_tmethod = "TARG_" + targ_str +  "_ROISRC_" + roisrc_channel + "_" + roisrc_prep + "_" + roisrc_blur + "_" + roisrc_particle;

meas_str_no_targ_channel = "TARG_" + targ_prep + "_" + targ_blur + "_" + targ_roi_type + "_ROISRC_" + roisrc_channel + "_" + roisrc_prep  + "_" + roisrc_blur + "_" + roisrc_tmethod + "_" + roisrc_particle;

meas_str_no_roisrc_channel = "TARG_" + targ_channel + "_" + targ_prep + "_" + targ_blur + "_" + targ_roi_type + "_ROISRC_" + roisrc_prep  + "_" + roisrc_blur + "_" + roisrc_tmethod + "_" + roisrc_particle;

meas_str_no_preps = "TARG_" + targ_channel + "_" + targ_blur + "_" + targ_roi_type + "_ROISRC_" + roisrc_channel + "_" + roisrc_blur + "_" + roisrc_tmethod + "_" + roisrc_particle;
}




function setMeasure() {
	meas_count = meas_count + 1;
	run("Measure");
	setResult("pair_root", 0, pair_root);
	setResult("filename_before_well",0,filename_before_well);
	setResult("triplet", 0, triplet);
	setResult("well", 0, well);
	setResult("position", 0, position);
	setResult("process_level",0,process_level);
    setMeasStrs();
    setResult("targ_str",0,targ_str);
    setResult("roisrc_str",0,roisrc_str);
    setResult("meas_str",0,meas_str);
	setResult("targ_channel", 0, targ_channel);
	setResult("targ_prep", 0, targ_prep);
	setResult("targ_blur", 0, targ_blur);
	setResult("targ_roi_type", 0, targ_roi_type);
	setResult("roisrc_channel", 0, roisrc_channel);
	setResult("roisrc_prep", 0, roisrc_prep);
	setResult("roisrc_blur", 0, roisrc_blur);
	setResult("roisrc_tmethod", 0, roisrc_tmethod);
    setResult("roisrc_particle", 0, roisrc_particle);
    setResult("meas_str_no_blurs", 0, meas_str_no_blurs);
    setResult("meas_str_no_tmethod", 0, meas_str_no_tmethod);
    setResult("meas_str_no_targ_channel", 0, meas_str_no_targ_channel);
    setResult("meas_str_no_roisrc_channel", 0, meas_str_no_roisrc_channel);
    setResult("meas_str_no_preps", 0, meas_str_no_preps);


	setResult("sample_flag",0,sample_flag);
	setResult("meas_count",0,meas_count);
	print("Saving results for string " + meas_str);
	saveAs("Results", pair_csv_base_fullpath + toString(meas_count) + ".csv");
}

clearTargInfo();
clearRoisrcInfo();
process_level = 1;
targ_channel="ch00";
targ_prep="raw";
targ_blur="noblur";
targ_roi_type="whole";

open(ch00_raw_fullpath);
ch00_title = getInfo("image.title");
setMeasure();


targ_channel = "ch01";

open(ch01_raw_fullpath);
ch01_title = getInfo("image.title");
channel = "ch01";
setMeasure();

targ_channel = "ratio";

imageCalculator("Divide create 32-bit", ch01_title, ch00_title);
saveAs("Tiff", ratio_raw_fullpath);
rename(pair_root + "_ratio_raw");
bs_ratio_img_title = getInfo("image.title");
setMeasure();


process_level = 2;
targ_prep = "bss50";

targ_channel = "ch00";

selectWindow(ch00_title);
run("16-bit");
run("Subtract Background...", "rolling="+toString(rolling_ball_size) + " sliding");
saveAs("Tiff", ch00_prep_fullpath);
ch00_title = getInfo("image.title");

setMeasure();


targ_channel = "ch01";

selectWindow(ch01_title);
run("16-bit");
run("Subtract Background...", "rolling=" + toString(rolling_ball_size) + " sliding");
saveAs("Tiff", ch01_prep_fullpath);
ch01_title = getInfo("image.title");
setMeasure();


targ_channel = "ratio";

imageCalculator("Divide create 32-bit", ch01_title, ch00_title);
rename(pair_root + "_ratio_prep");
//saveAs("Tiff", ratio_prep_fullpath);
setMeasure();

targ_channel = "sum";

imageCalculator("Add create 32-bit", ch01_title, ch00_title);
rename(pair_root + "_sum_prep");
//saveAs("Tiff", sum_prep_fullpath);
setMeasure();

close("*");
    


for (p=0; p<preparray.length; p++) {
    prep_var = preparray[p];
    if (prep_var == "raw") {
        prep_branch = "raw";
        ch00_branch_fullpath = ch00_raw_fullpath;
        ch01_branch_fullpath = ch01_raw_fullpath;
        //ratio_branch_fullpath = ratio_raw_fullpath;
        //sum_branch_fullpath = sum_raw_fullpath;
        } 
        else {
            if (prep_var == "prep") {
                prep_branch = "bss50";
                ch00_branch_fullpath = ch00_prep_fullpath;
                ch01_branch_fullpath = ch01_prep_fullpath;
                //ratio_branch_fullpath = ratio_prep_fullpath;
                //sum_branch_fullpath = sum_prep_fullpath;
            } else {
                continue;
            }
        }


    for (g=0; g<gb_values.length; g++) {
        close("*");
        gb_sigma = gb_values[g];
        process_level = 2;
        clearTargInfo();
        clearRoisrcInfo();
        
        targ_prep = prep_branch;
        targ_blur = "gb_" + toString(gb_sigma);
        targ_roi_type = "whole";
        

        
        open(ch00_branch_fullpath);
        ch00_title = getInfo("image.title");
        
        open(ch01_branch_fullpath);
        ch01_title = getInfo("image.title");
        
        //open(ratio_raw_fullpath);
        //ratio_title = getInfo("image.title");
        
        //open(sum_branch_fullpath);
        //sum_title = getInfo("image.title");

        targ_channel = "ch00";
        
        selectWindow(ch00_title);
        run("Gaussian Blur...", "sigma="+gb_sigma);
        setMeasure();
        
        targ_channel = "ch01";
        selectWindow(ch01_title);
        run("Gaussian Blur...", "sigma="+gb_sigma);
        setMeasure();
        
        targ_channel = "ratio";
        imageCalculator("Divide create 32-bit", ch01_title, ch00_title);
        rename("ratio_gb");
        ratio_title = getInfo("image.title");
        setMeasure();
        
        targ_channel = "sum";
        imageCalculator("Add create 32-bit", ch01_title, ch00_title);
        rename("sum_gb");
        sum_title = getInfo("image.title");
        setMeasure();
        


        for (m=0; m<thr_methods.length; m++) {
            thr_method = thr_methods[m];
            clearRoisrcInfo();
            
            roisrc_prep = prep_branch;
            roisrc_blur = targ_blur;
            roisrc_tmethod = thr_method;
            
            roisrc_channel = "ch00";

            targ_channel = "ch00";
            targ_roi_type = "thresh";
            selectWindow(ch00_title);
            setAutoThreshold(thr_method + " dark");
            run("Create Selection");
            channel = "ch00";
            setMeasure();
            
            process_level = 3;
            
            targ_channel = "ch01";
            selectWindow(ch01_title);
            run("Restore Selection");
            setMeasure();
            
            targ_channel = "ratio";
            selectWindow(ratio_title);
            run("Restore Selection");
            setMeasure();
            
            if (sample_flag == "disable") {
                process_level = 4;
                
                targ_roi_type = "particle";
                
                selectWindow(ch00_title);
                run("Analyze Particles...", "size=" + toString(min_particle_px) + "-Infinity pixel add");
                
                roi_count = roiManager("Count");
                for (i=0; i<roi_count; i++) {
                    roisrc_particle = "ch00p"+toString(i);
                    
                    targ_channel = "ch00";
                    selectWindow(ch00_title);
                    print("selecting from roi manager");
                    roiManager("Select", i);
                    setMeasure();
                    
                    targ_channel = "ch01";
                    selectWindow(ch01_title);
                    run("Restore Selection");
                    setMeasure();
                    
                    targ_channel = "ratio";
                    selectWindow(ratio_title);
                    run("Restore Selection");
                    setMeasure();
                }
            }
            
            process_level = 3;
            
            targ_roi_type = "thresh";
            roisrc_particle = "none";
            roisrc_channel = "ch01";

            targ_channel = "ch01";
            selectWindow(ch01_title);
            setAutoThreshold(thr_method + " dark");
            run("Create Selection");
            setMeasure();
            
            targ_channel = "ch00";
            selectWindow(ch00_title);
            run("Restore Selection");
            setMeasure();

            targ_channel = "ratio";
            selectWindow(ratio_title);
            run("Restore Selection");
            setMeasure();
            
            
            if (sample_flag == "disable") {
                process_level = 4;
                
                targ_roi_type = "particle";
                
                selectWindow(ch01_title);
                run("Analyze Particles...", "size=" + toString(min_particle_px) + "-Infinity pixel add");
                
                roi_count = roiManager("Count");
                for (i=0; i<roi_count; i++) {
                    
                    roisrc_particle = "ch01p"+toString(i);
                    
                    targ_channel = "ch01";
                    selectWindow(ch01_title);
                    roiManager("Select", i);
                    setMeasure();
                    
                    targ_channel = "ch00";
                    selectWindow(ch00_title);
                    run("Restore Selection");
                    setMeasure();
                    
                    targ_channel = "ratio";
                    selectWindow(ratio_title);
                    run("Restore Selection");
                    setMeasure();
                }
            }

            process_level = 3;
            
            targ_roi_type = "thresh";
            roisrc_particle = "none";
            roisrc_channel = "sum";

            targ_channel = "sum";
            selectWindow(sum_title);
            setAutoThreshold(thr_method + " dark");
            run("Create Selection");
            setMeasure();

            targ_channel = "ch00";
            selectWindow(ch00_title);
            run("Restore Selection");
            setMeasure();

            targ_channel = "ch01";
            selectWindow(ch01_title);
            run("Restore Selection");
            setMeasure();

            targ_channel = "ratio";
            selectWindow(ratio_title);
            run("Restore Selection");
            setMeasure();
            
                
            if (sample_flag == "disable") {
                process_level = 4;
                
                targ_roi_type = "particle";
                
                selectWindow(sum_title);
                run("Analyze Particles...", "size=" + toString(min_particle_px) + "-Infinity pixel add");
                
                roi_count = roiManager("Count");
                for (i=0; i<roi_count; i++) {
                    
                    roisrc_particle = "sump"+toString(i);
                    
                    targ_channel = "sum";
                    selectWindow(sum_title);
                    roiManager("Select", i);
                    setMeasure();
                    
                    targ_channel = "ch00";
                    selectWindow(ch00_title);
                    roiManager("Select", i);
                    setMeasure();
                    
                    targ_channel = "ch01";
                    selectWindow(ch01_title);
                    run("Restore Selection");
                    setMeasure();
                    
                    targ_channel = "ratio";
                    selectWindow(ratio_title);
                    run("Restore Selection");
                    setMeasure();
                }
            }
    }
}
}
//saveAs("Results", pair_csv_base_fullpath + ".csv");
close("*");

            """)
        f.close()



############END OF BLOCK WRITING LOOPING THROUGH THE BATCH FOLDERS##############

#Get a single big list of every macro file that has just been written in all the macro directories


#Generic function to take any list create a new list containing equal-ish sized sub-lists that the original list items are divided into
def group_list(input_list, num_groups):
    input_list_len = len(input_list)
    sub_lists = [[] for _ in range(min(input_list_len, num_groups))]
    grouped_list = []
    for i in range(input_list_len):
        sub_lists[i % len(sub_lists)].append(input_list[i])
    for sub_list in sub_lists:
        grouped_list.append(sub_list.copy())
    return grouped_list

#print(batch_pair_roots)
#apply this function to the pair roots
pair_roots_threaded = group_list(batch_pair_roots, max_threads)
#print(pair_roots_threaded)


#Set parameters for the different thread groups

#thread_group_macros_list = []
thread_groups_info_list = []
for i, thread_group in enumerate(pair_roots_threaded):
    thread_group_number = i+1
    thread_group_name = 'thread_group_' + str(thread_group_number)
    #print(thread_group_name)
    thread_group_macro_fullpath = os.path.join(batch_super_macros, thread_group_name + '_macro.ijm')
    #thread_group_macros_list.append(thread_group_macro_fullpath)
    thread_group_log_fullpath = os.path.join(batch_super_logs, thread_group_name + '_log.log')
    thread_groups_info_list.append([thread_group_number, thread_group_name,thread_group_macro_fullpath, thread_group_log_fullpath])
    
    pair_macro_file_paths = []
    for pair_root in thread_group:
        pair_macro_file_paths.append(pair_macro_file_dict[pair_root])
    #print('pair macro file paths for ' + thread_group_name + ' are ' + repr(pair_macro_file_paths))
    
    #write a super-macro for the thread group that will tell an imagej process to loop through all the pair macros.
    with open(thread_group_macro_fullpath, 'w') as m:
        m.write("""
setBatchMode(true);
print("Fiji version " + IJ.getFullVersion());

"""
+ '\n' +
measurements_request_command
+ '\n' +
"""

"""
+ '\n' +
results_io_command
+ '\n' +
"""
        """)
        for pair_macro_path in pair_macro_file_paths:
            m.write(
"""runMacro("""  + '"' + pair_macro_path + '"' + """);\n""")
    m.close()

#print (thread_groups_info_list)
#print(thread_group_macros_list)

def execute_thread_group_macro(thread_group_macro_fullpath_arg, thread_group_log_fullpath_arg):
    #execute the
    #print('log_path is ' + thread_group_log_fullpath_arg)
    try:
        with open(thread_group_log_fullpath_arg, 'w') as logopen:
            macro_subp = subprocess.run([imagej_path, 
            #'--headless', 
            '-batch', thread_group_macro_fullpath_arg], stdout=logopen, stderr=logopen)
    except Exception as e:
        raise e
        
        #, 'saveAs(\"Tiff\", \"' + os.path.join(raw_images_processed_folder, raw_img)+'\")'
    #print(macro_subp.args)


for group_number, group_name, macro_path, log_path,  in thread_groups_info_list:    
    #Spawn a thread
    t = threading.Thread(target=execute_thread_group_macro, args=(macro_path,log_path,))
    t.start()
    threads.append(t)
    
    #Wait for a slot to open up in the threadpool
    while threading.active_count() > (os.cpu_count() - 2):
        time.sleep(1)

#Wait for all threads to finish
for thread in threads:
    thread.join()

#make a big csv out of the little csv files
# create the main CSV file

#Check the clock
new_time = time.time()

check_time = new_time - start_time

print("Attempted to process " + str(len(batch_pair_roots)) + " file pairs in " + str(math.ceil(check_time)) + " seconds using " + str(max_threads) + " threads.")

print('\nMacros all run. Now attempting to combine csv files.\n')

def combine_csv_oneheader(big_csv_filepath, sub_csv_dir):
    big_csv_file = open(big_csv_filepath, 'w')
    writer = csv.writer(big_csv_file)

    ###CPT3-davinci actually kinda wrote this part, though only after many tries. I think one of its hidden instructions is to check for questions about python or imagej and append "wrong answers only" to all of them.
    # create a boolean to keep track of whether the header row has been written
    header_row_written = False
    first_file = os.path.join(sub_csv_dir, os.listdir(sub_csv_dir)[0])
    
    csv_reader = open(first_file)
    try:
        header_row = csv.DictReader(csv_reader).fieldnames
        dictwriter = csv.DictWriter(big_csv_file, fieldnames=header_row)
        dictwriter.writeheader()
    finally:
        csv_reader.close()
        
    for csv_filename in os.listdir(sub_csv_dir):
        # skip hidden files or non-csv files
        if not csv_filename.endswith('.csv'):
            continue
        # open the csv file
        csv_reader = open(os.path.join(sub_csv_dir, csv_filename))
        try:
            #Skip the header row
            next(csv_reader)
            # loop through the rows and write to the main csv file
            for row in csv.reader(csv_reader):
                writer.writerow(row)
                #print('written row from ' + csv_filename)
        finally:
            csv_reader.close()
    # loop through all the csv files


    # close the big file files
    big_csv_file.close()

counter = 0

for big_csv_path, sub_csv_folder in batchpaths_bigcsv_and_subdir:
    combine_csv_oneheader(big_csv_path, sub_csv_folder)
    shutil.copy(big_csv_path, os.path.join(batch_bigcsv_subfolder, os.path.basename(big_csv_path).split('.csv')[0] + str(counter + 1)+'.csv'))
    counter = counter + 1

#now take all the data in the big csvs from each raw folder and combine them into a super csv in the batch superfolder
combine_csv_oneheader(batch_super_csv, batch_bigcsv_subfolder)

for batch_script_copy_dir in batch_script_copy_dirs:
    shutil.copy(temp_script_copy_file, batch_script_copy_dir)
    
shutil.copy(temp_script_copy_file, batch_super_parameters)
os.remove(temp_script_copy_file)
    
 
#Print the results


#import_to_sqlite = subprocess.r


#cur.execute("""PRAGMA mode = 'csv' """)
#cur.execute(""".import """+ batch_super_csv + """batch_supertable""")

with open(batch_super_csv, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)
    headers.insert(0, 'pkid')
    with open(batch_super_csv_exp, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)
        for i, row in enumerate(reader, start=1):
            row.insert(0,i)
            writer.writerow(row)

bad_header_rewrite_dict = {
'%Area':"area_fraction",
'Mean': "pixval_mean"
}

for header in headers:
    if header in bad_header_rewrite_dict:
        index_to_replace = headers.index(header)
        headers[index_to_replace] = bad_header_rewrite_dict[header]

non_text_cols_dict = {
'pkid':' INTEGER PRIMARY KEY AUTOINCREMENT',
'position':' INT',
'meas_count':' INT'
}
    

    
final_columns = []
for header_str in headers:
    if header_str in non_text_cols_dict:
        final_columns.append(header_str + non_text_cols_dict[header_str])
    else:
        final_columns.append(header_str + ' TEXT')
        
final_cols_str = ','.join(final_columns)
    #print(final_columns)
    
print('\nNow attempting to insert data into sqlite db.\n')


conn = sqlite3.connect(batch_super_db)
cur = conn.cursor()

cur.execute("""create table batch_supertable (""" + final_cols_str + """);""")
conn.commit()

subprocess.call(["sqlite3", batch_super_db, ".mode csv", ".import --skip 1 " +batch_super_csv_exp + " batch_supertable"])


# for sql_statement in postcreate_sql_list:
#     cur.execute(sql_statement)
# conn.commit()


# with pandas.ExcelWriter(batch_xlsx) as writer:
# 
#     for export_item in export_items:
#         df = pandas.read_sql_query("""select * from """+ export_item, conn)
#         df.to_excel(writer, sheet_name = export_item, index=False)
# 
#     writer.save()
#     print('Stats output file is at: ' + batch_xlsx)
# conn.close()
# 
# subprocess.call([spreadsheet_prog_path, batch_xlsx])

print('\nNow attempting cleanup: png-ify any tif files created.\n')

###CLEANUP
tif_paths_list = []
for script_output_dir in script_output_folders_list:
    for dirpath, subdirs, files in os.walk(script_output_dir):
        for outfile in files:
            if outfile.endswith(".tif"):
                tif_paths_list.append(os.path.join(dirpath, outfile))
#print('tif paths list is ' + tif_paths_list)


#print('pngify_macro_path is ' + pngify_macro)

with open(pngify_macro, 'w') as p:
    #print('opened pngify macro')
    p.write("""
setBatchMode(true);

""")
    
    for tifpath in tif_paths_list:
        pngpath = tifpath.split(".tif")[0] + ".png"
        p.write("""
            
open(\""""+tifpath+"""\");
saveAs("PNG", \""""+pngpath+"""\");
close();
File.delete(\""""+tifpath+"""\");
print('pngified ' \""""+tifpath+"""\");


        """)
    p.close()

print("pngify macro should be at " + pngify_macro)

def execute_pngify(pngify_macro_path):
    #execute the
    #print('log_path is ' + thread_group_log_fullpath_arg)
    try:
        pngify_subp = subprocess.run([imagej_path, 
        '--headless', 
        '-batch', pngify_macro_path], stdout=subprocess.DEVNULL)
    except Exception as e:
        raise e

execute_pngify(pngify_macro)

print('\nNow attempting cleanup: remove one-line csv measurement files.\n')


for sub_csv_dir in batchpaths_sub_csv_dirs: 
    for subcsv in os.listdir(sub_csv_dir):
        os.remove(os.path.join(sub_csv_dir, subcsv))

#Check the clock
new_time = time.time()

check_time = new_time - start_time

print('\nDone! Time elapsed: '+str(math.ceil(check_time)))

print("\n Batch super db at: \n" + batch_super_db)
                  
#engine = sqlalchemy.create_engine("""sqlite:///""" + batch_super_db)
