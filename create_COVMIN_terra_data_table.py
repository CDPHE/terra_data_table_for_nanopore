#! /usr/bin/env python

import pandas as pd
import sys
import argparse
import re
import os
from google.cloud import storage
from datetime import date
import time
import subprocess

#### FUNCTIONS #####
def getOptions(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Parses command.")
    parser.add_argument("-i", '--input',  help="directory with gridion sample sheet; use full path")
    parser.add_argument("-o", "--output", help="output directory; if not specified, the output results will be written to the current directory", default = '')
#     parser.add_argument('--seq_run', help= 'covmin sequence run name')
    parser.add_argument('--entity_col_name' , help = 'abbreviation for entity:sample id column in terra data table', default = '')
    parser.add_argument('--bucket_path', help = 'path to google bucket where the fast_pass files are located')
    parser.add_argument('--terra_output_dir', help= 'optional, default is gs://covid_terra/{seq_run}/terra_outputs/', default = '')
    options = parser.parse_args(args)
    return options


def create_data_table(seq_run, sample_sheet_file, bucket_name, terra_output_dir_prefix, 
                      download_date, tech_platform, read_type):
    
    print('')
    print('  ..... creating datatable for oxford nanopore run %s' % seq_run)
    
    # get just the numbers of seq_run to use in the entity column
    seq_run_number = re.findall('COVMIN_([a-zA-Z0-9]+)', seq_run)[0]
    
    # read in sample sheet
    # determine header line
    sample_sheet = pd.read_excel(sample_sheet_file)
    for row in range(sample_sheet.shape[0]):
        if (re.search('Sample_ID', str(sample_sheet.loc[row, 'Unnamed: 0'])) or 
        re.search('Alias', str(sample_sheet.loc[row, 'Unnamed: 0']))):
            header_line = row + 1
        
    # now read in teh excel for real using the header line determined above      
    sample_sheet = pd.read_excel(sample_sheet_file, header = header_line, dtype = {'Alias' : object, 'Sample_ID' : object})
    if 'Sample_ID' in sample_sheet.columns:
        sample_sheet = sample_sheet.rename(columns = {'Sample_ID' : "Alias"})
        
    sample_sheet = sample_sheet.dropna(subset = ['Alias'])
    sample_sheet = sample_sheet.reset_index(drop = True)
    col_rename = {'Plate Location' : 'plate_sample_well', 'Other Name' : 'plate_name', 
                  'Barcode' : 'barcode', 'Primer_set' : 'primer_set', 
                  'Well_Location' : 'plate_sample_well', 'Other_Name' : 'plate_name' }
    sample_sheet = sample_sheet.rename(columns = col_rename)
    
    # create teh full terra output dir from the terra output dir prefix
    terra_output_dir = os.path.join(terra_output_dir_prefix, seq_run, 'terra_outputs')
    
    # add column for tech platform, download date, read_type, terra ouptu dir, and seq_run
    sample_sheet['tech_platform'] = tech_platform
    sample_sheet['download_date'] = download_date
    sample_sheet['read_type'] = read_type
    sample_sheet['out_dir'] = terra_output_dir
    sample_sheet['seq_run'] = seq_run
    
    # add primer set column if DNE
    if 'primer_set' not in sample_sheet.columns:
        print('  ..... primer_set not included in sample sheet; primer_set will be recorded as "not specified"')
        sample_sheet['primer_set'] = 'not specified'
        
    # loop through sample sheet:
    for row in range(sample_sheet.shape[0]):
        
        # append seq run name to POS and NC control samples
        sample_id = sample_sheet.Alias[row]
        if re.search('POS', str(sample_id)):
            sample_sheet.at[row, 'Alias'] = '%s_%s' % (sample_id, seq_run)
        elif re.search('NC', str(sample_id)):
            sample_sheet.at[row, 'Alias'] = '%s_%s' % (sample_id, seq_run)
        
        # create column for fastq_dir
        barcode = sample_sheet.barcode[row]
        fastq_dir_path = os.path.join(bucket_name, seq_run, 'fastq_pass', barcorde)
        sample_sheet.at[row, 'fastq_dir'] = fastq_dir_path
 
    
    col_order = ['Alias', 'barcode', 'seq_run', 'download_date','tech_platform', 'read_type', 
                 'primer_set', 'plate_name', 'plate_sample_well', 'out_dir', 'fastq_dir']
    sample_sheet = sample_sheet[col_order]
    col1_header = 'entity:sampleG%s_id' % seq_run_number
    sample_sheet = sample_sheet.rename(columns = {'Alias' : col1_header})

    return {"sample_sheet" : sample_sheet, 'entity_header' : col1_header}


def write_datatable(df, seq_run, out_dir, bucket_path):
    print('  ..... writing datatable')
    outfile = os.path.join(out_dir, '%s_terra_data_table.tsv' % seq_run) 
    df.to_csv(outfile, index = False, sep = '\t')
    
    print('  ..... pushing datable to bucket')
    full_bucket_path = os.path.join(bucket_path, seq_run)
    shell_command = 'gsutil -m cp -r %s %s/' % (outfile, full_bucket_path)
    subprocess.run(args = shell_command, shell = True, check = True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)



def get_seq_runs_from_file_list(sample_sheet_directory):
    
    print('  *** genenerating list of sequencing runs from sample sheets in directory')
    # use the list of sample sheets to get teh list of sequencing runs
    
    files = os.listdir(sample_sheet_directory)
    print('  ..... found %d sample sheets in directory' % len(files))
    
    # print files in directory
    for file in files:
        if re.search('.xlsx', file):
            print('       %s' % file)
    time.sleep(3)
    
    seq_run_list = []
    for file in files:
        if re.search('.xlsx', file):
            seq_run = get_seq_name_from_file(xlsx_file = file)
            seq_run_list.append(seq_run)
            
    # print the names of the seq runs to be used...
    print('  ..... the following will the seq_run names used, if incorrect correct name in sample sheet file name')
    for seq_run in seq_run_list:
        print('        %s' % seq_run)
    time.sleep(3)

#     print('  ..... first, individual terra datatables will be generated for each seq run' )
#     print('  ..... next, will concatenate all datatables into a single datatable')

    return seq_run_list


def get_seq_name_from_file(xlsx_file):
    seq_run = xlsx_file.split('.')[0]
    if not re.search('\d{4}', seq_run):
        print('')
        print('  ..... WARNING! the seq_run name may not be formatted correctly for %s' % xlsx_file)
        print('        ..... format should follow: "COVMIN_0000.xlsx" ')
        print('')
        time.sleep(3)
  
    return seq_run

def concat_dfs(terra_df_list, entity_col_name):
    print('')
    print('  *** concatentating terra data tables into a single terra data table')

    df = pd.concat(terra_df_list)
    df = df.reset_index(drop = True)
    df = df.rename(columns = {'entity' : entity_col_name})

    file_name_suffix = entity_col_name.replace('entity:sample', '')
    file_name_suffix = file_name_suffix.replace('_id', '')
    outfile = os.path.join(options.output, 'terra_data_table_concatenated_%s.tsv' % file_name_suffix)
    print('  ..... writing concatenated terra datatable to ouput')
    print('  ..... concatenated terra datatable called %s' % outfile)

    df.to_csv(outfile, sep = '\t', index = False)

if __name__ == '__main__':
    
        
    print('')
    print('  *************************************************************************')
    print('  *** starting CREATE COVMIN TERRA DATATABLE ***')
    print('      .... last updated (major) 2021-10-28')
    print('      .... last updated (minor) 2021-11-03')
    print('      .... lastest update (major) includes additonal column headers in datatable')
    print('      .... lastest update (minor) includes warning if seq_run not formatted correctly')
    print('')
    print('')
    
    time.sleep(2) # delay 2 seconds so can read output to screen
    
    
    options = getOptions()
    
    # create variables from user input
    tech_platform = 'Oxford Nanopore GridION'
    read_type = 'single'
    
    input_path = options.input
    if re.search('.lsx', options.input):
        input_type = 'single sample sheet'
    else:
        input_type = 'directory with sample sheets'
   
    outdirectory = options.output
    if outdirectory == '':
        outdirectory = os.getcwd()
        
    bucket_path = options.bucket_path
    download_date = str(date.today())
    
    terra_output_dir_prefix = options.terra_output_dir
    if terra_output_dir_prefix == '':
        terra_output_dir_prefix = 'gs://covid_terra/'
        
    entity_col_name = options.entity_col_name 
    if entity_col_name == '':
        concat_date = ''.join(download_date.split('-'))
        entity_col_name = 'entity:sampleGRID%s_id' % concat_date
    else:
        entity_col_name = 'entity:sample%s_id' % entity_col_name
                             


    print('  User Inputs and Parameters:')
    print('  input: %s' % input_path)
    print('  input_type: %s' % input_type)
    print('  download_date: %s' % download_date)
    print('  sequencing_tech_platform: %s' % tech_platform)
    print('  bucket_path: %s' % bucket_path)
    print('  read_type: %s' % read_type)
    print('  terra_output_bucket_directory_prefix: %s' % terra_output_dir_prefix)
    print('  entity_col_name: %s' % entity_col_name)
    print('  output_directory: %s' % outdirectory)
    print('')
    print('')
    time.sleep(6)
    
    # run through functions
    
    # get list of seq_runs if sample sheet directory
    if input_type == 'directory with sample sheets':
        seq_run_list = get_seq_runs_from_file_list(sample_sheet_directory= input_path)
        print('')
        print('')
        # create data table for each seq_run
        #### create empty df list to store df of each seq_run
        df_list = []
        print('  *** generating datatable for each indivdiual seq_run')
        for seq_run in seq_run_list:
            # get the path to the sample sheet:
            sample_sheet_file_path = os.path.join(input_path, '%s.xlsx' % seq_run)
            
            func_dict = create_data_table(seq_run = seq_run, 
                                          sample_sheet_file = sample_sheet_file_path, 
                                          bucket_name = bucket_path, 
                                          terra_output_dir_prefix = terra_output_dir_prefix, 
                                          download_date = download_date, 
                                          tech_platform = tech_platform, 
                                          read_type = read_type)
            
            df = func_dict['sample_sheet']
            entity_header = func_dict['entity_header']
            # change entity header
            df = df.rename(columns = {entity_header: 'entity'})
            
            df_list.append(df)
            
            write_datatable(df = df, 
                            seq_run = seq_run, 
                            out_dir = outdirectory, 
                            bucket_path = bucket_path)
            
        
        print('')
        print('')
        concat_dfs(terra_df_list = df_list, 
                   entity_col_name = entity_col_name)
    
    elif input_type == 'single sample sheet':
        seq_run = get_seq_name_from_file(xlsx_file = input_path)
        
        func_dict = create_data_table(seq_run = seq_run, 
                          sample_sheet_file = sample_sheet_file_path, 
                          bucket_name = bucket_path, 
                          terra_output_dir_prefix = terra_output_dir_prefix, 
                          download_date = download_date, 
                          tech_platform = tech_platform, 
                          read_type = read_type)
        
        df = func_dict['sample_sheet']
        
        write_datatable(df =df, 
                        seq_run = seq_run, 
                        out_dir = outdirectory, 
                        bucket_path = bucket_path)
        
    print('')
    print('  DONE!')
    print('')

    
    