#! /usr/bin/env python

import pandas as pd
import sys
import argparse
import re
import os
from google.cloud import storage
from datetime import date

# usage
# conda activate terra_seq_prep # use the conda environment
# create_COVMIN_terra_data_table.py -i {table} -o {out_dir} --seq_run {covmin_run_name} --bucket_path {path to google bucket}


#### FUNCTIONS #####
def getOptions(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Parses command.")
    parser.add_argument("-i", '--input',  help="directory with gridion sample sheet; use full path")
    parser.add_argument("-o", "--output", help="output directory; if not specified, the output results will be written to the current directory")
#     parser.add_argument('--seq_run', help= 'covmin sequence run name')
    parser.add_argument('--entity_col_name' , help = 'abbreviation for entity:sample id column in terra data table', default = '')
    parser.add_argument('--bucket_path', help = 'path to google bucket where the fast_pass files are located')
    parser.add_argument('--terra_output_dir', help= 'optional, default is gs://covid_terra/{seq_run}/terra_outputs/', default = '')
    options = parser.parse_args(args)
    return options


def create_data_table(seq_run, sample_sheet_file, bucket_name, terra_output_dir):
    
    print('')
    print('  CREATING DATATABLE FOR NANOPORE RUN %s' % seq_run)
    # get just the numbers of seq_run
    seq_run_number = re.findall('COVMIN_([a-zA-Z0-9]+)', seq_run)[0]
    
    # read in sample sheet
    # determine header line
    sample_sheet = pd.read_excel(sample_sheet_file)
    for row in range(sample_sheet.shape[0]):
        if (re.search('Sample_ID', str(sample_sheet.loc[row, 'Unnamed: 0'])) or 
        re.search('Alias', str(sample_sheet.loc[row, 'Unnamed: 0']))):
            header_line = row + 1
        
    # now read in teh excel for real
                
    sample_sheet = pd.read_excel(sample_sheet_file, header = header_line, dtype = {'Alias' : object, 'Sample_ID' : object})
    if 'Sample_ID' in sample_sheet.columns:
        sample_sheet = sample_sheet.rename(columns = {'Sample_ID' : "Alias"})
        
    sample_sheet = sample_sheet.dropna(subset = ['Alias'])
    sample_sheet = sample_sheet.reset_index(drop = True)
    col_rename = {'Plate Location' : 'plate_sample_well', 'Other Name' : 'plate_name', 
                  'Barcode' : 'barcode', 'Primer_set' : 'primer_set', 
                  'Well_Location' : 'plate_sample_well', 'Other_Name' : 'plate_name' }
    sample_sheet = sample_sheet.rename(columns = col_rename)
    # add primer set column if DNE
    if 'primer_set' not in sample_sheet.columns:
        print('  ....primer set not provided; defaulting to "not specified"')
        sample_sheet['primer_set'] = 'not specified'
                                               
    for row in range(sample_sheet.shape[0]):
        sample_id = sample_sheet.Alias[row]
        if re.search('POS', str(sample_id)):
            sample_sheet.at[row, 'Alias'] = '%s_%s' % (sample_id, seq_run)
        elif re.search('NC', str(sample_id)):
            sample_sheet.at[row, 'Alias'] = '%s_%s' % (sample_id, seq_run)
        
        barcode = sample_sheet.barcode[row]
        
        bucket_path = 'gs://%s/%s/fastq_pass/%s' % (bucket_name, seq_run, barcode)
        
        sample_sheet.at[row, 'fastq_dir'] = bucket_path
 
    
    col_order = ['Alias', 'barcode', 'fastq_dir', 'plate_name', 'plate_sample_well', 'primer_set']  
    sample_sheet = sample_sheet[col_order]
    col1_header = 'entity:sampleG%s_id' % seq_run_number
    sample_sheet = sample_sheet.rename(columns = {'Alias' : col1_header})
    sample_sheet['out_dir'] = terra_output_dir
    sample_sheet['seq_run'] = seq_run
    
    
    return {"sample_sheet" : sample_sheet, 'entity_header' : col1_header}


def write_datatable(df, seq_run, out_dir):
    print('  ....writing datatable')
    outfile = os.path.join(out_dir, '%s_terra_data_table.tsv' % seq_run) 
    df.to_csv(outfile, index = False, sep = '\t')
    
    return outfile


def push_to_bucket(outfile, seq_run, sample_sheet, bucket_name):
    print('  ....pushing datatable to bucket')
    
    client_storage = storage.Client()
    bucket = client_storage.get_bucket(bucket_name)
    
    bucket_path = os.path.join(seq_run, '%s_terra_data_table.tsv' % seq_run)
    blob = bucket.blob(bucket_path)
    blob.upload_from_filename(outfile)
    print('  ....uploaded {} to {} bucket'.format(outfile, bucket_path))
    
    print('')
    print('  ....pushing sample sheet to bucket')
    sample_sheet_base_name = sample_sheet.split('/')[-1]
    bucket_path = os.path.join(seq_run, sample_sheet_base_name)
    blob = bucket.blob(bucket_path)
    blob.upload_from_filename(sample_sheet)
    print('  ....uploaded {} to {} bucket'.format(outfile, bucket_path))
    
def get_seq_runs_from_file_list(sample_sheet_directory):
    
    print('  ....genenerating list of sequencing runs from sample sheets in directory')
    # use the list of sample sheets to get teh list of sequencing runs
    
    files = os.listdir(sample_sheet_directory)
    print('  ....found %d sample sheets in directory' % len(files))
    
    seq_run_list = []
    for file in files:
        print('      ....%s' % file)
        seq_run = file.split('.')[0]
        if re.search('COVMIN_\d{4}', seq_run) or re.search('COVMIN_COVSEQ_\d{4}', seq_run):
            seq_run_list.append(seq_run)
        else:
            print('')
            print('  ....ERROR! seq_run name is not formatted correctly in sample sheet file name.')
            print('  ....ERROR! format should follow: "COVMIN_0000.xlsx" or "COVMIN_0000rr.xlsx"')
            print('')
            raise Exception('ERROR! cannot find seq_run name from sample sheet file name. See error message print out.')
            
    
    print('  ....individual terra datatables will be generated for the following %d seq_runs' % len(seq_run_list))
    print('  ....and then concatenated into a single terra table:')
    for seq_run in seq_run_list:
        print('      ....%s' % seq_run)

    return seq_run_list


if __name__ == '__main__':
    
    print('')
    
    # parse parameters
    options = getOptions()

    # check agrument inputs
    # input directory
    if options.input is None or not os.path.exists(options.input):
        raise Exception('use -i to specify the path to the directory with the list of sample sheets')
        
    if re.search('.xlsx', options.input):
        input_type = 'single sample sheet'
    else:
        input_type = 'directory with sample sheets'

    # output directory
    if options.output is None:
        outdirectory == os.getcwd()
    else:
        outdirectory = options.output
        
   # entity_col_name
    if options.entity_col_name == '':
        today_date = str(date.today())
        entity_col_name = ''.join(today_date.split('-'))
    else:
        entity_col_name = options.entity_col_name
                                 
    # get bucket details
    bucket_path = options.bucket_path # this is the path inside the bucket
    if re.search('gs://', bucket_path): 
        bucket_path = re.sub('gs://', '', bucket_path) 
    bucket_path_components = bucket_path.split('/')
    bucket_name = bucket_path_components[0] # this is the bucket name 
#     bucket_prefix = os.path.join('/'.join(bucket_path_components[1:]), options.seq_run)
   
              
    # print inputs:
    print('')
    print('  ....user inputs:')
    if input_type == 'single sample sheet':
        print('  ....user provided a single sample sheet')
        print('  ....sample sheet input: %s' % options.input)
    else:
        print('  ....user provided a directory with mulitple sample sheets')
        print('  ....sample directory path: %s' % options.input)
    print('  ....output directory path: %s' % options.output)
    print('  ....google bucket path: %s' % options.bucket_path)
    
    #     # get the terra output dir
    if options.terra_output_dir == '':
        print('  ....terra_output_dir: %s' % options.terra_output_dir)
        print('  ....terra_output_dir will default to gs://covid_terra/%s/terra_outputs/' % '{seq_run}')
    else:
        x = options.terra_output_dir
        print('  ....terra_output_dir: %s' % x)
        if re.search('gs://', x):
            x = x.replace('gs://', '')
        if re.search('/$', x):
            x = re.sub('/$', '', x)
        print('  ....terra_output_dir will become: gs://%s/%s/terra_outputs/' % (x, '{seq_run}'))
    print('')
    
    # run functions
    if input_type == 'directory with sample sheets':
        seq_run_list = get_seq_runs_from_file_list(sample_sheet_directory = options.input)

        terra_df_list = []
        for seq_run in seq_run_list:
            # get the terra output dir:
            terra_output_dir = options.terra_output_dir
            if terra_output_dir == '':
                terra_output_dir = 'gs://covid_terra/%s/terra_outputs/' % seq_run
            else:
                if re.search('gs://', terra_output_dir):
                    terra_output_dir = terra_output_dir.replace('gs://', '')
                if re.search('/$', terra_output_dir):
                    terra_output_dir = re.sub('/$', '', terra_output_dir)
                terra_output_dir = 'gs://%s/%s/terra_outputs/' % (terra_output_dir, seq_run)
            
            # get the sample sheet:
            sample_sheet_file_name = os.path.join(options.input, '%s.xlsx' % seq_run)

            func_dict = create_data_table(seq_run = seq_run, 
                                   sample_sheet_file = sample_sheet_file_name, 
                                   bucket_name = bucket_name,
                                         terra_output_dir = terra_output_dir)

            df = func_dict['sample_sheet']
            entity_header = func_dict['entity_header']

            outfile_path = write_datatable(df = df, 
                                           seq_run = seq_run, 
                                           out_dir = outdirectory)


            push_to_bucket(outfile = outfile_path, 
                           seq_run = seq_run, 
                           sample_sheet = sample_sheet_file_name,
                          bucket_name = bucket_name)

            df = df.rename(columns = {entity_header : 'entity'})
            terra_df_list.append(df)

        print('')
        print('')
        print('  ....concatentating terra data tables into a single terra data table')
        df = pd.concat(terra_df_list)
        df = df.reset_index(drop = True)
        new_entity_col = 'entity:sampleGRID%s_id' % entity_col_name
        df = df.rename(columns = {'entity' : new_entity_col})
        
        file_name_suffix = new_entity_col.replace('entity:sample', '')
        file_name_suffix = file_name_suffix.replace('_id', '')
        outfile = os.path.join(options.output, 'terra_data_table_concatenated_%s.tsv' % file_name_suffix)
        print('  ....writing concatenated terra datatable to ouput')
        print('  ....output called %s' % outfile)

        df.to_csv(outfile, sep = '\t', index = False)

        print('')
        print('  Done!')
        print('')
    
    else:
        file = options.input
        seq_run = file.split('.')[0]
        if re.search('COVMIN_\d{4}', seq_run) or re.search('COVMIN_COVSEQ_\d{4}', seq_run):
            seq_run = seq_run
        else:
            print('')
            print('  ....ERROR! seq_run name is not formatted correctly in sample sheet file name.')
            print('  ....ERROR! format should follow: "COVMIN_0000.xlsx" or "COVMIN_0000rr.xlsx"')
            print('')
            raise Exception('ERROR! cannot find seq_run name from sample sheet file name. See error message print out.')
        
        # define teh out_dir
        terra_output_dir = options.terra_output_dir
        if terra_output_dir == '':
            terra_output_dir = 'gs://covid_terra/%s/terra_outputs/' % seq_run
        else:
            if re.search('gs://', terra_output_dir):
                terra_output_dir = terra_output_dir.replace('gs://', '')
            if re.search('/$', terra_output_dir):
                terra_output_dir = re.sub('/$', '', terra_output_dir)
            terra_output_dir = 'gs://%s/%s/terra_outputs/' % (terra_output_dir, seq_run)
        
        func_dict = create_data_table(seq_run = seq_run, 
                               sample_sheet_file = options.input, 
                               bucket_name = bucket_name,
                                     terra_output_dir = terra_output_dir)
        
        df = func_dict['sample_sheet']
#         entity_header = func_dict['entity_header']

        outfile_path = write_datatable(df = df, 
                                       seq_run = seq_run, 
                                       out_dir = outdirectory)

        push_to_bucket(outfile = outfile_path, 
                       seq_run = seq_run, 
                       sample_sheet = options.input,
                      bucket_name = bucket_name)

        print('')
        print('  Done!')
        print('')

    
    
    
    
    
    
    
    
