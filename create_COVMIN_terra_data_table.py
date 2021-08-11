#! /usr/bin/env python

import pandas as pd
import sys
import argparse
import re
import os
from google.cloud import storage

# usage
# conda activate terra_seq_prep # use the conda environment
# covmin_terra_datatable.py -i {table} -o {out_dir} --seq_run {covmin_run_name}


#### FUNCTIONS #####
def getOptions(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Parses command.")
    parser.add_argument("-i", "--input", help="minion sample sheet")
    parser.add_argument("-o", "--output", help="output directory; if not specified, the output results will be written to the current directory")
    parser.add_argument('--seq_run', help= 'covmin sequence run name')
    parser.add_argument('--bucket_path', help = 'path to google bucket where the fast_pass files are located')
    options = parser.parse_args(args)
    return options


def create_data_table(seq_run, sample_sheet_file, bucket_name):

    print('')
    print('CREATING DATATABLE FOR MINION RUN %s' % seq_run)
    # get just the numbers of seq_run
    seq_run_number = re.findall('COVMIN_([a-zA-Z0-9]+)', seq_run)[0]

    # read in sample sheet
    sample_sheet = pd.read_excel(sample_sheet_file, header = 10, dtype = {'Alias' : object})
    sample_sheet = sample_sheet.dropna(subset = ['Alias'])


    # initaitate datatable
    df = pd.DataFrame()
    sample_list = []
    barcode_list = []
    bucket_path_list = []

    for row in range(sample_sheet.shape[0]):
        sample_id = sample_sheet.Alias[row]
        barcode = sample_sheet.Barcode[row]

        bucket_path = 'gs://%s/%s/fastq_pass/%s' % (bucket_name, seq_run, barcode)

        sample_list.append(sample_id)
        barcode_list.append(barcode)
        bucket_path_list.append(bucket_path)

    col1_header = 'entity:sampleG%s_id' % seq_run_number
    df[col1_header] = sample_list
    df['barcode'] = barcode_list
    df['fastq_dir'] = bucket_path_list

    return df


def write_datatable(df, seq_run, out_dir):
    print('....writing datatable')
    outfile = os.path.join(out_dir, '%s_terra_data_table.tsv' % seq_run)
    df.to_csv(outfile, index = False, sep = '\t')

    return outfile


def push_to_bucket(outfile, seq_run, sample_sheet, bucket_name):
    print('....pushing datatable to bucket')

    client_storage = storage.Client()
    bucket = client_storage.get_bucket(bucket_name)

    bucket_path = os.path.join(seq_run, '%s_terra_data_table.tsv' % seq_run)
    blob = bucket.blob(bucket_path)
    blob.upload_from_filename(outfile)
    print('....uploaded {} to {} bucket'.format(outfile, bucket_path))

    print('')
    print('....pushing sample sheet to bucket')
    sample_sheet_base_name = sample_sheet.split('/')[-1]
    bucket_path = os.path.join(seq_run, sample_sheet_base_name)
    blob = bucket.blob(bucket_path)
    blob.upload_from_filename(sample_sheet)
    print('....uploaded {} to {} bucket'.format(outfile, bucket_path))


if __name__ == '__main__':

    print('')

    # parse parameters
    options = getOptions()

    # check agrument inputs
    if options.input is None or not os.path.exists(options.input):
        raise Exception('use -i to specify the path to the sample sheet')

    if options.output is None:
        outdirectory == os.getcwd()
    else:
        outdirectory = options.output

    if options.seq_run is None :
        raise Exception('use --seq_run to specify COVMIN_0000 run name')
    elif not re.search('COVMIN', options.seq_run):
        raise Exception('run name must follow format "COVMIN_0000"')

   # get bucket details
    bucket_path = options.bucket_path # this is the path inside the bucket
    if re.search('gs://', bucket_path):
        bucket_path = re.sub('gs://', '', bucket_path)
    bucket_path_components = bucket_path.split('/')
    bucket_name = bucket_path_components[0] # this is the bucket name
#     bucket_prefix = os.path.join('/'.join(bucket_path_components[1:]), options.seq_run)

    # run functions
    df = create_data_table(seq_run = options.seq_run,
                           sample_sheet_file = options.input,
                           bucket_name = bucket_name)

    outfile_path = write_datatable(df = df,
                                   seq_run = options.seq_run,
                                   out_dir = outdirectory)

    push_to_bucket(outfile = outfile_path,
                   seq_run = options.seq_run,
                   sample_sheet = options.input,
                  bucket_name = bucket_name)

    print('')
    print('Done!')
    print('')
