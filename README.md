# seq_assembly_prep_for_terra

## Purpose:
This python script accomplishes the following tasks:
- generates a terra data table compatiable for use with the cdphe nanopore-preprocessing-assemlby wdl workflow on terra
- pushes the terra data table and original sample sheet to a google bucket

## Requirements:
- The python modules neccessary to run this script are contained in a conda environment; therefore so you must have Anaconda or miniconda installed.
- conda environment -- the same conda environment used for the orgnaize_illumina_fastq.py should be used. The environmet.yml file and setup instructions can be found: https://github.com/CDPHE/organize_illumina_fastq

## Running the script:
### Part 1: Preparing your data
1. Log into the gridion vm and copy the fast_pass files to the google bucket:
``gsutil -m cp -r /fast_pass/* gs://bucket_name/seq_run_name/fast_pass/ ``
2. Download the sample sheet which should be an xlsx formatted file with the headers begining at line 11.

### Part 2: Runing the script
1. Activate the conda enviornment:
``conda activate terra_seq_prep``
2. Run the script. The following inputs need to be specified:
  - ``-i``: the path to the xlsx sample sheet
  - ``-o`` : the directory where you want the terra data table to be saved on your machine. if no path is specified then the table will be saved to the current directory
  - ``--seq_run`` : the sequencing run, which should be formatted as "COVMIN_0000" or "COVMIN_0000rr"
  - ``--bucket_path``: the path to the bucket where the fast_pass directory is located. Note: the script is NOT super flexible here. The script will only read the actual bucket name (the first part of the bucket path) and will push the outputs to the following bucket path: ``gs://bucket_name/seq_run/`` . For example if I specifiy ``--bucket_path`` as ``gs://molly_sandbox/practice/`` the output will be pushed to ``gs://molly_sandbox/COVMIN_0000`` and not ``gs://molly_sandbox/practice/COVMIN_0000``.
putting it altogether:
``create_COVMIN_terra_data_table.py -i <sample_sheet.xlsx> -o . --seq_run COVMIN_0000 --bucket_path gs://covid_terra/``


## Outputs
1. ``COVMIN_0000_terra_data_table.tsv`` in the specified directory and pushed to the google bucket path specified

2. ``COVMIN_0000.xlsx`` pushed to the google bucket path specified

-