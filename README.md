# terra_data_table_for_nanopore

## Purpose:
This python script accomplishes the following tasks:
- generates a terra data table compatiable for use with the cdphe nanopore-preprocessing-assemlby wdl workflow on terra
- pushes the terra data table and original sample sheet to a google bucket
- new! concatenates multiple datatables into a single datatable, if instead of supplying as input the path to the sample sheet, you supply the path to a directory wtih multiple sample sheets

## Requirements:
- The python modules neccessary to run this script are contained in a conda environment; therefore so you must have Anaconda or miniconda installed.
- conda environment -- the same conda environment used for the orgnaize_illumina_fastq.py should be used. The environmet.yml file and setup instructions can be found: https://github.com/CDPHE/organize_illumina_fastq

## Preparing your environment:
This only needs to be performed the first time you run the script.
1. Clone the repository to your machine and change directories to the repository on your machine.

``git clone https://github.com/CDPHE/terra_data_table_for_nanopore``

`` cd seq_assembly_prep_for_terra``

2. Create the conda environment using the ```environment.yml``` file. The environment's name should be ```terra_seq_prep```

``conda env create -f environment.yml``

3. If the environment already exists then to update the environment
``conda env update -f environment.yml``

3. Check that the environment exists. The name of the enivornment should be `terra_seq_prep`

``conda env list``

4. Activate the conda environment

``conda activate terra_seq_prep``

## Preparing to run the script:
1. Clone the repository to your machine and change directories ot the repository on your machine:
``git clone https://github.com/CDPHE/terra_data_table_for_nanopore``

2. Make the script executable:
``chmod 755 create_COVMIN_terra_data_table.py``

3. Option 1: add the script to a scripts directory that is listed in your machine's ``$PATH`` variable. In this case you will only need to specify the name of the script each time you run the script.

4. OPtion 2: specify the path to the script each time you run the script.

## Running the script:
### Part 1: Preparing your data
1. Log into the gridion vm and copy the fast_pass files to the google bucket:
``gsutil -m cp -r /fast_pass/* gs://bucket_name/seq_run_name/fast_pass/ ``

2. Download the sample sheet and upload to your machine. The sample sheet should be an xlsx formatted file with the headers begining at line 11.
    - NOTE! the script pulls the seq_run name from the sample sheet file name, so make sure that the seq_run name in the file name is formatted correctly! Example of correct foramt: COVMIN_0000.xlsx or COVMIN_0000rr.xlsx

### Part 2: Runing the script
1. Activate the conda enviornment:
``conda activate terra_seq_prep``

2. Run the script. The following inputs need to be specified:
  - ``-i``: the path to the xlsx sample sheet or the path to the directory with multiple sample sheets
  - ``-o`` : the directory where you want the terra data table to be saved on your machine. if no path is specified then the table will be saved to the current directory
  - ``--bucket_path``: the path to the bucket where the fast_pass directory is located. Note: the script is NOT super flexible here. The script will only read the actual bucket name (the first part of the bucket path) and will push the outputs to the following bucket path: ``gs://bucket_name/seq_run/`` . For example if I specifiy ``--bucket_path`` as ``gs://molly_sandbox/practice/`` the output will be pushed to ``gs://molly_sandbox/COVMIN_0000`` and not ``gs://molly_sandbox/practice/COVMIN_0000``.
  - ``--entity_col_name``: (optional) if supplied this will be the name of the entity:sampleGRID{entity_col_name}_id. if not supplied the default is to use the current date (enityt:sampleGRID20211021_id)

3. Putting it altogether:
  - if supplying single sample sheet
``create_COVMIN_terra_data_table.py -i <sample_sheet.xlsx> -o . --bucket_path gs://covid_terra/ ``
  - if supplying a directory with mulitple sample sheets:
  ``create_COVMIN_terra_data_table.py -i <path_to_directory_with_sample_sheets> -o . --bucket_path gs://covid_terra/ --entity_col_name <some_name>``

## Outputs
1. ``COVMIN_0000_terra_data_table.tsv`` in the specified output directory and pushed to the google bucket path specified
    - columns included: entity:sample_id, barcode, fasq_dir, plate_name, plate_sample_well, primer_set, out_dir, seq_run
2. ``terra_data_table_concatenated_{entity_col_name}.tsv' `` in the specified output directory (if directory path to sample sheets is specified)
3. ``COVMIN_0000.xlsx`` pushed to the google bucket path specified

-
