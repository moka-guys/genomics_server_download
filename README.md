# genomics_server_download v1.4
Scripts for downloading files from DNA Nexus to the ArcherDx analysis platform via Genomics server.

Currently only contains scripts to download and transfer fastq files for ArcherDX FusionPlex Pan Solid Tumour NGS runs. Runs are demultiplexed and uploaded to DNA Nexus using the automated scripts. FASTQ files must then be downloaded and transfered to the Archer server in order to be used by the Archer Analysis software.

These scripts are designed to run hourly and monitor DNANexus for new ArcherDx projects. When a project is found the fastqs are downloaded and transferred to the watched folders on the server hosting the archerdx analysis platform.

The run is demultiplexed and uploaded to DNANexus using code in the automate_demultiplex (https://github.com/moka-guys/automate_demultiplex/).

This script monitors recent DNANexus projects, searching the project name for those containing "ADX". If these have not already been processed and the data upload is complete the dxda tool is used to download the fastqs. The dxda performs an integrity check.

rsync is then used to transfer fastqs into the watched folder. rsync also checks for data integrity.

The watched folders will set off analyses when a file named analysisid_completed.txt is present. Analyses can consist of single samples or groups of samples. Samples are groups for analysis by Pan number (allowing separate analyses for different dry labs); a folder is created with the name of the analysis followed by the Pan number, e.g. ADX99999_Pan9999, and relevant fastqs are transferred into this folder on the Archer platform. Once all fastqs are successfully transferred the completed file is created.

### Scripts for the ArcherDX file transfer:
* archer_script.py
* archer_config.py
* git_tag.py

## Requirements
* Python 3.5 (from v1.4 onwards)
* DNA Nexus dxda (v0.5.7)
    * CLI tool to manage the download of large quantities of files from DNAnexus (https://github.com/dnanexus/dxda)
    * Series of scripts used to create and filter a manifest file, download files based on the manifest, then check (inspect) the intergrity of the download
* rsync
* DNA Nexus sdk
* DNA Nexus API token
* Archer server password

## Docker
From v1.3 these scripts were modified so they could be run from within a docker container. This can be run using the command 
`sudo docker run --rm  -v /usr/local/src/mokaguys/logfiles:/mokaguys/logfiles -v /usr/local/src/mokaguys/dx_downloads:/mokaguys/dx_downloads -v /usr/local/src/mokaguys/.dnanexus_auth_token:/mokaguys/.dnanexus_auth_token -v /usr/local/src/mokaguys/.archerVM_pw:/mokaguys/.archerVM_pw  genomics_server_download:latest`
(replacing the tag `latest` as required).

The scripts should still run outside of docker (set docker=False in archer_config.py) but there is one issue when running with testing == True. As described below, when running in testing mode files within the logfiles/manifest_files are deleted. However, if these were created by docker these files are owned by root so the rm command will fail and the script will hang (we cannot run the python script as root). Therefore need to chown the user on these files.

### using ssh within the Docker image
It is necessary to add the archer server host to the known hosts, otherwise any ssh command will return an interactive prompt that the authenticity of the host cannot be established. As this must be done each time the docker image is run, a method is included in archer_archive_script.py, `set_up_ssh_known_hosts()`, using ssh-keygen and ssh-keyscan, that is run each time the script runs 

## Running the script
The Docker image is run hourly as a CRON job (the python script doesn't work in cron due to an issue with the pythonpath run by CRON). 

### Testing mode
In the config file there is a testing variabke.
When set to `True` a named project is used, and any files which stop the run from being processed are ignored.

## Logging
Script logfiles are written to mokaguys/logfiles/script_logfiles/YYYYMMDD_TTTTTT.txt

## Alerts
Alerts are sent to Slack when errors or warnings are sent to the system log.