# genomics_server_download v1.2
Scripts for downloading files from DNA Nexus to the ArcherDx analysis platform via Genomics server.

Currently only contains scripts to download and transfer fastq files for ArcherDX FusionPlex Pan Solid Tumour NGS runs. Runs are demultiplexed and uploaded to DNA Nexus using the automated scripts. FASTQ files must then be downloaded and transfered to the Archer server in order to be used by the Archer Analysis software.

These scripts are designed to run hourly and monitor DNANexus for new ArcherDx projects. When a project is found the fastqs are downloaded and transferred to the watched folders on the server hosting the archerdx analysis platform.

The run is demultiplexed and uploaded to DNANexus using code in the automate_demultiplex (https://github.com/moka-guys/automate_demultiplex/).

This script monitors recent DNANexus projects, searching the project name for those containing "ADX". If these have not already been processed and the data upload is complete the dxda tool is used to download the fastqs. The dxda performs an integrity check.

rsync is then used to transfer fastqs into the watched folder. rsync also checks for data integrity.

The watched folders will set off analyses when a file named analysisid_completed.txt is present. Analyses can consist of single samples or groups of samples. The lab has chosen for all samples on a run to be grouped into one analysis, so a folder is created with the name of the analysis and fastqs transferred into this folder. Once all fastqs are successfully transferred the completed file is created.

### Scripts for the ArcherDX file transfer:
* archer_script.py
* archer_config.py
* git_tag.py

## Requirements
* Python 2.7
* DNA Nexus dxda (v0.5.7)
    * CLI tool to manage the download of large quantities of files from DNAnexus (https://github.com/dnanexus/dxda)
    * Series of scripts used to create and filter a manifest file, download files based on the manifest, then check (inspect) the intergrity of the download
* rsync
* DNA Nexus sdk
* DNA Nexus API token
* Archer server password

## Running the script
The Archer file transfer script is run hourly as a CRON job. 

## Logging
Script logfiles are written to mokaguys/logfiles/script_logfiles/YYYYMMDD_TTTTTT.txt

## Alerts
Alerts are sent to Slack when errors or warnings are sent to the system log.