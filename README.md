# genomics_server_upload
Scripts for downloading files from DNA Nexus to the Genomics Server (LOVD server).

Currently only contains script to download fastq files for ArcherDX FusionPlex Pan Solid Tumour NGS runs. Runs are demultiplexed and uploaded to DNA Nexus using the automated scripts. FASTQ files must then be downloaded and transfered to the Archer server in order to be used by the Archer Analysis software.
### Scripts for the ArcherDX file transfer:
* script.py
* config.py
* git_tag.py

## Requirements
* Python 2.7
* DNA Nexus dxda 
    * CLI tool to manage the download of large quantities of files from DNAnexus (https://github.com/dnanexus/dxda)
    * Used to create and filter a manifest file, download files based on the manifest, then check the intergrity of the download
* Archer server password 
* DNA Nexus API token

## Running the script
The Archer file transfer script is run hourly as a CRON job. 

## Logging
Script logfiles are written to mokaguys/logfiles/script_logfiles/YYYYMMDD_TTTTTT.txt

## Alerts
Alerts are sent to Slack when errors or warnings are sent to the system log.