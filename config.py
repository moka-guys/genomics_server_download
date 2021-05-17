import os
# =====location of input/output files=====
# root of folder that contains the apps, automate_demultiplexing_logfiles and
# development_area scripts
# (2 levels up from this file)
document_root = "/".join(os.path.dirname(os.path.realpath(__file__)).split("/")[:-2])

logfile_folder = os.path.join(document_root,"logfiles")
processed_runs_folder = os.path.join(logfile_folder,"processed_runs")
manifest_folder = os.path.join(logfile_folder,"manifest_files")

# DNA Nexus authentication token
nexus_api_key_file = "{document_root}/.dnanexus_auth_token".format(document_root=document_root)
with open(nexus_api_key_file, "r") as nexus_api:
	Nexus_API_Key = nexus_api.readline().rstrip()

# archerdx VM login
path_to_archerdx_pw = "{document_root}/.archerVM_pw".format(document_root=document_root)

download_location = "~/"
fastq_folder_path = "Data/Intensities/BaseCalls"
path_to_watch_folder = "/watched/aledjones\@nhs.net/test1"
source_command = " source /usr/local/src/mokaguys/apps/dx-toolkit/environment"

success_statement = "all fastqs downloaded, transferred to server and deleted ok"

export_environment = "export DX_API_TOKEN=%s" % Nexus_API_Key

path_to_manifest_script = os.path.join(document_root,"apps/dxda/scripts","create_manifest.py")
path_to_filter_manifest_script = os.path.join(document_root,"apps/dxda/scripts","filter_manifest.py")
path_to_dx_download_client = os.path.join(document_root,"apps","dx-download-agent-linux")