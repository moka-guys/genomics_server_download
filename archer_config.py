import os

testing = False
testing_project = "003_221215_NB552085_0225_AH2J2JAFX5_ADX99999" # project-GKQkYg00bKYY0gZjFPX6BYx3
docker = True
# =====location of input/output files=====
# root of folder that contains the apps, automate_demultiplexing_logfiles and
# development_area scripts
# (2 levels up from this file)
document_root = "/".join(os.path.dirname(os.path.realpath(__file__)).split("/")[:-2])
resources_root=os.path.dirname(os.path.realpath(__file__))
logfile_folder = os.path.join(document_root,"logfiles")
script_logfile_folder = os.path.join(logfile_folder,"script_logfiles")
processed_runs_folder = os.path.join(logfile_folder,"processed_runs")
manifest_folder = os.path.join(logfile_folder,"manifest_files")

# DNA Nexus authentication token
nexus_api_key_file = os.path.join(document_root,".dnanexus_auth_token")
with open(nexus_api_key_file, "r") as nexus_api:
	Nexus_API_Key = nexus_api.readline().rstrip()

# archerdx VM login
path_to_archerdx_pw = "{document_root}/.archerVM_pw".format(document_root=document_root)

download_location = os.path.join(document_root,"dx_downloads")
fastq_folder_path = "Data/Intensities/BaseCalls"
path_to_watch_folder = "/watched/aledjones\@nhs.net/FusionPlexPanSolidTumorv1_0" #folder made by RLH 20210622
success_statement = "all fastqs downloaded, transferred to server and deleted ok"
export_environment = "export DX_API_TOKEN=%s" % Nexus_API_Key

# when testing the script it may be easier to do so without running the docker image. Different paths are required for some inputs in this case
if docker:
    source_command = " source %s" % (os.path.join(resources_root,"resources","dx-toolkit","environment"))
else:
    source_command = " source %s" % (os.path.join(document_root,"apps","dx-toolkit","environment"))

path_to_manifest_script = os.path.join(resources_root,"resources","dxda_0.5.12","dxda","scripts","create_manifest.py")
path_to_filter_manifest_script = os.path.join(resources_root,"resources","dxda_0.5.12","dxda","scripts","filter_manifest.py")
path_to_dx_download_client = os.path.join(resources_root,"resources","dxda_0.5.12","dx-download-agent-linux")
