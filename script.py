import os, datetime, subprocess
# import config file
import config as config
# import function which reads the git tag
import git_tag as git_tag

class Archerdx():
	def __init__(self):
		self.now = str('{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now()))
		# Set script log file path and name for this hour's cron job (script log file).
		self.script_logfile_path = config.script_logfile_folder
		self.logfile_name = self.script_logfile_path + "/" + self.now + ".txt"
		# Open the script logfile for logging throughout script.
		self.script_logfile = open(self.logfile_name, 'a')

	def list_projects(self):
		"""
		Function which lists all projects visible to mokaguys user account (with at least contribute privileges). 
		This list is filtered to only include those created in the last 7 days and with the expected string (ADX) in the project name 
		This command is executed and a tuple is returned in format(projectid, projectname)
		"""
		# build command including the source cmd (activate the sdk)
		cmd = config.source_command+";dx find projects --created-after=-7d --name=*_ADX* --auth-token %s" % (config.Nexus_API_Key)
		self.logger("Archer download script %s started on genomics server. Checking for new ArcherDX projects. Command: %s" % (git_tag.git_tag(), cmd), "Archer list projects")
		out, err = self.execute_subprocess_command(cmd)
		# list of projects- loop through to yield the projectid and projectname as "project"
		for item in out.split("\n"):
			if len(item)>1:
				projectid,colon,projectname,access = item.split(" ")
				self.logger("Archer project identified. Project ID: %s, Project Name: %s" % (projectid, projectname), "Archer list projects")
				yield (projectid, projectname)
			else:
				self.logger("No new Archer projects.", "Archer list projects")

	def check_if_already_actioned(self, project):
		"""
		We don't want to process the same run over and over again 
		We can check for presence of a file which is only created if the run is processed successfully.
		see create_file_to_stop_subsequent_processing() for file creation
		"""
		expected_filename = "%s.txt" % (project[1])
		if expected_filename in os.listdir(config.processed_runs_folder):
			if self.check_if_completed_ok(expected_filename):
				self.logger("Archer project %s previously actioned and completed ok." % project[1], "Archer check previously actioned")
				return True
			else:
				self.logger("WARNING: Archer project %s previously actioned but NOT completed ok. Will attempt reprocessing." % project[1], "Archer check previously actioned")
				return False
		return False

	def check_if_completed_ok(self,logfile):
		"""
		Looks for success statement in the file created by create_file_to_stop_subsequent_processing()
		Returns true if statement is present
		"""
		with open(os.path.join(config.processed_runs_folder,logfile)) as logfile_to_check:
			for line in logfile_to_check.readlines():
				if config.success_statement in line:
					return True
		return False

	def check_all_files_closed(self,project):
		"""
		This function makes sure all fastq files have finished uploading to DNANexus before it tries to download them
		use the dx find data command, which returns the file status
		This function returns two values,a True/False flag denoting if all files are closed, and a list of all fastq filenames
		"""
		all_files_closed = True
		file_list = []
		# command to list all fastqs in project
		cmd = config.source_command+";dx find data --path %s --name '*.fastq.gz' --auth-token %s" % (project[0], config.Nexus_API_Key)
		out, err = self.execute_subprocess_command(cmd)
		# each fastq found is listed along with it's status eg open/closed.
		# it will be closed when the upload has finished.
		self.script_logfile.write("Fastq files in Archer project %s\n" % project[1])
		for line in out.split("\n"):
			# check if any files are not closed
			if len(line) >2 and not line.startswith("closed"):
				self.logger("Archer project %s not all files closed. Will not proceed with download" % project[1], "Archer check all files closed")
				all_files_closed = False
			# add filename to list and to logfile
			if len(line) > 2:
				filename = line.split("BaseCalls/")[1].split(" ")[0]
				# write fastq filename to script logfile
				self.script_logfile.write(filename + "\n")
				# add fastq filename to list of files to download
				file_list.append(filename)
		# if all files are closed, add this information to the log.
		if all_files_closed:
			self.logger("Archer project %s all files closed. Ready to proceed with download." % project[1], "Archer check all files closed")
		
		return all_files_closed, file_list

	def dx_login(self):
		"""
		The DNAnexus download agent (dxda) requires user to be logged in first
		for use with functions calling dxda scripts (create_manifest_file & create_filtered_manifest)
		login will time out after 30 mins
		"""
		cmd = config.source_command+";dx login --token %s --noprojects --timeout 30m" % (config.Nexus_API_Key)
		self.logger("login command for dxda: %s" % cmd, "Archer dx login")
		out, err = self.execute_subprocess_command(cmd)
		# if the login fails, add this to the log
		if err:
			self.logger("dx login failed.", "Archer dx login")

	def create_manifest_file(self, project):
		"""
		The DNAnexus download agent (dxda) requires a manifest file, which is a compressed JSON.
		This needs to first be generated then can be filtered before it is used to download.
		This function creates the unfiltered manifest file. It is called by create_filtered_manifest_file(). 
		The python script create_manifest.py within the dxda package is used create the manifest file - this is stated in config.
		Will return True if completes successfully.
		"""
		# dx login required for dxda scripts
		self.dx_login()

		# create cmd for dxda script to create manifest (create_manifest.py)
		manifest_filename = os.path.join(config.manifest_folder,"%s.json.bz2 " % (project[0]))
		cmd = "python %s %s --recursive --outfile %s" % (
			config.path_to_manifest_script, 
			project[0], 
			manifest_filename
			)
		
		out, err = self.execute_subprocess_command(cmd)
		if not err:
			self.logger("Unfiltered manifest file created %s" % manifest_filename, "Archer create manifest file")
			return True
		else:
			# Rapid7 alert set:
			self.logger("ERROR: Failed to create manifest file (unfiltered) %s" % manifest_filename, "Archer create manifest file")
			return False
	
	def create_filtered_manifest_file(self,project):
		"""
		The DNAnexus download agent (dxda) requires a manifest file, which is a compressed JSON.
		This needs to first be generated by calling create_manifest_file()) and then filtered before it is used to download.
		The python script filter_manifest.py within the dxda package is used filter the manifest file - this is stated in config.
		Will return True if completes successfully.
		"""
		# create manifest file
		self.create_manifest_file(project)

		# dx login required for dxda scripts
		self.dx_login()

		# create cmd for dxda script to filter manifest (create_manifest.py for fastqs)
		filtered_manifest_filename = os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0]))
		cmd = "python %s %s --output_file %s '(.*)_001.fastq.gz'" % (
			config.path_to_filter_manifest_script, 
			os.path.join(config.manifest_folder, "%s.json.bz2" % (project[0])),
			filtered_manifest_filename
			) 
		# execute filter manifest command
		out, err = self.execute_subprocess_command(cmd)
		if not err:
			self.logger("Filtered manifest file created %s. Ready to commence download." % filtered_manifest_filename, "Archer create filtered manifest file")
			return True
		else:
			#Rapid7 alert set:
			self.logger("ERROR: Failed to create manifest file (filtered) %s" % filtered_manifest_filename, "Archer create filtered manifest file")
			return False
			
	def download_using_manifest_file(self,project):
		"""
		Once the filtered manufest file is created we can use it to download the fastqs using the dxda
		note the folder struture in DNANexus is replicated after download.
		"""
		# source the dnanexus sdk and then download using filtered manifest and auth token
		self.logger("Preparing to download using filtered manifest file", "Archer download fastqs")
		cmd = "cd %s;%s;%s download %s --auth-token %s" % (
			config.download_location, 
			config.export_environment, 
			config.path_to_dx_download_client, 
			os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0])),
			config.Nexus_API_Key)
		self.logger("Download fastqs using dxda command: %s" % cmd, "Archer download fastqs")
		out, err = self.execute_subprocess_command(cmd)
		self.logger("Fastq files for project %s downloaded to %s" % (project[1], config.download_location), "Archer download fastqs")
		# pass stderr to error checking function which looks for expected error related strings at this stage and stdout to the success checking function
		if not self.errors_in_stderr(err, "download") and self.success_in_stdout(out,"Download completed successfully"):
			# check the integrity of the downloads
			if self.inspect_download(project):
				return True

	def inspect_download(self,project):
		"""
		The dxda package includes a function which checks the integrity of the downloaded files.
		if error returned from inspect download, only the first file that fails will be listed (and added to the log).
		"""
		cmd = "cd %s;%s;%s inspect %s --auth-token %s" % (
			config.download_location, 
			config.export_environment, 
			config.path_to_dx_download_client, 
			os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0])),
			config.Nexus_API_Key)
		self.logger("Inspect downloaded fastqs using dxda command: %s" % cmd, "Archer Inspect Download")
		out, err = self.execute_subprocess_command(cmd)
		# if inspect download doesn't retrun any errors
		if not self.errors_in_stderr(err, "inspect") and self.success_in_stdout(out,"Integrity check for regular files complete."):
			self.logger("Integrity check for %s Archer fastq dowload complete." % project[1], "Archer Inspect Download")
			return True
		else:
			# add inspect download errors to the logfile
			self.logger("ERROR: inspect download stderr: %s. Check this error and restart download. See log for list of fastq files expected." % err, "Archer Inspect Download")
			return False

	def organise_file_transfer(self, project, fastq_files_list):
		"""
		Fastqs have been downloaded and integrity checked.
		We now need to transfer them to the archerdx server.
		This function starts this process, calling other functions transfer_file_to_server() and create_completed_file().
		
		The archerdx server has watched folders, which will apply preset analysis settings to data within that folder.
		Samples will only be processed once a completed file is present.
		Samples can be processed individually or grouped into a single analysis. 
		When grouped, a subfolder within the analysis folder is created and a single completed file is needed named foldername.completed.
		The lab prefer samples to be grouped so this approach is taken, using the ADX runnumber (eg ADX001) to name the analysis
		"""
		self.logger("Preparing to transfer files for project %s to Archer server" % project[1], "Archer file transfer")
		# fastq list is given as an input
		file_list = fastq_files_list
		# assume all fastqs were transferred ok
		all_fastqs_transferred_ok=True
		# for each fastq
		for fastq in file_list:
			# ignore undertermined
			if not fastq.startswith("Undetermined"):
				# capture run name to create a run folder on archer platform (sample names are in format ADX001_01_ID1_ID2_etc)
				# The library/run number is always the first item and used to identify the runs
				# TODO potential bug = if there are multiple libraries on the same run, they will be put into different folders, but the completed file command (below) is only run once
				runnumber = "ADX"+fastq.split("ADX")[1].split("_")[0]
				file_to_transfer = os.path.join(config.download_location, project[1].replace("003_","").replace("002_",""),config.fastq_folder_path, fastq)
				self.logger("file to transfer %s" % file_to_transfer, "Archer file transfer")
				# transfer fastq to server
				if not self.transfer_file_to_server(file_to_transfer,runnumber):
					# if it wasn't successful 
					all_fastqs_transferred_ok = False
					self.logger("%s fastq file not transferred successfully" % fastq, "Archer file transfer")
		# if all transferred ok
		if all_fastqs_transferred_ok:					
			# create and move completed file to start analysis
			file_path=os.path.join(self.create_completed_file(runnumber))
			if file_path:
				if self.transfer_file_to_server(file_path, runnumber, complete_file=True):
					self.logger("complete file created for %s" % project[1], "Archer file transfer")
					return True
				else:
					# Rapid7 alert set:
					self.logger("WARNING: complete file NOT created for %s. Run will not be processed" % project[1], "Archer file transfer")
					return False

	def transfer_file_to_server(self,file,runnumber, complete_file=False):
		"""
		for each file use rsync to copy it. 
		first read the password from file.
		use sshpass to provide this to rsync command.
		echo the status of the rsync command - 0 means success
		rsync writes to a temporary file, and moves it into place once it's finished. 
		Thus, if rsync returns a success code, you know the file is present and a correct, complete copy; 
		if rsync hasn't returned an error code then no file will be present (unless there was an older version of the file, 
		in which case that older version won't be modified).
		"""
		if not complete_file:
			# make the runfolder folder (using -p incase it already exists), 
			# then change permissions on the folder to allow the poller to access the data then transfer using rsync 
			cmd = "archer_pw=$(<%s); \
			sshpass -p $archer_pw ssh s_archerupload@grpvgaa01.viapath.local mkdir -p %s ; \
			sshpass -p $archer_pw ssh s_archerupload@grpvgaa01.viapath.local chgrp archer_daemon_web_access %s ; \
			sshpass -p $archer_pw rsync %s s_archerupload@grpvgaa01.viapath.local:%s/;\
			echo $?" % (
				config.path_to_archerdx_pw, 
				os.path.join(config.path_to_watch_folder,runnumber),
				os.path.join(config.path_to_watch_folder,runnumber),
				file, 
				os.path.join(config.path_to_watch_folder,runnumber)
				)
		else:
			cmd = "archer_pw=$(<%s); sshpass -p $archer_pw rsync %s s_archerupload@grpvgaa01.viapath.local:%s;echo $?" % (
				config.path_to_archerdx_pw, file, os.path.join(config.path_to_watch_folder, file.split("/")[-1]))
		# capture stdout and look for exit code
		self.logger(cmd,"Archer file transfer")
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out.rstrip(), "0"):
			return runnumber
		return False

	def create_completed_file(self,run_number):
		"""
		Create an empty text file to denote the analysis can start
		file shoul be called runnumber.completed, i.e.: ADX##.completed 
		File made here, moved to correct location on Archer server by transfer_file_to_server() above
		"""
		# make the file needed to start analysis
		file_path=os.path.join(config.download_location,run_number+".completed")
		cmd = "touch %s;echo $?" % (file_path)
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out, "0"):
			return file_path
	
	def cleanup(self, project):
		"""
		If the files have been transferred to the server ok we can delete the downloaded files.
		Input - project (tuple (projectid, projectname))
		"""
		# downloaded fastq files location on genomics server
		path_to_downloaded_files = os.path.join(config.download_location, project[1].replace("003_","").replace("002_",""))
		# command to delete the downloaded fastq files
		cmd = "rm -r %s; echo $?" % (path_to_downloaded_files)
		self.logger("Command to delete project %s downloaded files (%s)" % (project[1], path_to_downloaded_files), "Archer Cleanup")
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out, "0"):
			self.logger("Runfolder for project %s deleted successfully from genomics server" % project[1], "Archer Cleanup")
			return True
		else:
			# Rapid7 alert set:
			self.logger("WARNING: Runfolder for project %s NOT deleted from genomics server." % project[1], "Archer Cleanup")
			return False

	def create_file_to_stop_subsequent_processing(self,project):
		"""
		Create a file "project_name.txt" and save in the processed runs folder (/mokaguys/logfiles/processed_runs/)
		Add text to the file- success statement if all processed ok
		This will prevent future processing of the run-
		the script looks for this file containing the success statement to decide whether to process the run
		"""
		file_path=os.path.join(config.processed_runs_folder,project[1]+".txt")
		cmd = "touch %s;echo $?" % (file_path)
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out, "0"):
			with open(file_path,'w') as logfile_to_write:
				logfile_to_write.write(config.success_statement)
			return file_path

	def success_in_stdout(self,stdout, expected_txt=False):
		"""
		Returns True if expected statement in stdout
		"""
		if expected_txt:
			if expected_txt in stdout:
				return True
				
	def errors_in_stderr(self, stderr, stage):
		"""
		Returns True if any stderror to worry about.
		"""
		if stage == "inspect":
			if "panic:" in stderr:
				return True
		if stage == "download":
			if "panic:" in stderr:
				return True
		return False

	def execute_subprocess_command(self, command):
		"""
		Input = command (string)
		Takes a command, executes using subprocess.Popen
		Returns =  (stdout,stderr) (tuple)
		"""
		proc = subprocess.Popen(
			[command],
			stderr=subprocess.PIPE,
			stdout=subprocess.PIPE,
			shell=True,
			executable="/bin/bash",
		)
		# capture the streams
		return proc.communicate()
	
	def logger(self, message, tool):
		"""Write log messages to the system log.
		Arguments:
		message (str)
			Details about the logged event.
		tool (str)
			Tool name. Used to search within the insight ops website.
		"""
		# Create subprocess command string, passing message and tool name to the command
		log = "/usr/bin/logger -t %s '%s'" % (tool, message)

		if subprocess.call([log], shell=True) == 0:
			# If the log command produced no errors, record the log command string to the script logfile.
			self.script_logfile.write("Log written - " + tool + ": " + message + "\n")
		# Else record failure to write to system log to the script log file
		else:
			self.script_logfile.write("Failed to write log to /usr/bin/logger\n" + log + "\n")

	def go(self):
		for project in self.list_projects():
			fastq_files_closed, fastq_files_list  = self.check_all_files_closed(project)
			if not self.check_if_already_actioned(project) and fastq_files_closed:
				if self.create_filtered_manifest_file(project):
					if self.download_using_manifest_file(project):
						if self.organise_file_transfer(project, fastq_files_list):
							if self.cleanup(project):
								self.create_file_to_stop_subsequent_processing(project)

if __name__ == "__main__":
	archer = Archerdx()
	archer.go()