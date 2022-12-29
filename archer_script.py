import os, datetime, subprocess
# import config file
import archer_config as config
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
		# if testing use project defined in config file
		if config.testing:
			# build command including the source cmd (activate the sdk)
			cmd = config.source_command+";dx find projects --name=%s --auth-token %s" % (config.testing_project, config.Nexus_API_Key)
		else:	
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
				self.logger("No outstanding Archer projects found.", "Archer list projects")

	def check_if_already_actioned(self, project):
		"""
		We don't want to process the same run over and over again. 
		Check for presence of a file which is only created if the run is processed successfully.
		see create_file_to_stop_subsequent_processing() for file creation.
		If file is present, calls check_if_completed_ok() to check for presence of success statement
		
		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs:	returns True is project previously analysed, False if not
		"""
		expected_filename = "%s.txt" % (project[1])
		if expected_filename in os.listdir(config.processed_runs_folder):
			if self.check_if_completed_ok(expected_filename):
				if config.testing:
					self.logger("Archer project %s previously actioned and completed ok but testing = true so continuing." % project[1], "Archer check previously actioned")
					return False
				else:
					self.logger("Archer project %s previously actioned and completed ok." % project[1], "Archer check previously actioned")
					return True
			else:
				self.logger("WARNING: Archer project %s previously actioned but NOT completed ok. Will attempt reprocessing." % project[1], "Archer check previously actioned")
				return False
		return False

	def check_if_completed_ok(self,logfile):
		"""
		Looks for success statement in the file created by create_file_to_stop_subsequent_processing()

		Inputs:		logfile- text file generated by create_file_to_stop_subsequent_processing()
		Outputs:	Returns true if statement is present
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
		
		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs: 	all_files_closed - True/False
					file_list - list of fastq files in the project
		"""
		all_files_closed = True
		file_list = []
		# command to list all fastqs in project
		cmd = config.source_command+";dx find data --path %s --name '*.fastq.gz' --auth-token %s" % (project[0], config.Nexus_API_Key)
		out, err = self.execute_subprocess_command(cmd)
		# each fastq found is listed along with it's status eg open/closed.
		# it will be closed when the upload has finished.
		self.script_logfile.write("Fastq files in Archer project %s:\n" % project[1])
		for line in out.split("\n"):
			# check if any files are not closed
			if len(line) >2 and not line.startswith("closed"):
				self.logger("Archer project %s not all files closed (not closed: %s). Will not proceed with download" % (project[1],line.split("BaseCalls/")[1].split(" ")[0]), "Archer check all files closed")
				all_files_closed = False
			# add filename to list and to logfile
			elif len(line) > 2 and line.startswith("closed"):
				filename = line.split("BaseCalls/")[1].split(" ")[0]
				# write fastq filename to script logfile
				self.script_logfile.write("\t" + filename + "\n")
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
		# --no-projects doesn't require a project to be selected which would otherwise require an additional response.
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

		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs:	return True if completes successfully. Manifest file (unflitered) created
		"""
		# dx login required for dxda scripts
		self.dx_login()
		# if we're testing the script the download agent remembers that it has downloaded these previously so will not re-download them
		# this causes the integrity checking step to fail.
		# therefore we need to delete the contents of the manifest folder for this project
		if config.testing:
			self.logger("Testing run - if script is failing at this point you need to `chown $username logfiles/manifest_files/%s`" % project[0], "Test run step")
			cmd = "rm %s; echo $?" % (os.path.join(config.manifest_folder,"%s*" % (project[0])))
			out, err = self.execute_subprocess_command(cmd)
			if self.success_in_stdout(out, "0"):
				self.logger("Testing run - existing manifest files deleted for project %s" % project[0], "Test run step")
			else:
				self.logger("Testing run - existing manifest files NOT deleted for project %s" % project[0], "Test run step")

		manifest_filename = os.path.join(config.manifest_folder,"%s.json.bz2 " % (project[0]))
		# create cmd for dxda script to create manifest (create_manifest.py)
		# take the path to the generate manifest file dxda script (see config) and the projectID, return the manifest file json
		cmd = "python %s %s --recursive -o %s" % (
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
			self.logger("ERROR: Failed to create manifest file (unfiltered) %s. cmd = %s" % (manifest_filename,cmd), "Archer create manifest file")
			return False
	
	def create_filtered_manifest_file(self,project):
		"""
		The DNAnexus download agent (dxda) requires a manifest file, which is a compressed JSON.
		This needs to first be generated by calling create_manifest_file()) and then filtered before it is used to download.
		The python script filter_manifest.py within the dxda package is used filter the manifest file - this is stated in config.
		
		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs: 	return True if completes successfully. Manifest file (filtered) created
		"""
		# create manifest file
		self.create_manifest_file(project)

		# dx login required for dxda scripts
		self.dx_login()

		filtered_manifest_filename = os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0]))
		# create cmd for dxda script to filter manifest (create_manifest.py for fastqs)
		# take the path to the filter manifest file dxda script (see config), the path of the unfiltered manifest file and the projectID 
		# return the filtered manifest file json
		cmd = "python %s %s -o %s '(.*)_001.fastq.gz'" % (
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
			self.logger("ERROR: Failed to create manifest file (filtered) %s. cmd = %s" % (filtered_manifest_filename, cmd), "Archer create filtered manifest file")
			return False
			
	def download_using_manifest_file(self,project):
		"""
		Once the filtered manufest file is created we can use it to download the fastqs using the dxda
		note the folder struture in DNANexus is replicated after download.
		Downloaded files then integrity checked using inspect_download()

		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs:	Returns True if files downloaded and inspect_download does not return any errors. 
					fastq files will be saved on server (location in config file)
		"""
		self.logger("Preparing to download using filtered manifest file", "Archer download fastqs")
		# commend to source the dnanexus sdk and then download using filtered manifest and auth token-
		# cd to the folder where files will be downloaded, source the dnanexus sdk (dnanexus API key given as last command input);
		# give location of the dxda scripts, call the download function to download the fastq files using the filtered manifest file
		cmd = "cd %s;%s;%s download %s --auth-token %s" % (
			config.download_location, 
			config.export_environment, 
			config.path_to_dx_download_client, 
			os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0])),
			config.Nexus_API_Key)
		self.logger("Download fastqs using dxda command: %s" % cmd, "Archer download fastqs")
		out, err = self.execute_subprocess_command(cmd)
		# pass stderr to error checking function which looks for expected error related strings at this stage and stdout to the success checking function
		if not self.errors_in_stderr(err, "download") and self.success_in_stdout(out,"Download completed successfully"):
			self.logger("Fastq files for project %s downloaded to %s" % (project[1], config.download_location), "Archer download fastqs")
			# check the integrity of the downloads
			if self.inspect_download(project):
				return True
		# if any errors thrown by the download fastqs function:
		else:
			# Rapid7 alert set:
			self.logger("ERROR: download FASTQ stderr: %s. \nCheck this error and restart download. See log for list of fastq files expected.\n" % (err), "Archer download fastqs")
			return False

	def inspect_download(self,project):
		"""
		The dxda package includes a function "inspect" which checks the integrity of the downloaded files.
		if error returned from inspect download, only the first file that fails will be listed (and added to the log).
		
		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs:	Returns True if no errors from the dxda inspect function
		"""
		# commend to source the dnanexus sdk and then inspect the download-
		# cd to the folder where files will be downloaded, source the dnanexus sdk (dnanexus API key given as last command input);
		# give location of the dxda scripts, call the inspect function to download the fastq files using the filtered manifest file
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

	def set_up_ssh_known_hosts(self):
		"""
		Need to add the archer server host into the known hosts otherwise at the rsync stage there is an interactive prompt that the authenticity of the host cannot be established
		Therefore we can run a command which tests if the server is in the known hosts list and if not it's added using ssh-keygen and ssh-keyscan
		This is run once each time the docker image is run.
		Returns True if host was added
		"""
		# make ~./ssh and ~/.ssh/known_hosts
		# if the host is not present in this file
		# use ssh-keyscan to get the key and add it in.
		# repeat the ssh-keygen command - this should now return a string which includes "# Host grpvgaa01.viapath.local found" - stdout can be tested for this
		# NB expect some outputs in stderr from the ssh-keyscan command at this stage
		cmd="mkdir -p ~/.ssh; touch ~./ssh/known_hosts;\
			if [ -z $(ssh-keygen -F grpvgaa01.viapath.local) ]; then \
				ssh-keyscan -H grpvgaa01.viapath.local >> ~/.ssh/known_hosts; fi; ssh-keygen -F grpvgaa01.viapath.local"
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out, "Host grpvgaa01.viapath.local found"):
			self.logger("host added to known hosts ok", "SSH set up")
			return True
		else:
			self.logger("host NOT added to known hosts", "SSH set up")
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

		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
					fastq_files_list - list of fastq files generated by check_all_files_closed() (as file_list)
		Outputs:	Returns True if all files transferred correctly and complete file created
		"""
		self.logger("Preparing to transfer files for project %s to Archer Analysis platform" % project[1], "Archer file transfer")
		# fastq list is given as an input
		file_list = fastq_files_list		
		# assume all fastqs were transferred ok
		all_fastqs_transferred_ok=True
		# list of runnumbers
		runnumber_list = []
		# for each fastq
		for fastq in file_list:
			# ignore undertermined
			if not fastq.startswith("Undetermined"):
				# capture run name to create a run folder on archer platform (sample names are in format ADX001_01_ID1_ID2_etc)
				# The library/run number is always the first item and used to identify the runs
				# The lab want these grouped by library/run number but if different libraries are sequenced together they will go into different folders.
				# therefore create a list of library/run number and loop through this to ensure they are all processed
				runnumber = "ADX"+fastq.split("ADX")[1].split("_")[0]+"_"+fastq.split("ADX")[1].split("_")[5]
				runnumber_list.append(runnumber)
				# file structure is maintained from dnanexus project - recreate this path
				file_to_transfer = os.path.join(config.download_location, project[1].replace("003_","").replace("002_",""),config.fastq_folder_path, fastq)
				# add file to be transfered to the logfile
				self.logger("file to transfer %s" % file_to_transfer.replace("\t",""), "Archer file transfer")
				# call transfer_file_to_server() to transfer fastq to server
				if not self.transfer_file_to_server(file_to_transfer,runnumber):
					# if it wasn't successful 
					all_fastqs_transferred_ok = False
					self.logger("%s fastq file not transferred successfully" % fastq, "Archer file transfer")
		# if all transferred ok
		if all_fastqs_transferred_ok:
			for runnumber in set(runnumber_list):
				# create and move completed file to start analysis
				file_path=os.path.join(self.create_completed_file(runnumber))
				if file_path:
					if self.transfer_file_to_server(file_path, runnumber, complete_file=True):
						self.logger("complete file created for runnumber %s in %s. File name %s" % (runnumber,project[1],file_path), "Archer file transfer")
					else:
						# Rapid7 alert set:
						self.logger("WARNING: complete file NOT created for runnumber %s in %s. Run will not be processed" % (runnumber,project[1]), "Archer file transfer")
						return False
		return True

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

		Inputs:		file - fastq filename (generated by organise_file_transfer())
					runnumber - ArcherDX dun number, format ADX###_Pan## (generated by organise_file_transfer())
		Outputs:	Returns runnumber if transfer successful, False if not
		"""
		if not complete_file:
			# make the runfolder folder (using -p incase it already exists), 
			# then change permissions on the folder to allow the poller to access the data then transfer using rsync 
			# echo $? returns exit status of last command, non zero means it's failed
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
			# this command used to transfer the completed file- completed files don't need the permissions change step needed for the fastqs
			cmd = "archer_pw=$(<%s); sshpass -p $archer_pw rsync %s s_archerupload@grpvgaa01.viapath.local:%s;echo $?" % (
				config.path_to_archerdx_pw, file, os.path.join(config.path_to_watch_folder, file.split("/")[-1]))
		# capture stdout and look for exit code
		self.logger(cmd.replace("\t",""),"Archer file transfer")
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out.rstrip(), "0"):
			return runnumber
		return False

	def create_completed_file(self,run_number):
		"""
		Create an empty text file to denote the analysis can start
		file should be named runnumber.completed, i.e.: ADX###_Pan###.completed 
		File made here, moved to correct location on Archer Analysis by transfer_file_to_server() above

		Inputs:		runnumber - ArcherDX run number, format ADX###_Pan## (generated by organise_file_transfer())
		Outputs:	Returns file path for the complete file
		"""
		# make the file needed to start analysis
		if config.testing: # to stop files being picked up and processed on the archer platform when testing.
			file_path=os.path.join(config.download_location,run_number+".testing") 
		else:
			file_path=os.path.join(config.download_location,run_number+".completed")
		cmd = "touch %s;echo $?" % (file_path)
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out, "0"):
			return file_path
	
	def cleanup(self, project):
		"""
		If the files have been transferred to the server ok we can delete the downloaded files.
		
		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs:	Returns True if all files deleted successfully
		"""
		# downloaded fastq files location on genomics server
		path_to_downloaded_files = os.path.join(config.download_location, project[1].replace("003_","").replace("002_",""))
		# command to delete the downloaded fastq files
		cmd = "rm -r %s; echo $?" % (path_to_downloaded_files)
		self.logger("Delete project %s downloaded files (%s). cmd = %s" % (project[1], path_to_downloaded_files, cmd), "Archer Cleanup")
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

		Inputs:		project - a tuple (projectid,projectname) generated by list_projects()
		Outputs:	Returns file path of the file to prevent subsequent processing
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

	def execute_subprocess_command(self,command):
		"""
		Input = command (string)
		Takes a command, executes using subprocess.Popen
		Returns =  (stdout,stderr) (tuple)
		universal_newlines=True is required to force the outputs to be strings not bytes in python 3. For python 3.7 onwards can use text=True instead
		"""
		proc = subprocess.Popen(
			[command],
			stderr=subprocess.PIPE,
			stdout=subprocess.PIPE,
			shell=True,
			universal_newlines=True, 
			executable="/bin/bash",
		)
		# capture the streams
		return proc.communicate()
	
	def logger(self, message, tool):
		"""
		Write log messages to the system log.
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
			self.script_logfile.write(tool + ": " + message + "\n")
		# Else record failure to write to system log to the script log file
		else:
			self.script_logfile.write("Failed to write log to /usr/bin/logger\n" + log + "\n")

	def go(self):
		"""
		Calls all other functions
		"""
		# call list_projects to look for any new ADX projects in DNA nexus. 
		# For each identified project check fastq files are closed (return True), return a lift of fastq files and call subsequent functions
		for project in self.list_projects():
			# check project not already actioned
			if not self.check_if_already_actioned(project):
				fastq_files_closed, fastq_files_list  = self.check_all_files_closed(project)
				# check all fastq files in project are closed
				if fastq_files_closed:
					# generate a filtered manifest file for the project
					if self.create_filtered_manifest_file(project):
						# download the fastq files for the project
						if self.download_using_manifest_file(project):
							if self.set_up_ssh_known_hosts():
								# transfer the fastq files to the Archer Analysis
								if self.organise_file_transfer(project, fastq_files_list):
									# delete the copy of the fastq files from the genomics server
									if self.cleanup(project):
										# create the file to prevent the project being processed again
										self.create_file_to_stop_subsequent_processing(project)

if __name__ == "__main__":
	archer = Archerdx()
	archer.go()