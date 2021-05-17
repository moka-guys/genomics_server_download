import os, datetime, subprocess
import config as config
class Archerdx():
	def __init__(self):
		self.now = datetime.datetime.now()

	
	def list_projects(self):
		"""
		generator which returns project
		returns tuple of projectid, projectname
		"""
		# list all projects in nexus
		cmd = config.source_command+";dx find projects --created-after=-7d  --name=*ADX* --auth-token %s" % (config.Nexus_API_Key)
		out, err = self.execute_subprocess_command(cmd)
		for item in out.split("\n"):
			if len(item)>1:
				projectid,colon,projectname,access = item.split(" ")
				yield (projectid, projectname)
	
	def check_if_already_actioned(self, project):
		"""
		"""
		expected_filename = "%s.txt" % (project[1])
		if expected_filename in os.listdir(config.processed_runs_folder):
			if self.check_if_completed_ok(expected_filename):
				print project[1], "processed and completed ok"
				return True
			else:
				print project[1], "processed but NOT completed ok"
				return False
		print project[1], "not yet processed"
		return False

		

	def check_if_completed_ok(self,logfile):
		"""
		"""
		with open(os.path.join(config.processed_runs_folder,logfile)) as logfile_to_check:
			for line in logfile_to_check.readlines():
				if config.success_statement in line:
					return True
		return False

	def check_all_files_closed(self,project):
		"""
		"""
		all_files_closed = True
		file_list = []
		cmd = config.source_command+";dx find data --path %s --name '*.fastq.gz' --auth-token %s" % (project[0], config.Nexus_API_Key)
		out, err = self.execute_subprocess_command(cmd)
		for line in out.split("\n"):
			if len(line) >2 and not line.startswith("closed"):
				all_files_closed = False
			if len(line) > 2:
				file_list.append(line.split("BaseCalls/")[1].split(" ")[0])
		return all_files_closed, file_list

	def create_manifest_file(self, project):
		"""
		"""
		create_manifest_cmd = "python %s %s --recursive  --outfile %s" % (
			config.path_to_manifest_script, 
			project[0], 
			os.path.join(config.manifest_folder,"%s.json.bz2 " % (project[0])))
		filered_manifest_path = os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0]))
		filter_manifest_cmd = "python %s %s --output_file %s '(.*)_001.fastq.gz'" % (
			config.path_to_filter_manifest_script, 
			os.path.join(config.manifest_folder, "%s.json.bz2" % (project[0])),
			os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0]))
			) 
		cmd = "%s;%s" % (create_manifest_cmd,filter_manifest_cmd)
		print cmd
		out, err = self.execute_subprocess_command(cmd)
		if not err:
			print "manifest file created"
			return True
		else:
			print err
			
	def download_using_manifest_file(self,project):
		"""
		"""
		cmd = "%s;%s download %s --auth-token %s" % (config.export_environment, config.path_to_dx_download_client, os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (project[0])),config.Nexus_API_Key)
		out, err = self.execute_subprocess_command(cmd)
		print out,err
		if not self.errors_in_stderr(err, "download") and self.success_in_stdout(out,"Download completed successfully"):
			print "no stderr, success statement in stdout"
			if self.inspect_download(project):
				return True

	def inspect_download(self,project):
		"""
		"""
		#cmd = "%s download %s " % (config.path_to_dx_download_client, os.path.join(config.manifest_folder, "%s_filtered.json.bz2" % (config.download_location, project[0])))
		#print cmd
		#out, err = self.execute_subprocess_command(cmd)
		#print out, err
		#if not self.errors_in_stderr(err, "inspect") and self.success_in_stdout(out,"Integrity check for regular files complete."):
		#	return True
		return True

	def organise_file_transfer(self, project):
		"""
		"""
		file_list = self.check_all_files_closed(project)[1]
		all_fastqs_transferred_ok=True
		for fastq in file_list:
			if not fastq.startswith("Undetermined"):
				# capture run name to create a run folder on archer platform
				runnumber = "ADX"+fastq.split("ADX")[1].split("_")[0]
				print "file to transfer %s" % os.path.join(config.download_location, project[1].replace("003_","").replace("002_",""),config.fastq_folder_path, fastq)
				if not self.transfer_file_to_server(os.path.join(config.download_location, project[1].replace("003_","").replace("002_",""),config.fastq_folder_path, fastq),runnumber):
					all_fastqs_transferred_ok = False
		if all_fastqs_transferred_ok:					
			# create and move completed file to start analysis
			file_path=os.path.join(self.create_completed_file(runnumber))
			if file_path:
				if self.transfer_file_to_server(file_path, runnumber, complete_file=True):
					print "complete file created"
					return True
				else:
					print "complete file not created"
					return False
		else:
			raise AssertionError("Failure when transferring fastqs")

	def transfer_file_to_server(self,file,runnumber, complete_file=False):
		"""
		for each file use rsync to copy it. 
		first read the password from file.
		use sshpass to provide this to rsync command.
		escho the status of the rsync command - 0 means success
		rsync writes to a temporary file, and moves it into place once it's finished. 
		Thus, if rsync returns a success code, you know the file is present and a correct, complete copy; 
		if rsync hasn't returned an error code then no file will be present (unless there was an older version of the file, 
		in which case that older version won't be modified).
		"""
		if not complete_file:
			# make the runfolder folder, then rsync 
			cmd = "archer_pw=$(<%s); sshpass -p $archer_pw ssh s_archerupload@grpvgaa01.viapath.local mkdir -p %s ; sshpass -p $archer_pw ssh s_archerupload@grpvgaa01.viapath.local chgrp archer_daemon_web_access %s ; sshpass -p $archer_pw rsync %s s_archerupload@grpvgaa01.viapath.local:%s/;echo $?" % (config.path_to_archerdx_pw, os.path.join(config.path_to_watch_folder,runnumber),os.path.join(config.path_to_watch_folder,runnumber),file, os.path.join(config.path_to_watch_folder,runnumber))
		else:
			cmd = "archer_pw=$(<%s); sshpass -p $archer_pw rsync %s s_archerupload@grpvgaa01.viapath.local:%s;echo $?" % (config.path_to_archerdx_pw, file, os.path.join(config.path_to_watch_folder, file.split("/")[-1]))
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out.rstrip(), "0"):
			return runnumber
		return False

	def create_completed_file(self,run_number):
		"""
		Create an empty text file to denote the analysis can start
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
		Input - project
		"""
		path_to_downloaded_files = config.download_location, project[1].replace("003_","").replace("002_","")
		cmd = "rm -r %s; echo $?" % (path_to_downloaded_files)
		out, err = self.execute_subprocess_command(cmd)
		if self.success_in_stdout(out, "0"):
			print "runfolder deleted ok"
			return True
		else:
			print "runfolder has not been deleted"
			return False

	def create_file_to_stop_subsequent_processing(self,project):
		with open(os.path.join(config.processed_runs_folder,logfile),'w') as logfile_to_write:
			logfile_to_write(config.success_statement)

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
	
	def go(self):
		for project in self.list_projects():
			if not self.check_if_already_actioned(project) and self.check_all_files_closed(project)[0]:
				if self.create_manifest_file(project):
					if self.download_using_manifest_file(project):
						if self.organise_file_transfer(project):
							if self.cleanup(project):
								self.create_file_to_stop_subsequent_processing(project)


if __name__ == "__main__":
	archer = Archerdx()
	archer.go()