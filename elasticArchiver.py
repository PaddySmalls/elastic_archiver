
import smtplib
import json
import requests
import datetime
import time
import logging
import logging.handlers

class ElasticArchiver:

	HTTP_OK = 200
	HTTP_SERVER_ERROR = 500
	HTTP_NOT_FOUND = 404

	SUCCESS_MESSAGE = "Elasticsearch backup successful"
	FAILURE_MESSAGE = "Elasticsearch backup failed"

	MILLIS_PER_DAY = 24 * 60 * 60 * 1000

	



	def __init__(self, backupPath, repositoryName, backupExpirationInDays, elasticURI, loggingPath, sendMail=False, smtpIP="", mailSender="", mailReceiver=""):  
		self.__backupPath = backupPath
		self.__repositoryName = repositoryName
		self.__backupExpirationInDays = backupExpirationInDays
		self.__elasticURI = elasticURI
		self.__sendMail = sendMail
		self.__smtp = smtpIP
		self.__sender = mailSender
		self.__receiver = mailReceiver
		self.__requestHeader = {'Content-Type':'application/json'}
		self.__repoSettingsJSON = {"type":"fs", "settings": { "compress":"true", "location": self.__backupPath }}
		self.__loggingPath = loggingPath

		log_handler = logging.handlers.TimedRotatingFileHandler(filename=self.__loggingPath + "/archiver.log", when='W6', backupCount=5)
		formatter = logging.Formatter('%(asctime)s - ElasticTreasure: %(message)s', '%b %d %H:%M:%S')
		log_handler.setFormatter(formatter)
		logger = logging.getLogger()
		logger.addHandler(log_handler)
		logger.setLevel(logging.INFO)



### 	Public API


	def startBackupProcess(self):
		logging.info("Starting backup process..")
		response = self.__sendBackupRequest()
		self.__evalResponse(response)		
	

	

	def cleanUpBackups(self):
		logging.info("Starting clean up outdated backups..")
		response = self.__retrieveAllSnapshots()
		self.__deleteExpired(response)
	


	def deleteAllSnapshotsFromRepo(self):
		res = self.__retrieveAllSnapshots()
		snapshotList = json.loads(res.content)["snapshots"]
		for s in snapshotList:
			requests.delete(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/" + s["snapshot"])



	def restoreBackup(self, snapshotID):
		logging.info("Start restoring snapshot: " + snapshotID)
		if(True == self.__snapshotExists(snapshotID)):
			self.__sendRestoringRequest(snapshotID)
		else:
			logging.error("Error: Snapshot " + snapshotID + " does not exist. Restoring process terminated.")






### Restore existing backups


	def __snapshotExists(self, snapshotID):
		response = requests.get(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/" + snapshotID)
		if(self.HTTP_OK == response.status_code):
			return True
                else:
                        return False


        def __sendRestoringRequest(self, snapshotID):
                response = requests.post(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/" + snapshotID + "/_restore")
                if(self.HTTP_OK == response.status_code):
                        logging.info("Successfully restored backup: " + backupID)
		elif(self.HTTP_SERVER_ERROR == response.status_code):
			logging.error("Error: Are you trying to restore an open index? - " + response.content)
                elif(self.HTTP_NOT_FOUND == response.status_code):
                        logging.error("Error: Unable to find snapshot with name " + snapshotID + ". - " + response.content)
		else:
			logging.error("Oops, something went wrong!")




###     Deleteing expired snapshots

	

	def __retrieveAllSnapshots(self):
		logging.info("Retrieve all snapshots from repository..")
		response = requests.get(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/_all")
		return response
	

	
	def __deleteExpired(self, response):
		if(self.HTTP_OK == response.status_code):
			logging.info("Start searching for expired backups..")
			snapshotList = json.loads(response.content)["snapshots"]
			for snapshot in snapshotList:
				self.__removeIfExpired(snapshot)
		else:
			logging.info("Could not find any snapshots!")


	def __removeIfExpired(self, snapshot):
		snapshotAgeInMillis = snapshot["start_time_in_millis"]
		snapshotName = snapshot["snapshot"]
		currentTimeMillis = int(round(time.time() * 1000))
		if((currentTimeMillis - snapshotAgeInMillis) > self.__backupExpirationInMillis()):
			self.__sendDeleteRequest(snapshotName)
		else:
			logging.info("Snapshot " + snapshotName + " is not expired yet.")


	
	def __sendDeleteRequest(self, snapshotName):
		logging.info("Found outdated snapshot: " + snapshotName + " - Start sending delete request..")
		response = requests.delete(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/" + snapshotName) 
		if(self.HTTP_OK == response.status_code):
			logging.info("Deleting snapshot " + snapshotName + " has been succesful.") 
		else:
			logging.info("Deleting snapshot " + snapshotName + " failed. Server says: " + response.content)
	

	def __backupExpirationInMillis(self):
		expirationIntervalInMillis = self.__backupExpirationInDays * self.MILLIS_PER_DAY 
		return expirationIntervalInMillis	
	






### 	Creating a backup of Elasticsearch contents



	def __sendBackupRequest(self):
		repoStatus = self.__isRepositoryReady()
		if(repoStatus):
			response = requests.put(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/backup_" + self.__getCurrentDateTime(),
			headers=self.__requestHeader)
			logging.info("Backup request has been sent. Server says: " + response.content)
			return response
		
	

	def __isRepositoryReady(self):
		response = requests.get(self.__elasticURI + "/_snapshot/" + self.__repositoryName)
		if(self.HTTP_OK == response.status_code):
			logging.info("Repository " + self.__repositoryName + " already exists.")
			return True
		else:
			logging.info("Repository " + self.__repositoryName + " does not exist yet.")
			if(self.__createRepoIfNotExists()):
				return True
			else:
				logging.error("An error occurred. Backup procedure terminated.")
				return False
			 
	

	def __createRepoIfNotExists(self):
		logging.info("Creating repository..")
		response = requests.put(self.__elasticURI + "/_snapshot/" + self.__repositoryName, data=json.dumps(self.__repoSettingsJSON), headers=self.__requestHeader)
		if(self.HTTP_OK == response.status_code):
			logging.info("Creating repository has been successful.")
			return True
		else:
			logging.error("Error: Creating repository failed!")
			return False
		


	def __getCurrentDateTime(self):
		timestamp = time.time()
		timeFormatString = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d_%H:%M:%S')
		return timeFormatString
	


	def __evalResponse(self, response):
		if(self.HTTP_OK == response.status_code and True == self.__sendMail):
			logging.info("Request successful. Sending confirmation mail..")
			self.__sendSuccessMail(response)
		elif(True == self.__sendMail):
			logging.info("Something went wrong. Start sending mail containing further details..")
			self.__sendFailureMail(response)


	

###	Sending eMail that shows if our backup process works as expected


	def __sendSuccessMail(self, response):
		mailServer = self.__prepareSMTPServer()
		self.__buildAndSendMail(mailServer, self.SUCCESS_MESSAGE, response)


	def __sendFailureMail(self, response):
		mailServer = self.__prepareSMTPServer()
		self.__buildAndSendMail(mailServer, self.FAILURE_MESSAGE, response)



	def __prepareSMTPServer(self):
		smtpServer = smtplib.SMTP(self.__smtp)
		return smtpServer



	def __buildAndSendMail(self, mailServer, message, response):
		mailMessage = "From: ElasticArchiver" + " <" + self.__sender + ">\nTo: To Person <" + self.__receiver + "> \nSubject: ElasticArchiver backup report\n\n" + message + ": " + response.content
		mailServer.sendmail(self.__sender, self.__receiver, mailMessage)
		mailServer.quit()





 




	
	
