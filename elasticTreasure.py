
import smtplib
import json
import requests
import datetime
import time

class ElasticTreasure:

	HTTP_OK = 200
	HTTP_SERVER_ERROR = 500
	HTTP_NOT_FOUND = 404

	SUCCESS_MESSAGE = "Elasticsearch backup successful"
	FAILURE_MESSAGE = "Elasticsearch backup failed"



	def __init__(self, backupPath, repositoryName, backupExpirationInDays, elasticURI, sendMail=False, smtpIP="", mailSender="", mailReceiver=""):  
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



### 	Public API


	def startBackupProcess(self):
		response = self.__sendBackupRequest()
		self.__evalResponse(response)		
	

	

	def cleanUpBackups(self):
		response = self.__retrieveAllSnapshots()
		self.__deleteExpired(response)
	


	def deleteAllSnapshotsFromRepo(self):
		res = self.__retrieveAllSnapshots()
		print "Statuscode: " + str(res.status_code)
		snapshotList = json.loads(res.content)["snapshots"]
		for s in snapshotList:
			requests.delete(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/" + s["snapshot"])



	def restoreBackup(self, snapshotID):
		if(True == self.__snapshotExists(snapshotID)):
			self.__sendRestoringRequest(snapshotID)






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
                        print "Successfully restored backup: " + backupID
		elif(self.HTTP_SERVER_ERROR == response.status_code):
			print "Error: Are you trying to restore an open index? - " + response.content
                elif(self.HTTP_NOT_FOUND == response.status_code):
                        print "Error: Unable to find snapshot with name " + snapshotID + ". - " + response.content
		else:
			print "Oops, something went wrong!"




###     Deleteing expired snapshots

	

	def __retrieveAllSnapshots(self):
		response = requests.get(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/_all")
		return response
	

	
	def __deleteExpired(self, response):
		if(self.HTTP_OK == response.status_code):
			snapshotList = json.loads(response.content)["snapshots"]
			for snapshot in snapshotList:
				self.__removeIfExpired(snapshot)
		else:
			print "Nothing to backup!"


	def __removeIfExpired(self, snapshot):
		snapshotAgeInMillis = snapshot["start_time_in_millis"]
		currentTimeMillis = int(round(time.time() * 1000))
		if((currentTimeMillis - snapshotAgeInMillis) > self.__backupExpirationInMillis()):
			self.__sendDeleteRequest(snapshot["snapshot"])


	
	def __sendDeleteRequest(self, snapshotName):
		requests.delete(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/" + snapshotName) 


	

	def __backupExpirationInMillis(self):
		expirationIntervalInMillis = self.__backupExpirationInDays * 24 * 60 * 1000
		return expirationIntervalInMillis	
	






### 	Creating a backup of Elasticsearch contents



	def __sendBackupRequest(self):
		repoStatus = self.__isRepositoryReady()
		if(repoStatus):
			response = requests.put(self.__elasticURI + "/_snapshot/" + self.__repositoryName + "/backup_" + self.__getCurrentDateTime(),
			headers=self.__requestHeader)
			return response
		
	

	def __isRepositoryReady(self):
		response = requests.get(self.__elasticURI + "/_snapshot/" + self.__repositoryName)
		if(self.HTTP_OK == response.status_code):
			print "Repo is ready"
			return True
		else:
			print "Repo is not ready. Creating repository.."
			if(self.__createRepoIfNotExists()):
				return True
			else:
				print "Aborting."
				return False
			 
	

	def __createRepoIfNotExists(self):
		response = requests.put(self.__elasticURI + "/_snapshot/" + self.__repositoryName, data=json.dumps(self.__repoSettingsJSON), headers=self.__requestHeader)
		if(self.HTTP_OK == response.status_code):
			print "Creation successful"
			return True
		else:
			print "Creation failed"
			return False
		


	def __getCurrentDateTime(self):
		timestamp = time.time()
		timeFormatString = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d_%H:%M:%S')
		return timeFormatString
	


	def __evalResponse(self, response):
		if(self.HTTP_OK == response.status_code and True == self.__sendMail):
			self.__sendSuccessMail(response)
		elif(True == self.__sendMail):
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
		mailMessage = "From: ElasticTreasure" + " <" + self.__sender + ">\nTo: To Person <" + self.__receiver + "> \nSubject: ElasticTreasure backup report\n\n" + message + ": " + response.content
		mailServer.sendmail(self.__sender, self.__receiver, mailMessage)
		mailServer.quit()





 




	
	
