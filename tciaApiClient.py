__author__ = 'arosado'

import requests
import json
from apikey import ApiKeyHolder

class TciaApiClient:

    apiKey = None
    baseUrl = None
    apiResourceUrl = None
    apiFormat = None
    sharedResource = '/SharedList'

    currentCollection = None
    currentBodyPartExamined = None
    currentModality = None
    currentStudyInstanceUID = None
    currentSeriesInstanceUID = None
    currentPatientId = None
    currentManufacturerModelName = None
    currentDate = None
    currentCollection = None

    collectionValues = None

    apiConnectionSession = requests.session()

    def makeApiCall(self, queryUrl, queryParameters):
        try:
            queryRequest = self.apiConnectionSession.get(queryUrl, params=queryParameters)

            if queryUrl == (self.apiResourceUrl + '/query/getImage'):
                queryResponse = queryRequest.content
                #zipFile = open('tciaZipTest.zip', mode='wb')
                #zipFile.write(queryResponse)
                #zipFile.close()
            elif self.apiFormat == 'json':
                queryResponse = json.loads(queryRequest.text)
            else:
                queryResponse = queryRequest.text

            return queryResponse

        except:
            print('Problem with performing API Request')

    def getCollectionValues(self):
        queryEndpoint = '/query/getCollectionValues'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning collection values')

    def getModalityValues(self, collection=None, bodyPartExamined=None):
        queryEndpoint = '/query/getModalityValues'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            if collection != None:
                queryParameters['Collection'] = collection
            if bodyPartExamined != None:
                queryParameters['BodyPartExamined'] = bodyPartExamined

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning modality values')

    def getBodyPartValues(self, collection=None, modality=None):
        queryEndpoint = '/query/getBodyPartValues'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            if collection != None:
                queryParameters['Collection'] = collection
            if modality != None:
                queryParameters['Modality'] = modality

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning body part values')

    def getManufacturerValues(self, collection=None, modality=None, bodyPartExamined=None):
        queryEndpoint = '/query/getManufacturerValues'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            if collection != None:
                queryParameters['Collection'] = collection
            if modality != None:
                queryParameters['Modality'] = modality
            if bodyPartExamined != None:
                queryParameters['BodyPartExamined'] = bodyPartExamined

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning manufacture values')

    def getPatient(self, collection=None):
        queryEndpoint = '/query/getPatient'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            if collection != None:
                queryParameters['Collection'] = collection

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning patients by collection')

    def patientsByModality(self, collection, modality):
        queryEndpoint = '/query/PatientsByModality'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        queryParameters['Collection'] = collection
        queryParameters['Modality'] = modality

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning patients by modality')

    def getPatientStudy(self, collection=None, patientId=None, studyInstanceUID=None):
        queryEndpoint = '/query/getPatientStudy'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            if collection != None:
                queryParameters['Collection'] = collection
            if patientId != None:
                queryParameters['PatientID'] = patientId
            if studyInstanceUID != None:
                queryParameters['StudyInstanceUID'] = studyInstanceUID

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning patient study')

    def getSeries(self, collection=None, studyInstanceUID=None, patientId=None, seriesInstanceUID=None, modality=None, bodyPartExamined=None, manufacturerModelName=None, manufacturer=None):
        queryEndpoint = '/query/getSeries'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            if collection != None:
                queryParameters['Collection'] = collection
            if studyInstanceUID != None:
                queryParameters['StudyInstanceUID'] = studyInstanceUID
            if patientId != None:
                queryParameters['PatientID'] = patientId
            if seriesInstanceUID != None:
                queryParameters['SeriesInstanceUID'] = seriesInstanceUID
            if modality != None:
                queryParameters['Modality'] = modality
            if bodyPartExamined != None:
                queryParameters['BodyPartExamined'] = bodyPartExamined
            if manufacturerModelName != None:
                queryParameters['ManufacturerModelName'] = manufacturerModelName
            if manufacturer != None:
                queryParameters['Manufacturer'] = manufacturer

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning series')

    def getSeriesSize(self, seriesInstanceUID):
        queryEndpoint = '/query/getSeriesSize'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        queryParameters['SeriesInstanceUID'] = seriesInstanceUID

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning series size')

    def getImage(self, seriesInstanceUID):
        queryEndpoint = '/query/getImage'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        queryParameters['SeriesInstanceUID'] = seriesInstanceUID
        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)
        except:
            raise Exception('Problem with returning series image zip')

    def getSOPInstanceUIDs(self, seriesInstanceUID):
        queryEndpoint = '/query/getSOPInstanceUIDs'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        queryParameters['SeriesInstanceUID'] = seriesInstanceUID

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)
        except:
            raise Exception('Problem with returning SOP Instances')

    def getSingleImage(self, seriesInstanceUID=None, sopInstanceUID=None):
        queryEndpoint = '/query/getSingleImage'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        if sopInstanceUID != None:
            queryParameters['SOPInstanceUID'] = sopInstanceUID
        if seriesInstanceUID != None:
            queryParameters['SeriesInstanceUID'] = seriesInstanceUID

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)
        except:
            raise Exception('Problem with returning single DICOM object')

    def newPatientsInCollection(self, date, collection):
        queryEndpoint = '/query/NewPatientsInCollection'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        queryParameters['Date'] = date
        queryParameters['Collection'] = collection

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning new patients in collection')

    def newStudiesInPatientCollection(self, date, collection, patientId=None):
        queryEndpoint = '/query/NewStudiesInPatientCollection'
        queryParameters = {'format': self.apiFormat}

        queryUrl = self.apiResourceUrl + queryEndpoint

        queryParameters['Date'] = date
        queryParameters['Collection'] = collection

        try:
            if patientId != None:
                queryParameters['PatientID'] = patientId

            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)

        except:
            raise Exception('Problem with returning new studies in patient collection')

    def sharedList(self, name):
        queryEndpoint = '/query/ContentsByName'
        queryParameters = {'format': self.apiFormat}

        queryParameters['name'] = name

        queryUrl = self.apiResourceUrl + queryEndpoint

        try:
            return self.makeApiCall(queryUrl=queryUrl, queryParameters=queryParameters)
        except:
            raise Exception('Problem with returning shared list')

    def __init__(self, apiKey, baseApiUrl, apiResource, apiFormat):
        self.apiKey = apiKey
        self.baseUrl = baseApiUrl
        self.apiFormat = apiFormat
        self.apiResourceUrl = baseApiUrl + '/' + apiResource
        self.apiSharedResourceUrl = baseApiUrl + '/' + self.sharedResource
        self.apiConnectionSession.headers.update({'api_key': apiKey, 'Accept-Encoding' : '*'})

#Please put in your own api key to use
apiClient = TciaApiClient(ApiKeyHolder.tciaApiKey, 'https://services.cancerimagingarchive.net/services/v3', 'TCIA', 'json')
#apiClient.getCollectionValues()
apiClient.getImage('1.3.6.1.4.1.14519.5.2.1.7695.4001.283259628156413143686226633798')
