__author__ = 'arosado'

import pymongo
import json
from tciaApiClient import TciaApiClient
import gridfs
from apikey import ApiKeyHolder

class TciaDataSyncer:
    #

    apiCollections = None
    apiClient = None
    apiModalities = None
    apiBodyParts = None
    apiManufactures = None
    apiPatients = None
    apiPatientStudies = None
    apiSeries = None
    apiSeriesSizes = None
    apiImages = None
    apiNewPatientsInCollection = None
    apiNewStudiesInPatientCollection = None

    apiSharedList = None

    mongodbConnection = None
    mongoTciaDatabase = None
    mongoGridFs = None

    def loadCollections(self):
        try:
            self.apiCollections = self.apiClient.getCollectionValues()
            pass
        except:
            print('Failed to get collections object using api client')

    def loadModalities(self):
        modalitiesHolder = []

        try:
            for collection in self.apiCollections:
                modalitiesHolder.append({collection['Collection']: self.apiClient.getModalityValues(collection['Collection'])})

            self.apiModalities = modalitiesHolder

        except:
            print('Failed to get modality value objects using api client')

    def loadBodyParts(self):
        bodyPartHolder = []

        try:
            for collection in self.apiCollections:
                bodyPartHolder.append({collection['Collection']: self.apiClient.getBodyPartValues(collection=collection['Collection'])})

            self.apiBodyParts = bodyPartHolder

        except:
            print('Failed to get body part value objects using api client')

    def loadManufactures(self):
        manufactureHolder = []

        try:
            for collection in self.apiCollections:
                manufactureHolder.append({collection['Collection']: self.apiClient.getManufacturerValues(collection=collection['Collection'])})

            self.apiManufactures = manufactureHolder

        except:
            print('Failed to get manufacture objects using api client')

    def loadPatients(self):
        patientHolder = []

        try:
            for collection in self.apiCollections:
                patientHolder.append({collection['Collection']: self.apiClient.getPatient(collection=collection['Collection'])})

            self.apiPatients = patientHolder

        except:
            print('Failed to get patient objects using api client')

    def loadPatientStudies(self):
        patientStudyHolder = []

        try:
            for collection in self.apiPatients:
                for collectionKey in collection:
                    for patient in collection[collectionKey]:
                        patientStudyHolder.append({patient['PatientID']: self.apiClient.getPatientStudy(patientId=patient['PatientID'])})
                break

            self.apiPatientStudies = patientStudyHolder

        except:
            print('Failed to get patient study objects using api client')

    def loadSeries(self):
        seriesHolder = []

        try:
            for patient in self.apiPatientStudies:
                for patientId in patient:
                    for study in patient[patientId]:
                        seriesHolder.append({study['StudyInstanceUID']: self.apiClient.getSeries(studyInstanceUID=study['StudyInstanceUID'])})

            self.apiSeries = seriesHolder

        except:
            print('Failed to get series objects using api client')

    def loadSeriesSize(self):
        seriesSizeHolder = []

        try:
            for patient in self.apiPatientStudies:
                for patientId in patient:
                    for study in patient[patientId]:
                        seriesSizeHolder.append({study['StudyInstanceUID']: self.apiClient.getSeries(studyInstanceUID=study['StudyInstanceUID'])})

            self.apiSeriesSizes = seriesSizeHolder

        except:
            print('Failed to get series size objects using api client')

    def loadSeriesDataIntoMongoDb(self):
        seriesCollection = None

        try:
            if 'series' in self.mongoTciaDatabase.collection_names():
                seriesCollection = self.mongoTciaDatabase.get_collection('series')

            else:
                print('Creating new collection for series file ids')
                seriesCollection = self.mongoTciaDatabase.get_collection('series')

            for study in self.apiSeries:
                for studyId in study:
                    for series in study[studyId]:
                        try:
                            querySeries = seriesCollection.find_one({'SeriesInstanceUID': series['SeriesInstanceUID']})
                            if querySeries == None:
                                print('Series '+series['SeriesInstanceUID']+' being added to database')
                                seriesCollection.insert_one(series)
                            if querySeries != None:
                                print('Series '+series['SeriesInstanceUID']+ ' already in database')
                        except:
                            print('Failed to add series '+series['SeriesInstanceUID']+' to database')
        except:
            print('Failed to load series data documents into MongoDb')

    def loadSeriesImagesIntoMongoDb(self):
        imagesHolder = []

        fileIdCollection = None
        temp = None

        try:
            if 'seriesFileIds' in self.mongoTciaDatabase.collection_names():
                fileIdCollection = self.mongoTciaDatabase.get_collection('seriesFileIds')

            else:
                print('Creating new collection for series file ids')
                fileIdCollection = self.mongoTciaDatabase.get_collection('seriesFileIds')


            for study in self.apiSeries:
                for studyId in study:
                    for series in study[studyId]:
                        try:
                            querySeries = fileIdCollection.find_one({'SeriesInstanceUID': series['SeriesInstanceUID']})
                            if querySeries == None:
                                print('Series '+series['SeriesInstanceUID']+' images zip being added to database')
                                seriesFileName = series['SeriesInstanceUID']+'.zip'
                                newFile = self.mongoGridFs.new_file(filename=seriesFileName)
                                newFile.write(self.apiClient.getImage(series['SeriesInstanceUID']))
                                newFile.close()
                                print('Downloaded and saved series '+series['SeriesInstanceUID']+' images zip')
                                #imagesHolder.append({series['SeriesInstanceUID']: self.apiClient.getImage(series['SeriesInstanceUID'])})
                                seriesFileIdDocument = {"SeriesInstanceUID": series['SeriesInstanceUID'], "fileId": newFile._id, "filename": seriesFileName}
                                fileIdCollection.insert_one(seriesFileIdDocument)
                                print('Added series file id to file id collection')
                            if querySeries != None:
                                print('Series '+series['SeriesInstanceUID']+' images zip already in database')
                        except:
                            print('Failed to add series to database')

            # testFile = self.mongoGridFs.get(temp['fileId']).read()
            # testFileSave = open('test', mode='wb')
            # testFileSave.write(testFile)
            # testFileSave.close()

            #self.apiImages = imagesHolder

        except:
            print('Failed to get image objects using api client')

    def saveObjectsJson(self):
        try:
            if self.apiCollections != None:
                collectionFile = open('tciaCollections.json', mode='wb')
                json.dump(self.apiCollections, collectionFile)
            if self.apiModalities != None:
                modalitiesFile = open('tciaModalities.json', mode='wb')
                json.dump(self.apiModalities, modalitiesFile)
            if self.apiBodyParts != None:
                bodyPartsFile = open('tciaBodyParts.json', mode='wb')
                json.dump(self.apiBodyParts, bodyPartsFile)
            if self.apiManufactures != None:
                manufacturesFile = open('tciaManufactures.json', mode='wb')
                json.dump(self.apiManufactures, manufacturesFile)
            if self.apiPatients != None:
                patientsFile = open('tciaPatients.json', mode='wb')
                json.dump(self.apiPatients, patientsFile)
            if self.apiPatientStudies != None:
                patientStudiesFile = open('tciaPatientStudies.json', mode='wb')
                json.dump(self.apiPatientStudies, patientStudiesFile)
            if self.apiSeries != None:
                seriesFile = open('tciaSeries.json', mode='wb')
                json.dump(self.apiSeries, seriesFile)
            if self.apiSeriesSizes != None:
                seriesSizeFile = open('tciaSeriesSizes.json', mode='wb')
                json.dump(self.apiSeriesSizes, seriesSizeFile)


        except:
            print('Failed to save objects into json')

    def loadObjectsJson(self):
        try:
            collectionFile = open('tciaCollections.json', mode='rb')
            self.apiCollections = json.load(collectionFile)
        except:
            print('Failed to load collection json object')
        try:
            modalitiesFile = open('tciaModalities.json', mode='rb')
            self.apiModalities = json.load(modalitiesFile)
        except:
            print('Failed to load modalities json object')
        try:
            bodyPartsFile = open('tciaBodyParts.json', mode='rb')
            self.apiBodyParts = json.load(bodyPartsFile)
        except:
            print('Failed to load body parts json object')

        try:
            manufacturesFile = open('tciaManufactures.json', mode='rb')
            self.apiManufactures = json.load(manufacturesFile)
        except:
            print('Failed to load manufactures json object')

        try:
            patientsFile = open('tciaPatients.json', mode='rb')
            self.apiPatients = json.load(patientsFile)
        except:
            print('Failed to load patients json object')

        try:
            patientStudiesFile = open('tciaPatientStudies.json', mode='rb')
            self.apiPatientStudies = json.load(patientStudiesFile)
        except:
            print('Failed to load patient studies json object')

        try:
            seriesFile = open('tciaSeries.json', mode='rb')
            self.apiSeries = json.load(seriesFile)
        except:
            print('Failed to load series json object')

        try:
            seriesSizeFile = open('tciaSeriesSizes.json', mode='rb')
            self.apiSeriesSizes = json.load(seriesSizeFile)
        except:
            print('Failed to load series sizes json object')

        try:
            imagesFile = open('tciaImages.json', mode='rb')
            self.apiImages = json.load(imagesFile)
        except:
            print('Failed to load images json object')

    def loadJsonIntoMongoDb(self):
        print("Loading collections into mongodb")
        if self.apiCollections != None:
            try:
                if 'collections' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('collections')
                collectionDb = self.mongoTciaDatabase.get_collection('collections')
                for collection in self.apiCollections:
                    collectionDb.insert(collection)
            except:
                print('Failed to load collections into database')
        print("Loading body parts into mongodb")
        if self.apiBodyParts != None:
            try:
                if 'bodyparts' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('bodyparts')
                collectionDb = self.mongoTciaDatabase.get_collection('bodyparts')
                for bodypart in self.apiBodyParts:
                    pass
            except:
                print('Failed to load body parts into database')
        print("Loading manufactures into mongodb")
        if self.apiManufactures != None:
            try:
                if 'manufactures' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('manufactures')
                collectionDb = self.mongoTciaDatabase.get_collection('manufactures')
                for manufacture in self.apiManufactures:
                    pass
            except:
                print('Failed to load manufactures into database')
        print("Loading modalities into mongodb")
        if self.apiModalities != None:
            try:
                if 'modalities' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('modalities')
                collectionDb = self.mongoTciaDatabase.get_collection('modalities')
                for modals in self.apiModalities:
                    pass
            except:
                print('Failed to load modalities into database')
        print("Loading patients into mongodb")
        if self.apiPatients != None:
            try:
                if 'patients' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('patients')
                collectionDb = self.mongoTciaDatabase.get_collection('patients')
                for collection in self.apiPatients:
                    for patientCollection in collection:
                        for patient in collection[patientCollection]:
                            collectionDb.insert_one(patient)
            except:
                print('Failed to load patients into database')
        print("Loading patient studies into mongodb")
        if self.apiPatientStudies != None:
            try:
                if 'patientstudies' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('patientstudies')
                collectionDb = self.mongoTciaDatabase.get_collection('patientstudies')

                for patient in self.apiPatientStudies:
                    for patientKey in patient:
                        for patientStudy in patient[patientKey]:
                            collectionDb.insert_one(patientStudy)
            except:
                print("Failed to load patient studies into database")
        print("Loading series into mongodb")
        if self.apiSeries != None:
            try:
                if 'series' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('series')
                collectionDb = self.mongoTciaDatabase.get_collection('series')
                for study in self.apiSeries:
                    for studyKey in study:
                        for series in study[studyKey]:
                            collectionDb.insert_one(series)
            except:
                print('Failed to load the series into database')
        print("Loading series sizes into mongodb")
        if self.apiSeriesSizes != None:
            try:
                if 'seriessizes' in self.mongoTciaDatabase.collection_names():
                    self.mongoTciaDatabase.drop_collection('seriessizes')
                collectionDb = self.mongoTciaDatabase.get_collection('seriessizes')
                for study in self.apiSeriesSizes:
                    for studyKey in study:
                        for seriessize in study[studyKey]:
                            collectionDb.insert_one(seriessize)
            except:
                print('Failed to load series sizes into mongodb')
        print('Finished loading data into database')

    def __init__(self):
        #Please use your own api key.
        self.apiClient = TciaApiClient(ApiKeyHolder.tciaApiKey, 'https://services.cancerimagingarchive.net/services/v3', 'TCIA', 'json')

        try:
            self.mongodbConnection = pymongo.MongoClient('localhost', 27017)
            databaseNames = self.mongodbConnection.database_names()
            if 'TCIA' in databaseNames:
                print('TCIA database found... using TCIA database ')
                self.mongoTciaDatabase = self.mongodbConnection.get_database('TCIA')
                self.mongoGridFs = gridfs.GridFS(self.mongoTciaDatabase)
            else:
                print('Creating new database...')
                self.mongoTciaDatabase = self.mongodbConnection.get_database('TCIA')
                self.mongoGridFs = gridfs.GridFS(self.mongoTciaDatabase)

        except:
            print('Error initiating sync.  Check MongoDB connection and initial setup.')
            exit()



tciaSync = TciaDataSyncer()
# tciaSync.loadCollections()
# tciaSync.saveObjectsJson()
# tciaSync.loadModalities()
# tciaSync.saveObjectsJson()
# tciaSync.loadBodyParts()
# tciaSync.saveObjectsJson()
# tciaSync.loadManufactures()
# tciaSync.saveObjectsJson()
# tciaSync.loadPatients()
# tciaSync.saveObjectsJson()
# tciaSync.loadPatientStudies()
# tciaSync.saveObjectsJson()
# tciaSync.loadSeries()
# tciaSync.saveObjectsJson()
# tciaSync.loadSeriesSize()
# tciaSync.saveObjectsJson()
#tciaSync.loadSeriesDataIntoMongoDb()



tciaSync.loadObjectsJson()
tciaSync.loadJsonIntoMongoDb()
#tciaSync.loadSeriesImagesIntoMongoDb()