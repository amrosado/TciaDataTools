__author__ = 'arosado'
import pymongo
import requests
import json
import gridfs
from tciaApiClient import TciaApiClient
from apikey import ApiKeyHolder

class TciaSync:
    apiClient = None
    apiClientBaseUrl = None
    mongoDbClient = None
    tciaDb = None
    tciaFilesGridDb = None
    tciaRequestsSession = None

    def retrieveAndUpdateCollections(self):
        try:
            tciaCollectionsCollection = self.tciaDb.get_collection('tciaCollections')
            apiCollections = self.apiClient.getCollectionValues()
            for tciaCollection in apiCollections:
                collectionQuery = tciaCollectionsCollection.find(tciaCollection)
                if collectionQuery.count() > 0:
                    print 'Collection %s already in collection collection' % (str(tciaCollection))
                else:
                    tciaCollectionsCollection.insert_one(tciaCollection)
                    print 'Added collection %s into collection collection' % (str(tciaCollection))
        except:
            raise Exception('Failed to retrieve and update collections')

    def retrieveAndUpdateModalities(self):
        try:
            tciaModalitiesCollection = self.tciaDb.get_collection('tciaModalities')

            tciaModalitiesApiQuery = self.apiClient.getModalityValues()
            for tciaModality in tciaModalitiesApiQuery:
                modalitiesQuery = tciaModalitiesCollection.find(tciaModality)
                if modalitiesQuery.count() > 0:
                    print 'Modality %s already in modality collection' % (str(tciaModality))
                else:
                    tciaModalitiesCollection.insert_one(tciaModality)
                    print 'Added modality %s into modality collection' % (str(tciaModality))

        except:
            raise Exception('Failed to retrieve and update modalities')

    def retrieveAndUpdateBodyParts(self):
        try:
            tciaBodyPartCollection = self.tciaDb.get_collection('tciaBodyParts')

            tciaBodyPartsApiQuery = self.apiClient.getBodyPartValues()
            for tciaBodyPartDic in tciaBodyPartsApiQuery:
                bodyPartQuery = tciaBodyPartCollection.find(tciaBodyPartDic)
                if bodyPartQuery.count() > 0:
                    print 'Body part %s already in body part collection' % (str(tciaBodyPartDic))
                else:
                    tciaBodyPartCollection.insert_one(tciaBodyPartDic)
                    print 'Added body part %s into body part collection' % (str(tciaBodyPartDic))
        except:
            raise Exception('Failed to retrieve and update body parts')

    def retrieveAndUpdateManufactures(self):
        try:
            tciaManufacturesCollection = self.tciaDb.get_collection('tciaManufactures')

            tciaManufacturesApiQuery = self.apiClient.getManufacturerValues()
            for tciaManufactureDic in tciaManufacturesApiQuery:
                manufactureQuery = tciaManufacturesCollection.find(tciaManufactureDic)
                if manufactureQuery.count() > 0:
                    print 'Manufacture %s already in manufacture collection' % (str(tciaManufactureDic))
                else:
                    tciaManufacturesCollection.insert_one(tciaManufactureDic)
                    print 'Added manufacture %s into manufacture collection' % (str(tciaManufactureDic))
        except:
            raise Exception('Failed to retrieve and update manufactures')

    def retrieveAndUpdatePatients(self):
        try:
            tciaPatientsCollection = self.tciaDb.get_collection('tciaPatients')
            tciaPatientApiQuery = self.apiClient.getPatient()

            for tciaPatientDic in tciaPatientApiQuery:
                patientQuery = tciaPatientsCollection.find(tciaPatientDic)
                if patientQuery.count() > 0:
                    print 'Patient %s already in patient collection' % (str(tciaPatientDic))
                else:
                    tciaPatientsCollection.insert_one(tciaPatientDic)
                    print 'Added patient %s into patient collection' % (str(tciaPatientDic))
        except:
            raise Exception('Failed to retrieve and update patients')

    def retrieveAndUpdatePatientStudies(self):
        try:
            tciaPatientStudiesCollection = self.tciaDb.get_collection('tciaPatientStudies')
            tciaPatientCollection = self.tciaDb.get_collection('tciaPatients')

            tciaPatientQuery = tciaPatientCollection.find()

            for patient in tciaPatientQuery:
                tciaPatientStudiesApiQuery = self.apiClient.getPatientStudy(patientId=patient['PatientID'])
                for tciaPatientStudyDic in tciaPatientStudiesApiQuery:
                    patientStudyQuery = tciaPatientStudiesCollection.find(tciaPatientStudyDic)
                    if patientStudyQuery.count() > 0:
                        print 'Patient study %s already in patient studies collection' % (str(tciaPatientStudyDic))
                    else:
                        tciaPatientStudiesCollection.insert_one(tciaPatientStudyDic)
                        print 'Added patient study %s into patient studies collection' % (str(tciaPatientStudyDic))
        except:
            raise Exception('Failed to retrieve and update patient studies')

    def retrieveAndUpdatePatientSeriesSizes(self):
        try:
            tciaPatientSeriesSizeCollection = self.tciaDb.get_collection('tciaPatientSeriesSize')
            tciaPatientStudiesCollection = self.tciaDb.get_collection('tciaPatientStudies')

            tciaPatientStudiesQuery = tciaPatientStudiesCollection.find()

            for tciaPatientStudy in tciaPatientStudiesQuery:
                tciaPatientSeriesSizeApiQuery = self.apiClient.getSeriesSize(tciaPatientStudy['SeriesInstanceUID'])
                for tciaPatientSeriesSizeDic in tciaPatientSeriesSizeApiQuery:
                    patientSeriesSizeQuery = tciaPatientSeriesSizeCollection.find(tciaPatientSeriesSizeDic)
                    if patientSeriesSizeQuery.count() > 0:
                        print 'Patient series size %s already in patient series size collection' % (str(tciaPatientSeriesSizeDic))
                    else:
                        tciaPatientSeriesSizeCollection.insert_one(tciaPatientSeriesSizeDic)
                        print 'Added series size %s into series size collection' % (str(tciaPatientSeriesSizeDic))
        except:
            raise Exception('Failed to retrieve and update patient series sizes collection')

    def retrieveAndUpdatePatientSeries(self):
        try:
            tciaPatientSeriesCollection = self.tciaDb.get_collection('tciaPatientSeries')
            tciaPatientStudiesCollection = self.tciaDb.get_collection('tciaPatientStudies')

            tciaPatientStudiesQuery = tciaPatientStudiesCollection.find()

            for tciaPatientStudy in tciaPatientStudiesQuery:
                tciaPatientSeriesApiQuery = self.apiClient.getSeries(studyInstanceUID=tciaPatientStudy['StudyInstanceUID'])
                for tciaPatientSeriesDic in tciaPatientSeriesApiQuery:
                    patientSeriesSizeQuery = tciaPatientSeriesCollection.find(tciaPatientSeriesDic)
                    if patientSeriesSizeQuery.count() > 0:
                        print 'Patient series %s already in patient series collection' % (str(tciaPatientSeriesDic))
                    else:
                        tciaPatientSeriesCollection.insert_one(tciaPatientSeriesDic)
                        print 'Added series %s into series collection' % (str(tciaPatientSeriesDic))
        except:
            raise Exception('Failed to retrieve and update patient series collection')

    def retrieveAndUpdatePatientZipImages(self):
        try:
            #self.tciaDb.drop_collection('tciaSeriesImagesZipFileList')
            #self.tciaFilesGridDb.drop_collection('tciaSeriesImagesZip')
            tciaSeriesImagesZipFileList = self.tciaDb.get_collection('tciaSeriesImagesZipFileList')
            tciaSeriesImageZipGrid = gridfs.GridFS(self.tciaFilesGridDb, 'tciaSeriesImagesZip')
            tciaPatientSeriesCollection = self.tciaDb.get_collection('tciaPatientSeries')

            tciaPatientSeriesQuery = tciaPatientSeriesCollection.find()

            for tciaPatientSeries in tciaPatientSeriesQuery:
                seriesZipFilename = tciaPatientSeries['SeriesInstanceUID'] + '.zip'
                tciaSeriesZipFileListDict = self.buildImageZipFileListBaseInfo(tciaPatientSeries)
                tciaZipImageFileListQuery = tciaSeriesImagesZipFileList.find(tciaSeriesZipFileListDict)
                if tciaZipImageFileListQuery.count() > 0:
                    for tciaZipImageFileInfo in tciaZipImageFileListQuery:
                        if tciaZipImageFileInfo['active']:
                            if tciaZipImageFileInfo['filename'] == seriesZipFilename:
                                print 'Series zip file %s already into mongo grid' % (str(tciaSeriesZipFileListDict))
                            elif tciaZipImageFileInfo['filename'] != seriesZipFilename:
                                tciaImageZipApiQuery = self.apiClient.getImage(tciaPatientSeries['SeriesInstanceUID'])
                                newTciaImageZipFile = tciaSeriesImageZipGrid.new_file()
                                newTciaImageZipFile.write(tciaImageZipApiQuery)
                                newTciaImageZipFile.filename = seriesZipFilename
                                newTciaImageZipFile.close()
                                tciaZipImageFileInfo['active'] = False
                                tciaSeriesZipFileListDict['active'] = True
                                tciaSeriesZipFileListDict['md5'] = newTciaImageZipFile.md5
                                tciaSeriesZipFileListDict['filename'] = newTciaImageZipFile.filename
                                tciaSeriesZipFileListDict['fileId'] = newTciaImageZipFile._id
                                tciaSeriesImagesZipFileList.insert_one(tciaSeriesZipFileListDict)
                                print 'Updating zip file for series zip file %s into mongo grid' % (str(tciaSeriesZipFileListDict))
                else:
                    tciaImageZipApiQuery = self.apiClient.getImage(tciaPatientSeries['SeriesInstanceUID'])
                    newTciaImageZipFile = tciaSeriesImageZipGrid.new_file()
                    newTciaImageZipFile.write(tciaImageZipApiQuery)
                    newTciaImageZipFile.filename = seriesZipFilename
                    newTciaImageZipFile.close()
                    tciaSeriesZipFileListDict['active'] = True
                    tciaSeriesZipFileListDict['md5'] = newTciaImageZipFile.md5
                    tciaSeriesZipFileListDict['filename'] = newTciaImageZipFile.filename
                    tciaSeriesZipFileListDict['fileId'] = newTciaImageZipFile._id
                    tciaSeriesImagesZipFileList.insert_one(tciaSeriesZipFileListDict)
                    print 'Added zip file for series zip file %s into mongo grid' % (str(tciaSeriesZipFileListDict))

            return True

        except:
            raise Exception('Failed to retrieve and update patient zip images collection')

    def buildImageZipFileListBaseInfo(self, tciaPatientSeries):
        fileListInfo = {}
        for name in tciaPatientSeries:
            if name != '_id':
                fileListInfo[name] = tciaPatientSeries[name]
        return fileListInfo

    def retrieveAndUpadateSOPInstanceUids(self):
        try:
            tciaSOPInstanceUidCollection = self.tciaDb.get_collection('tciaSOPInstanceUids')
        except:
            raise Exception('Failed to retrieve and update SOP instance uids collection')

    def retrieveAndUpdateSingleImages(self):
        try:
            tciaSingleImageFileListCollection = self.tciaDb.get_collection('tciaSingleImageFileList')

        except:
            raise Exception('Failed to retrieve and update single images collection')

    def syncTciaDb(self):
        self.retrieveAndUpdateCollections()
        self.retrieveAndUpdateModalities()
        self.retrieveAndUpdateBodyParts()
        self.retrieveAndUpdateManufactures()
        self.retrieveAndUpdatePatients()
        self.retrieveAndUpdatePatientStudies()
        self.retrieveAndUpdatePatientSeriesSizes()
        self.retrieveAndUpdatePatientSeries()
        self.retrieveAndUpdatePatientZipImages()

    def __init__(self, apiBaseUrl=None):
        self.apiClient = TciaApiClient(ApiKeyHolder.tciaApiKey, 'https://services.cancerimagingarchive.net/services/v3', 'TCIA', 'json')
        if apiBaseUrl == None:
            self.apiClientBaseUrl = 'https://services.cancerimagingarchive.net/services/v3'
        self.mongoDbClient = pymongo.MongoClient('localhost', 27017)
        self.tciaDb = self.mongoDbClient.get_database('tciaData')
        self.tciaFilesGridDb = self.mongoDbClient.get_database('tciaDataFiles')
        self.tciaRequestsSession = requests.Session()
        pass

tciaSync = TciaSync()
tciaSync.syncTciaDb()