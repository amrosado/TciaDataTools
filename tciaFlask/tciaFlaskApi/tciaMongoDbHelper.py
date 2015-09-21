__author__ = 'arosado'

import pymongo
import gridfs
import json
from bson.objectid import ObjectId

class TciaMongoDbHelper:
    mongoClient = None
    tciaDb = None
    tciaFilesGridDb = None

    def convertMongoObjectIntoUsefulDict(self, mongoObject):
        returnDict = {}
        for key in mongoObject:
            if key == '_id':
                returnDict[key] = str(mongoObject[key])
            else:
                returnDict[key] = mongoObject[key]
        return returnDict

    def manageQuery(self, queryDict):
        mongoQuery = {}

        for key in queryDict:
            querySplit = queryDict[key].split('-')
            if key == '_id':
                mongoQuery[key] = ObjectId(queryDict[key])
            elif key == 'Collection':
                if queryDict[key] == 'TCGA':
                    mongoQuery[key] = {'$regex': 'TCGA.+'}
                else:
                    mongoQuery[key] = queryDict[key]
            else:
                mongoQuery[key] = queryDict[key]
        return mongoQuery

    def retrieveTciaPatientListByCollectionId(self, collectionFilter):
        patientHolder = []
        tciaCollectionsCollection = self.tciaDb.get_collection('tciaCollections')

        managedQuery = self.manageQuery(collectionFilter)

        tciaCollectionQuery = tciaCollectionsCollection.find_one(managedQuery)

        tciaPatientQuery = {}
        tciaPatientQuery['Collection'] = tciaCollectionQuery['Collection']

        tciaPatientCollection = self.tciaDb.get_collection('tciaPatients')

        tciaPatientQuery = tciaPatientCollection.find(tciaPatientQuery)

        for patient in tciaPatientQuery:
            processedDict = self.convertMongoObjectIntoUsefulDict(patient)
            patientHolder.append(processedDict)

        return json.dumps(patientHolder)

    def retrieveTciaStudyListByPatientId(self, patientFilter):
        studyHolder = []
        tciaPatientsCollection = self.tciaDb.get_collection('tciaPatients')

        managedQuery = self.manageQuery(patientFilter)

        tciaPatientQuery = tciaPatientsCollection.find_one(managedQuery)

        tciaSeriesQueryFilter = {}
        tciaSeriesQueryFilter['PatientID'] = tciaPatientQuery['PatientID']

        tciaPatientStudiesCollection = self.tciaDb.get_collection('tciaPatientStudies')

        tciaPatientStudiesQuery = tciaPatientStudiesCollection.find(tciaSeriesQueryFilter)

        for study in tciaPatientStudiesQuery:
            processedDict = self.convertMongoObjectIntoUsefulDict(study)
            studyHolder.append(processedDict)

        return json.dumps(studyHolder)

    def retrieveTciaSeriesListByStudyId(self, studyFilter):
        seriesHolder = []
        tciaStudiesCollection = self.tciaDb.get_collection('tciaPatientStudies')

        managedQuery = self.manageQuery(studyFilter)

        tciaStudyQuery = tciaStudiesCollection.find_one(managedQuery)

        tciaSeriesQueryFilter = {}
        tciaSeriesQueryFilter['StudyInstanceUID'] = tciaStudyQuery['StudyInstanceUID']

        tciaSeriesCollection = self.tciaDb.get_collection('tciaPatientSeries')

        tciaSeriesQuery = tciaSeriesCollection.find(tciaSeriesQueryFilter)

        for series in tciaSeriesQuery:
            processedDict = self.convertMongoObjectIntoUsefulDict(series)
            seriesHolder.append(processedDict)

        return json.dumps(seriesHolder)

    def retrieveTciaSeriesBySeriesId(self, seriesFilter):
        tciaPatientsCollection = self.tciaDb.get_collection('tciaPatients')

        managedQuery = self.manageQuery(seriesFilter)

        tciaSeriesCollection = self.tciaDb.get_collection('tciaPatientSeries')

        tciaPatientQuery = tciaSeriesCollection.find_one(managedQuery)

        processedDict = self.convertMongoObjectIntoUsefulDict(tciaPatientQuery)

        return json.dumps(processedDict)

    def retrieveTciaCollectionsList(self, dictFilter=None):
        collectionHolder = []
        tciaCollectionsCollection = self.tciaDb.get_collection('tciaCollections')

        managedQuery = self.manageQuery(dictFilter)

        tciaCollectionsQuery = tciaCollectionsCollection.find(managedQuery)

        for collection in tciaCollectionsQuery:
            collectionDict = {}
            collectionDict['name'] = collection['Collection']
            collectionDict['id'] = str(collection['_id'])
            collectionHolder.append(collectionDict)

        return collectionHolder

    def __init__(self):
        self.mongoClient = pymongo.MongoClient('localhost', 27017)
        self.tciaDb = self.mongoClient.get_database('tciaData')
        self.tciaFilesGridDb = self.mongoClient.get_database('tciaDataFiles')