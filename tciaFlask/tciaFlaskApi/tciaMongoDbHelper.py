__author__ = 'arosado'

import pymongo
import gridfs

class TciaMongoDbHelper:
    mongoClient = None
    tciaDb = None
    tciaFilesGridDb = None

    def manageQuery(self, queryDict):
        mongoQuery = {}

        for key in queryDict:
            querySplit = queryDict[key].split('-')
            if key == 'Collection':
                if queryDict[key] == 'TCGA':
                    mongoQuery[key] = {'$regex': 'TCGA.+'}
                else:
                    mongoQuery[key] = queryDict[key]
            else:
                mongoQuery[key] = queryDict[key]
        return mongoQuery

    def retrieveTciaCollectionsList(self, dictFilter=None):
        collectionHolder = []
        tciaCollectionsCollection = self.tciaDb.get_collection('tciaCollections')

        managedQuery = self.manageQuery(dictFilter)

        tciaCollectionsQuery = tciaCollectionsCollection.find(managedQuery)

        for collection in tciaCollectionsQuery:
            collectionHolder.append(collection['Collection'])

        return collectionHolder

    def __init__(self):
        self.mongoClient = pymongo.MongoClient('localhost', 27017)
        self.tciaDb = self.mongoClient.get_database('tciaData')
        self.tciaFilesGridDb = self.mongoClient.get_database('tciaDataFiles')