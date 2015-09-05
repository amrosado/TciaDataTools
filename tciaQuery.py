__author__ = 'arosado'

from pymongo import MongoClient

class TciaQuery:
    mongoClient = None
    mongoDb = None
    mongoSeriesCollection = None
    mongoSeriesSizesCollection = None
    mongoPatientCollection = None
    mongoCollectionCollection = None
    mongoStudyCollection = None

    def __init__(self):
        try:
            self.mongoClient = MongoClient('localhost', 27017)
            self.mongoDb = self.mongoClient.get_database('TCIA')
            if 'collections' in self.mongoDb.collection_names():
                self.mongoCollectionCollection = self.mongoDb.get_collection('collections')
            if 'series' in self.mongoDb.collection_names():
                self.mongoSeriesCollection = self.mongoDb.get_collection('series')
            if 'seriessizes' in self.mongoDb.collection_names():
                self.mongoSeriesSizesCollection = self.mongoDb.get_collection('seriessizes')
            if 'patients' in self.mongoDb.collection_names():
                self.mongoPatientCollection = self.mongoDb.get_collection('patients')
            if 'studies' in self.mongoDb.collection_names():
                self.mongoPatientCollection = self.mongoDb.get_collection('studies')
        except:
            print('Failed to connect to mongoDB properly')
