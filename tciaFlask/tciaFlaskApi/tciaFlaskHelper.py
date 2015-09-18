__author__ = 'arosado'

from tciaMongoDbHelper import TciaMongoDbHelper

class TciaFlaskHelper:
    tciaMongoDbHelper = TciaMongoDbHelper()

    def initialTableSetup(self):
        tableComponents = {}
        tableComponents['tableHeadings'] = ['Collection Name']
        tableComponents['Collections'] = self.tciaMongoDbHelper.retrieveTciaCollectionsList({'Collection': 'TCGA'})

        return tableComponents

    def __init__(self):
        pass