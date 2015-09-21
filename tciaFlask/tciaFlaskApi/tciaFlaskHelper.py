__author__ = 'arosado'

from tciaMongoDbHelper import TciaMongoDbHelper

class TciaFlaskHelper:
    tciaMongoDbHelper = TciaMongoDbHelper()

    def initialTemplateVariables(self):
        templateVariables = {}
        templateVariables['bootstrapCssUrl'] = '/static/bower_components/bootstrap/dist/css/bootstrap.css'
        templateVariables['fontawesomeCssUrl'] = '/static/bower_components/font-awesome/css/font-awesome.css'
        templateVariables['bootstrapJsUrl'] = '/static/bower_components/bootstrap/dist/js/bootstrap.js'
        templateVariables['html5shivJsUrl'] = '/static/bower_components/html5shiv/dist/html5shiv.js'
        templateVariables['respondJsUrl'] = '/static/bower_components/respond/src/respond.js'
        templateVariables['jQueryJsUrl'] = '/static/bower_components/jquery/dist/jquery.js'
        templateVariables['tciaFlaskQueryJsUrl'] = '/static/js/tciaQuery.js'

        return templateVariables

    def initialViewerSetup(self, varDic):
        tableComponents = varDic
        tableComponents['tableHeadings'] = ['Collection Name']
        tableComponents['tciaCollections'] = self.tciaMongoDbHelper.retrieveTciaCollectionsList({'Collection': 'TCGA'})

        return tableComponents

    def __init__(self):
        pass