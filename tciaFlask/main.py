__author__ = 'arosado'

from flask import Flask
from flask import request
from flask import Response
from flask import render_template
from tciaFlaskApi.tciaFlaskHelper import TciaFlaskHelper
from tciaFlaskApi.tciaMongoDbHelper import TciaMongoDbHelper

app = Flask('tcia')
env = app.create_jinja_environment()

tciaFlaskViewHelper = TciaFlaskHelper()
tciaMongoDbHelper = TciaMongoDbHelper()

@app.route('/')
def index():
    return 'Hello world!'

@app.route('/view')
def view():
    templateVars = {}
    templateVariables = tciaFlaskViewHelper.initialTemplateVariables()
    templateVariables = tciaFlaskViewHelper.initialViewerSetup(templateVariables)
    #app.update_template_context(templateVariables)
    template = env.get_template('viewer.html')
    renderedTemplate = template.render(templateVariables)
    #renderedTemplate = render_template('viewer.html')
    #app.update_template_context(templateVariables)
    #app.context_processor
    return renderedTemplate

@app.route('/queryJson')
def queryJson():
    if request.method == 'GET':
        collectionId = request.args.get('collectionId')
        patientId = request.args.get('patientId')
        queryType = request.args.get('type')
        seriesId = request.args.get('seriesId')
        studyId = request.args.get('studyId')
        if queryType == 'patient':
            if patientId != None:
                pass
            if collectionId != None:
                json = tciaMongoDbHelper.retrieveTciaPatientListByCollectionId({'_id': collectionId})
                response = Response(json, mimetype='javascript/json')
        if queryType == 'study':
            if patientId != None:
                json = tciaMongoDbHelper.retrieveTciaStudyListByPatientId({'_id': patientId})
                response = Response(json, mimetype='javascript/json')
        if queryType == 'series':
            if seriesId != None:
                json = tciaMongoDbHelper.retrieveTciaSeriesBySeriesId({'_id': seriesId})
                response = Response(json, mimetype='javascript/json')
            if studyId != None:
                json = tciaMongoDbHelper.retrieveTciaSeriesListByStudyId({'_id': studyId})
                response = Response(json, mimetype='javascript/json')
    return response

@app.route('/static/', defaults={'path': ''})
@app.route('/static/<path:path>')
def static_proxy(path):
    return app.send_static_file(path)

if __name__ == '__main__':
    app.run(debug=True)