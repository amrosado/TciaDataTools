__author__ = 'arosado'

from flask import Flask
from flask import request
from flask import Response
from flask import render_template
from tciaFlaskApi.tciaFlaskHelper import TciaFlaskHelper

app = Flask('tcia')

tciaFlaskViewHelper = TciaFlaskHelper()

@app.route('/')
def index():
    templateVars = {}
    tableComponents = tciaFlaskViewHelper.initialTableSetup()
    renderedTemplate = render_template('index.html')
    return 'Hello world!'

if __name__ == '__main__':
    app.run()