import os, json, traceback, pymysql
pymysql.install_as_MySQLdb()
from uuid              import uuid1
from sqlalchemy_utils  import create_database, database_exists
from datetime          import datetime
from flask             import Flask
from flask_wtf.csrf    import CSRFProtect
from flask_sqlalchemy  import SQLAlchemy

app  = Flask(__name__)

class Config(object):
    DEBUG      = False
    TESTING    = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = ''
    WTF_CSRF_ENABLED    = False
    WTF_CSRF_SSL_STRICT = False
    RUN_SETTING = {
        'host': '0.0.0.0',
        'port': 80
    }

    DB_SETTING = {
        'DB_TYPE':'mysql',
        'DB_NAME':'',
        'CHARSET':'utf8mb4',
    }

    ES_SETTING = {
        'CONNECTION':{},
        'ES_INDEX':{
            'ARTICLE':{
                'INDEX_NAME_TEMPLATE':'dcard_{}',
                'MAPPING_FILEPATH':'/app/lib/es/dcard_articles.json'
            },
            'COMMENT':{
                'INDEX_NAME_TEMPLATE':'dcard_comments_{}',
                'MAPPING_FILEPATH':'/app/lib/es/dcard_comments.json'
            }
        }
    }

    GOOGLE_SENDER_CONF = {
        'FROM_ADDRESS':'',
        'FROM_ADDRESS_PSW':'',
        'SMTP_SERVER':'smtp.gmail.com',
        'SMTP_PORT':'587',
        'RECEIVER_LIST':['']
    }

def formal_settings():
    app.config['DB_SETTING']['HOST'] = ''
    app.config['DB_SETTING']['PORT'] = ''
    app.config['DB_SETTING']['ACCOUNT'] = ''
    app.config['DB_SETTING']['PASSWORD'] = ''

    app.config['ES_SETTING']['CONNECTION']['HOST'] = ['']
    app.config['ES_SETTING']['CONNECTION']['PORT'] = 9200
    app.config['ES_SETTING']['CONNECTION']['ACCOUNT'] = ''
    app.config['ES_SETTING']['CONNECTION']['PASSWORD'] = ''

def dev_settings():
    app.config['DB_SETTING']['HOST'] = ''
    app.config['DB_SETTING']['PORT'] = ''
    app.config['DB_SETTING']['ACCOUNT'] = ''
    app.config['DB_SETTING']['PASSWORD'] = ''

    app.config['ES_SETTING']['CONNECTION']['HOST'] = ['']
    app.config['ES_SETTING']['CONNECTION']['PORT'] = 9200
    app.config['ES_SETTING']['CONNECTION']['ACCOUNT'] = ''
    app.config['ES_SETTING']['CONNECTION']['PASSWORD'] = ''

def local_dev_settings():
    app.config['DB_SETTING']['HOST'] = ''
    app.config['DB_SETTING']['PORT'] = ''
    app.config['DB_SETTING']['ACCOUNT'] = ''
    app.config['DB_SETTING']['PASSWORD'] = ''

    app.config['ES_SETTING']['CONNECTION']['HOST'] = ['']
    app.config['ES_SETTING']['CONNECTION']['PORT'] = 9200
    app.config['ES_SETTING']['CONNECTION']['ACCOUNT'] = ''
    app.config['ES_SETTING']['CONNECTION']['PASSWORD'] = ''
    for index_category, index_info in app.config['ES_SETTING']['ES_INDEX'].items():
        app.config['ES_SETTING']['ES_INDEX'][index_category]['MAPPING_FILEPATH'] = app.config['ES_SETTING']['ES_INDEX'][index_category]['MAPPING_FILEPATH'].replace('/app/', '../')

def general_settings():
    app.config['SQLALCHEMY_DATABASE_URI'] = '%(DB_TYPE)s://%(ACCOUNT)s:%(PASSWORD)s@%(HOST)s:%(PORT)s/%(DB_NAME)s?charset=%(CHARSET)s' % app.config['DB_SETTING']

app.config.from_object('settings.environment.Config')
dynamic_settings = {
    'FORMALITY':formal_settings,
    'DEV'      :dev_settings,
    'LOCAL_DEV':local_dev_settings,
    None       :local_dev_settings
}
dynamic_settings[os.environ.get('API_PROPERTY')]()
general_settings()

csrf = CSRFProtect()
csrf.init_app(app)
app.url_map.strict_slashes = False
db   = SQLAlchemy(app)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS, GET, PATCH, DELETE, PUT')

    db.session.rollback()
    db.session.close()
    return(response)

@app.teardown_request
def teardown_request(exception):
    if exception:
        db.session.rollback()
    db.session.remove()

try:
    if not database_exists(app.config['SQLALCHEMY_DATABASE_URI']):
        create_database(app.config['SQLALCHEMY_DATABASE_URI'])
except:
    print(traceback.format_exc())