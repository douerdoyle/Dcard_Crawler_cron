from settings.environment          import app
def hello():
    return('api running!')
app.add_url_rule('/'            , view_func=hello       , methods=['GET'])