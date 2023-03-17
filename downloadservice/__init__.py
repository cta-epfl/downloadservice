import os
from flask import Flask, request
from flask_oidc import OpenIDConnect

app = Flask(__name__)

app.config['OIDC_CLIENT_SECRETS'] = '/home/savchenk/cscs/sdc/downloadservice/secrets.json'
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET')
app.config['OIDC_COOKIE_SECURE'] = False
# https://ds.dev.ctaodc.ch/hub/oauth_callback

oidc = OpenIDConnect(app)

@app.route('/')
def index():
    if oidc.user_loggedin:
        return 'Welcome %s' % oidc.user_getfield('email')
    else:
        return 'Not logged in, <a href="login">login</a>'

@app.route('/login')
@oidc.require_login
def login():
    return 'Welcome %s' % oidc.user_getfield('email')

# @app.route('/')
# def index():
#     return oidc.redirect_to_auth_server(None, request.values)

# @app.route('/custom_callback')
# # @oidc.custom_callback
# def callback(data):
#     return 'Hello. You submitted %s' % data