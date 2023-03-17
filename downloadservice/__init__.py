import os
from flask import Flask, request
from flask_oidc import OpenIDConnect
import gfal2

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


@app.route('/list')
# @oidc.require_login
def list():
    # oidc.user_getfield('email')
    ctx = gfal2.creat_context()
    baseurl = request.args.get("url", default="https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst/")

    try:
        directory = ctx.opendir(baseurl)
    except gfal2.GError as e:
        nread = 2000
        def generate():
            f = ctx.open(baseurl, 'r')
            print("opened", f)
            while True:
                r = f.read(nread)
                print("read", r)
                yield r
                if len(r) < nread:
                    break
        return generate() #, {"Content-Type": "text/csv"}

    urls = []

    while True:
        try:
            (dirent, fstat) = directory.readpp()
        except gfal2.GError as e:            
            continue

        if dirent is None or dirent.d_name is None or len(dirent.d_name) == 0:
            break

        urls.append(os.path.join(baseurl, dirent.d_name))


    return {'delivered_for': oidc.user_getfield('email'),
            'urls': urls}



# @app.route('/')
# def index():
#     return oidc.redirect_to_auth_server(None, request.values)

# @app.route('/custom_callback')
# # @oidc.custom_callback
# def callback(data):
#     return 'Hello. You submitted %s' % data