import os
from urllib.parse import urljoin, urlparse
from flask import Flask, Response, request, stream_with_context, url_for
from flask_oidc import OpenIDConnect
import requests
from bs4 import BeautifulSoup
from pathlib import Path


try:
    import gfal2
except ImportError:
    gfal2 = None


app = Flask(__name__)

app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET')

app.config['OIDC_CLIENT_SECRETS'] = '/home/savchenk/cscs/sdc/downloadservice/secrets.json'
app.config['OIDC_COOKIE_SECURE'] = False
app.config['OIDC_INTROSPECTION_AUTH_METHOD'] = 'client_secret_post'
app.config['OIDC_TOKEN_TYPE_HINT'] = 'access_token'
app.config['CTADS_CABUNDLE'] = os.environ.get('CTADS_CABUNDLE')
app.config['CTADS_CLIENTCERT'] = os.environ.get('CTADS_CLIENTCERT')

oidc = OpenIDConnect(app)

def index():
    if oidc.user_loggedin:
        return 'Welcome %s' % oidc.user_getfield('email')
    else:
        return 'Not logged in, <a href="login">login</a>'

@app.route('/login')
@oidc.require_login
def login():
    return f"Welcome {oidc.user_getfield('email')} access token {oidc.get_access_token()} cookie id token {oidc.get_cookie_id_token()}"


@app.route('/list-gfal', methods=["GET", "POST"])
@oidc.accept_token(require_token=True)
# @oidc.accept_token()
# @oidc.require_login
def list_gfal():
    # oidc.user_getfield('email')

    print(dict(request.headers))

    host = request.headers['Host']

    ctx = gfal2.creat_context()
    baseurl = request.args.get("url", default="https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst/")

    try:
        directory = ctx.opendir(baseurl)
    except gfal2.GError as e:
        nread = 1024*1024
        def generate():
            f = ctx.open(baseurl, 'r')
            print("opened", f)
            while True:
                r = f.read(nread)
                print("read", len(r))
                yield r
                if len(r) < nread:
                    print("done read", len(r))
                    break
        return Response(stream_with_context(generate())) #, {"Content-Type": "text/csv"}

    urls = []

    while True:
        try:
            (dirent, fstat) = directory.readpp()
        except gfal2.GError as e:            
            continue

        if dirent is None or dirent.d_name is None or len(dirent.d_name) == 0:
            break

        url = os.path.join(baseurl, dirent.d_name)
        urls.append((url, 
                     "http://" + host +  url_for("list") + "?url=" + url))


    return {'delivered_for': oidc.user_getfield('email') if oidc.user_loggedin else None,
            'urls': urls}



def iter_dirlist(base, page):
    soup = BeautifulSoup(page, 'html.parser')
    # return [base + node.get('href') for node in soup.find_all('a') if node.get('href').startswith('../../..')]
    return [urljoin(base, urlparse(node.get('href')).path) for node in soup.find_all('a') if node.get('href').startswith('..')]
    

@app.route('/fetch/', methods=["GET", "POST"], defaults={'basepath': None})
@app.route('/fetch/<path:basepath>', methods=["GET", "POST"])
# @oidc.require_login
# @oidc.accept_token(require_token=True)
def list(basepath):
    host = request.headers['Host']
    
    baseurl = request.args.get("url", default="https://dcache.cta.cscs.ch:2880/" + (basepath or ""))
    # TODO: here do a permission check; in the future, the check will be done with rucio maybe

    session = requests.Session()
    session.verify = "/home/savchenk/cabundle.pem"
    session.cert = "/tmp/x509up_u1000"

    content = session.get(baseurl).content

    # print("content", content)

    # TODO: better
    if b'Ecole polytechnique federale de Lausanne, EPFL/OU=SCITAS/CN=Volodymyr Savchenko' in content:
        urls = []
        for url in iter_dirlist(basepath, content):
            urls.append("http://" + host + "/fetch/" + url)

        return urls
    else:
        nread = 1024*1024
        headers = {} 
        def generate():
            with session.get(baseurl, stream=True) as f:
                print(f.headers)
                # headers['Content-Type'] = f.headers['content-type']
                print("opened", f)
                for r in f.iter_content(chunk_size=1024*1024):                    
                    yield r

        return Response(stream_with_context(generate())), headers
    # TODO print useful logs for loki

    