from functools import wraps
import os
from urllib.parse import urljoin, urlparse
from flask import Blueprint, Flask, Response, make_response, redirect, request, session, stream_with_context, url_for
# from flask_oidc import OpenIDConnect
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import secrets

try:
    import gfal2
except ImportError:
    gfal2 = None


try:
    from jupyterhub.services.auth import HubOAuth
    auth = HubOAuth(api_token=os.environ['JUPYTERHUB_API_TOKEN'], cache_max_age=60)
except Exception as e:
    auth = None


bp = Blueprint('downloadservice', __name__,
                template_folder='templates')

app = Flask(__name__)

url_prefix = os.getenv("JUPYTERHUB_SERVICE_PREFIX", "").rstrip("/")

app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET', secrets.token_bytes(32))
app.secret_key = app.config['SECRET_KEY']

app.config['OIDC_CLIENT_SECRETS'] = 'secrets.json'
app.config['OIDC_COOKIE_SECURE'] = False
app.config['OIDC_INTROSPECTION_AUTH_METHOD'] = 'client_secret_post'
app.config['OIDC_TOKEN_TYPE_HINT'] = 'access_token'
app.config['CTADS_CABUNDLE'] = os.environ.get('CTADS_CABUNDLE', '/etc/cabundle.pem')
app.config['CTADS_CLIENTCERT'] = os.environ.get('CTADS_CLIENTCERT', '/tmp/x509up_u1000')


disable_all_auth = os.environ.get('CTADS_DISABLE_ALL_AUTH', False)

def authenticated(f):
    """Decorator for authenticating with the Hub via OAuth"""

    if disable_all_auth:
        return f
    else:
        @wraps(f)
        def decorated(*args, **kwargs):
            token = session.get("token") or request.args.get('token')

            if token:
                user = auth.user_for_token(token)
                if user is not None:
                    if user['name'] not in ['volodymyr.savchenko@epfl.ch', 'andrii.neronov@epfl.ch', 'pavlo.kashko@epfl.ch']:
                        user = None
            else:
                user = None


            if user:
                return f(user, *args, **kwargs)
            else:
                # redirect to login url on failed auth
                state = auth.generate_state(next_url=request.path)
                response = make_response(redirect(auth.login_url + '&state=%s' % state))
                response.set_cookie(auth.state_cookie_name, state)
                return response

        return decorated


@app.route(url_prefix)
@app.route(url_prefix + "/")
def login():
    token = session.get("token") or request.args.get('token')

    if token:
        user = auth.user_for_token(token)
    else:
        user = None

    return f"Welcome {user} access token {token}"


def iter_dirlist(base, page):
    soup = BeautifulSoup(page, 'html.parser')
    # return [base + node.get('href') for node in soup.find_all('a') if node.get('href').startswith('../../..')]
    return [urljoin(base, urlparse(node.get('href')).path) for node in soup.find_all('a') if node.get('href').startswith('..')]
    

@app.route(url_prefix + '/fetch/', methods=["GET", "POST"], defaults={'basepath': None})
@app.route(url_prefix + '/fetch/<path:basepath>', methods=["GET", "POST"])
@authenticated
# @oidc.require_login
# @oidc.accept_token(require_token=True)
def list(user, basepath):
    host = request.headers['Host']
    
    baseurl = request.args.get("url", default="https://dcache.cta.cscs.ch:2880/" + (basepath or ""))
    # TODO: here do a permission check; in the future, the check will be done with rucio maybe

    session = requests.Session()
    session.verify = app.config['CTADS_CABUNDLE']
    session.cert = app.config['CTADS_CLIENTCERT'] 

    content = session.get(baseurl).content

    # print("content", content)

    # TODO: better
    if b'Ecole polytechnique federale de Lausanne, EPFL/OU=SCITAS/CN=Volodymyr Savchenko' in content:
        urls = []
        for url in iter_dirlist(basepath, content):
            urls.append("http://" + host + "/fetch/" + url)

        return {'urls' :urls, 'user': user}
    else:
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


@app.route(url_prefix + '/oauth_callback')
def oauth_callback():
    code = request.args.get('code', None)
    if code is None:
        return 403

    # validate state field
    arg_state = request.args.get('state', None)
    cookie_state = request.cookies.get(auth.state_cookie_name)
    if arg_state is None or arg_state != cookie_state:
        # state doesn't match
        return 403

    token = auth.token_for_code(code)
    # store token in session cookie
    session["token"] = token
    next_url = auth.get_next_url(cookie_state) or url_prefix
    response = make_response(redirect(next_url))
    return response


def main():
    app.run(host='0.0.0.0', port=5000)
    