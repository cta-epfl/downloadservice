from functools import wraps
import os
import io
from urllib.parse import urljoin, urlparse
import requests
from pathlib import Path

from bs4 import BeautifulSoup
import secrets
import xml.etree.ElementTree as ET

from flask import Blueprint, Flask, Response, make_response, redirect, request, session, stream_with_context, url_for
from flask import redirect, request

# from flask_oidc import OpenIDConnect

import logging

logger = logging.getLogger(__name__)

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

url_prefix = os.getenv("JUPYTERHUB_SERVICE_PREFIX", "").rstrip("/")

def create_app():
    app = Flask(__name__)


    app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET', secrets.token_bytes(32))
    app.secret_key = app.config['SECRET_KEY']

    app.config['OIDC_CLIENT_SECRETS'] = 'secrets.json'
    app.config['OIDC_COOKIE_SECURE'] = False
    app.config['OIDC_INTROSPECTION_AUTH_METHOD'] = 'client_secret_post'
    app.config['OIDC_TOKEN_TYPE_HINT'] = 'access_token'
    app.config['CTADS_CABUNDLE'] = os.environ.get('CTADS_CABUNDLE', '/etc/cabundle.pem')
    app.config['CTADS_CLIENTCERT'] = os.environ.get('CTADS_CLIENTCERT', '/tmp/x509up_u1000')
    app.config['CTADS_DISABLE_ALL_AUTH'] = False
    app.config['CTADS_UPSTREAM_ROOT'] = "https://dcache.cta.cscs.ch:2880/"    

    return app


app = create_app()


def authenticated(f):
    """Decorator for authenticating with the Hub via OAuth"""

    print("authenticated check:", app.config)

    @wraps(f)
    def decorated(*args, **kwargs):
        if app.config['CTADS_DISABLE_ALL_AUTH']:
            return f("anonymous", *args, **kwargs)
        else:
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


@app.before_request
def clear_trailing():
    rp = request.path
    if rp != '/' and rp.endswith('/'):
        print("redirect", rp[:-1])
        return redirect(rp[:-1])
    

@app.route(url_prefix + "/")
def login():
    token = session.get("token") or request.args.get('token')

    if token:
        user = auth.user_for_token(token)
    else:
        user = None

    return f"Welcome {user} access token {token}"



def get_session():
    session = requests.Session()
    session.verify = app.config['CTADS_CABUNDLE']
    session.cert = app.config['CTADS_CLIENTCERT']

    return session


@app.route(url_prefix + "/health")
def health():    
    return f"Welcome to the health page"

    

@app.route(url_prefix + '/fetch', methods=["GET", "POST"], defaults={'basepath': None})
@app.route(url_prefix + '/fetch/<path:basepath>', methods=["GET", "POST"])
@authenticated
# @oidc.require_login
# @oidc.accept_token(require_token=True)
def list(user, basepath):
    host = request.headers['Host']
    
    baseurl = request.args.get("url", default="https://dcache.cta.cscs.ch:2880/" + (basepath or ""))
    # TODO: here do a permission check; in the future, the check will be done with rucio maybe

    session = get_session()

    r = session.request('PROPFIND', baseurl, headers={'Depth': '1'})

    logger.debug("request: %s", r.request.headers)
    logger.debug("response: %s", r.content.decode())
            
    try:
        tree = ET.parse(io.BytesIO(r.content))
        root = tree.getroot()
    except ET.ParseError as e:
        logger.error("Error parsing XML %s in %s", e, r.content)
        raise

    logger.info("xml: %s", root)

    for i in root.iter('{DAV:}response'):
        logger.info("i: %s", i)
        for j in i.iter():
            logger.info("j: %s", j.text)

    # curl -i -X PROPFIND http://example.com/webdav/ --upload-file - -H "Depth: 1"
    # urls = []
    # for url in iter_dirlist(basepath, content):
    #     urls.append("http://" + host + "/fetch/" + url)

    return urls
    # TODO print useful logs for loki


@app.route(url_prefix + '/fetch', methods=["GET", "POST"], defaults={'basepath': None})
@app.route(url_prefix + '/fetch/<path:subpath>', methods=["GET", "POST"])
@authenticated
# @oidc.require_login
# @oidc.accept_token(require_token=True)
def fetch(user, subpath):
    url = request.args.get("url", default=app.config['CTADS_UPSTREAM_ROOT'] + (subpath or ""))

    session = get_session()
    
    headers = {} 
    def generate():
        with session.get(url, stream=True) as f:
            logger.debug("got response headers: %s", f.headers)
            # headers['Content-Type'] = f.headers['content-type']
            logger.info("opened %s", f)
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
    
