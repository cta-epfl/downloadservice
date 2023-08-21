from datetime import date, datetime, timedelta
from functools import wraps
import OpenSSL
import os
import io
import re
from urllib.parse import urlparse
import requests
import secrets
import xml.etree.ElementTree as ET
import importlib.metadata

from flask import (
    Blueprint, Flask, Response, jsonify, make_response, redirect, request,
    session, stream_with_context, render_template
)

import logging

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

sentry_sdk.init(
    dsn='https://452458c2a6630292629364221bff0dee@o4505709665976320\
        .ingest.sentry.io/4505709666762752',
    integrations=[
        FlaskIntegration(),
    ],

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0,

    release='downloadservice:' + importlib.metadata.version("downloadservice"),
    environment=os.environ.get('SENTRY_ENVIRONMENT', 'dev'),
)

# from flask_oidc import OpenIDConnect
logger = logging.getLogger(__name__)


def urljoin_multipart(*args):
    """Join multiple parts of a URL together, ignoring empty parts."""
    logger.info('urljoin_multipart: %s', args)
    return '/'.join(
        [arg.strip('/')
         for arg in args if arg is not None and arg.strip('/') != '']
    )


try:
    from jupyterhub.services.auth import HubOAuth
    auth = HubOAuth(
        api_token=os.environ['JUPYTERHUB_API_TOKEN'], cache_max_age=60)
except Exception:
    logger.warning('Auth system not configured')
    auth = None

bp = Blueprint('downloadservice', __name__,
               template_folder='templates')

url_prefix = os.getenv('JUPYTERHUB_SERVICE_PREFIX', '').rstrip('/')

default_chunk_size = 10 * 1024 * 1024


def create_app():
    app = Flask(__name__)

    app.config['SECRET_KEY'] = os.environ.get(
        'FLASK_SECRET', secrets.token_bytes(32))
    app.secret_key = app.config['SECRET_KEY']

    app.config['OIDC_CLIENT_SECRETS'] = 'secrets.json'
    app.config['OIDC_COOKIE_SECURE'] = False
    app.config['OIDC_INTROSPECTION_AUTH_METHOD'] = 'client_secret_post'
    app.config['OIDC_TOKEN_TYPE_HINT'] = 'access_token'
    app.config['CTADS_CERTIFICATE_DIR'] = \
        os.environ.get('CTADS_CERTIFICATE_DIR', './certificate/')
    app.config['CTADS_CABUNDLE'] = \
        os.environ.get('CTADS_CABUNDLE', '/etc/cabundle.pem')
    app.config['CTADS_CLIENTCERT'] = \
        os.environ.get('CTADS_CLIENTCERT', '/tmp/x509up_u1000')
    app.config['CTADS_DISABLE_ALL_AUTH'] = \
        os.getenv('CTADS_DISABLE_ALL_AUTH', 'False') == 'True'
    app.config['CTADS_UPSTREAM_ENDPOINT'] = \
        os.getenv('CTADS_UPSTREAM_ENDPOINT',
                  'https://dcache.cta.cscs.ch:2880/')
    app.config['CTADS_UPSTREAM_BASEPATH'] = \
        os.getenv('CTADS_UPSTREAM_BASEPATH', 'pnfs/cta.cscs.ch/')
    app.config['CTADS_UPSTREAM_BASEPATH'] = \
        os.getenv('CTADS_UPSTREAM_BASEPATH', 'pnfs/cta.cscs.ch/')
    app.config['CTADS_UPSTREAM_BASEFOLDER'] = \
        os.getenv('CTADS_UPSTREAM_BASEFOLDER', 'lst')

    # Check certificate folder
    os.makedirs(app.config['CTADS_CERTIFICATE_DIR'], exist_ok=True)
    
    # TODO: Check certificates and their validity

    return app


app = create_app()


def authenticated(f):
    # TODO: here do a permission check;
    # in the future, the check will be done with rucio maybe
    """Decorator for authenticating with the Hub via OAuth"""

    print('authenticated check:', app.config)

    @wraps(f)
    def decorated(*args, **kwargs):
        if app.config['CTADS_DISABLE_ALL_AUTH']:
            return f({'name':'anonymous', 'admin': True}, *args, **kwargs)
        else:
            if auth is None:
                return 'Unable to use jupyterhub to verify access to this\
                    service. At this time, the downloadservice uses jupyterhub\
                    to control access to protected resources', 500

            header = request.headers.get('Authorization')
            if header and header.startswith('Bearer '):
                header_token = header.split()[1]
            else:
                header_token = None

            token = session.get('token') \
                or request.args.get('token') \
                or header_token

            if token:
                user = auth.user_for_token(token)
                if user is not None and not auth.check_scopes(
                        'access:services!service=downloadservice', user):
                    user = None
            else:
                user = None

            if user:
                return f(user, *args, **kwargs)
            else:
                # redirect to login url on failed auth
                state = auth.generate_state(next_url=request.path)
                response = make_response(
                    redirect(auth.login_url + '&state=%s' % state)
                )
                response.set_cookie(auth.state_cookie_name, state)
                return response

    return decorated


# @app.before_request
# def clear_trailing():
#     rp = request.path
#     if rp != '/' and rp.endswith('/'):
#         logger.warning('redirect %s', rp[:-1])
#         return redirect(rp[:-1])

@app.route(url_prefix + '/')
@authenticated
def login(user):
    token = session.get('token') or request.args.get('token')
    return render_template('index.html', user=user, token=token)


def certificate_validity(certificate):
    x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, certificate)
    asn1_time=x509.get_notAfter()
    return datetime.strptime(asn1_time.decode(), '%Y%m%d%H%M%S%fZ')


@app.route(url_prefix + '/upload-cert', methods=['POST'])
@authenticated
def upload_cert(user):
    filename = user_to_path_fragment(user) + ".crt"
    certificate_file = app.config['CTADS_CERTIFICATE_DIR'] + filename

    certificate = request.json.get('certificate')
    try:
        if certificate and certificate_validity(certificate).date() > \
            (date.today()+timedelta(days=1)):
            return 'certificate validity too large', 400
    except:
        return 'invalid certificate', 400

    with open(certificate_file, 'w') as f:
        f.write(certificate)

    return 'Certificate stored', 200

@app.route(url_prefix + '/upload-main-cert', methods=['POST'])
@authenticated
def upload_main_cert(user):
    if not isinstance(user, dict) or user['admin'] != True:
        return 'access denied', 401

    data = request.json
    certificate = data.get('certificate', None)
    cabundle = data.get('cabundle', None)

    if certificate is None and cabundle is None:
        return 'missing certificate of cabundle', 400
    try:
        if certificate and certificate_validity(certificate).date() > \
            (date.today()+timedelta(days=1)):
            return 'certificate validity too large', 400
    except:
        return 'invalid certificate', 400
    
    updated = set()
    if certificate is not None:
        with open(app.config['CTADS_CLIENTCERT'], 'w') as f:
            f.write(certificate)
            updated.add('Certificate')
    if cabundle is not None:
        with open(app.config['CTADS_CABUNDLE'], 'w') as f:
            f.write(cabundle)
            updated.add('CABundle')
    
    return ' and '.join(updated) + ' stored', 200

def get_upstream_session(user = None):
    session = requests.Session()

    cert = app.config['CTADS_CLIENTCERT']
    own_certificate = False
    if user is not None:
        filename = user_to_path_fragment(user) + ".crt"
        own_certificate_file = app.config['CTADS_CERTIFICATE_DIR'] + filename
        
        if os.path.isfile(own_certificate_file):
            cert = own_certificate_file

    with open(own_certificate_file) as f:
        certificate = f.read()
        if certificate_validity(certificate) < datetime.now():
            if own_certificate:
                raise f'Your configured certificate is invalid, please refresh it.'
            else: 
                logger.exception('outdated main certificate')
                raise f'Service certificate invalid please contact us.'

        session.verify = app.config['CTADS_CABUNDLE']
        session.cert = cert

    return session


@app.route(url_prefix + '/health')
def health():
    url = app.config['CTADS_UPSTREAM_ENDPOINT'] + \
        app.config['CTADS_UPSTREAM_BASEPATH'] + \
        app.config['CTADS_UPSTREAM_BASEFOLDER']

    upstream_session = get_upstream_session()
    try:
        r = upstream_session.request('PROPFIND', url, headers={'Depth': '1'},
                                     timeout=10)
        if r.status_code in [200, 207]:
            return 'OK', 200
        else:
            logger.error('service is unhealthy: %s', r.content.decode())
            return 'Unhealthy!', 500
    except requests.exceptions.ReadTimeout as e:
        logger.error('service is unhealthy: %s', e)
        return 'Unhealthy!', 500


# @oidc.require_login
# @oidc.accept_token(require_token=True)

@app.route(url_prefix + '/list', methods=['GET', 'POST'],
           defaults={'path': ''})
@app.route(url_prefix + '/list/<path:path>', methods=['GET', 'POST'])
@authenticated
def list(user, path):
    # host = request.headers['Host']

    upstream_url = urljoin_multipart(
        app.config['CTADS_UPSTREAM_ENDPOINT'],
        app.config['CTADS_UPSTREAM_BASEPATH'],
        (path or '')
    )

    upstream_session = get_upstream_session(user)

    r = upstream_session.request(
        'PROPFIND', upstream_url, headers={'Depth': '1'})

    if r.status_code not in [200, 207]:
        return f'Error: {r.status_code} {r.content.decode()}', r.status_code

    logger.debug('response: %s', r.content.decode())

    try:
        tree = ET.parse(io.BytesIO(r.content))
        root = tree.getroot()
    except ET.ParseError as e:
        logger.error('Error parsing XML %s in %s', e, r.content)
        raise

    logger.info('xml: %s', root)

    entries = []

    keymap = dict([
        ('{DAV:}href', 'href'),
        ('{DAV:}getcontentlength', 'size'),
        ('{DAV:}getlastmodified', 'mtime')
    ])

    for i in root.iter('{DAV:}response'):
        logger.debug('i: %s', i)

        entry = {}
        entries.append(entry)

        for j in i.iter():
            logger.debug('> j: %s %s', j.tag, j.text)
            if j.tag in keymap:
                entry[keymap[j.tag]] = j.text

        up = urlparse(request.url)
        entry['href'] = re.sub(
            '^/*' + app.config['CTADS_UPSTREAM_BASEPATH'], '', entry['href'])

        entry['url'] = '/'.join([
            up.scheme + ':/', up.netloc,
            re.sub(path, '', up.path), entry['href']
        ])

        if entry['href'].endswith('/'):
            entry['type'] = 'directory'
        else:
            entry['type'] = 'file'

    return jsonify(entries)
    # TODO print useful logs for loki


@app.route(url_prefix + '/fetch', methods=['GET', 'POST'],
           defaults={'path': ''})
@app.route(url_prefix + '/fetch/<path:path>', methods=['GET', 'POST'])
@authenticated
def fetch(user, path):
    if '..' in path:
        return "Error: path cannot contain '..'", 400

    url = urljoin_multipart(app.config['CTADS_UPSTREAM_ENDPOINT'],
                            app.config['CTADS_UPSTREAM_BASEPATH'],
                            (path or ''))

    chunk_size = request.args.get('chunk_size', default_chunk_size, type=int)

    logger.info('fetching upstream url %s', url)

    upstream_session = get_upstream_session(user)

    def generate():
        with upstream_session.get(url, stream=True) as f:
            logger.debug('got response headers: %s', f.headers)
            # headers['Content-Type'] = f.headers['content-type']
            logger.info('opened %s', f)
            for r in f.iter_content(chunk_size=chunk_size):
                yield r

    return Response(stream_with_context(generate()))
    # TODO print useful logs for loki


def user_to_path_fragment(user):
    if isinstance(user, dict):
        user = user['name']

    return re.sub('[^0-1a-z]', '_', user.lower())


@app.route(url_prefix + '/upload', methods=['POST'], defaults={'path': None})
@app.route(url_prefix + '/upload/<path:path>', methods=['POST'])
@authenticated
def upload(user, path):

    # TODO: Not secure for production
    if '..' in path:
        return "Error: path cannot contain '..'", 400

    upload_base_path = urljoin_multipart(
        app.config['CTADS_UPSTREAM_BASEFOLDER'],
        'users',
        user_to_path_fragment(user))
    upload_path = urljoin_multipart(upload_base_path, path)

    baseurl = urljoin_multipart(
        app.config['CTADS_UPSTREAM_ENDPOINT'],
        app.config['CTADS_UPSTREAM_BASEPATH'],
        upload_base_path
    )

    url = urljoin_multipart(baseurl, path)

    chunk_size = request.args.get('chunk_size', default_chunk_size, type=int)

    logger.info('uploading to path %s', path)
    logger.info('uploading to base upstream url %s', baseurl)
    logger.info('uploading to upstream url %s', url)
    logger.info('uploading chunk size %s', chunk_size)

    upstream_session = get_upstream_session(user)

    r = upstream_session.request('MKCOL', baseurl)

    stats = dict(total_written=0)

    def generate(stats):
        while r := request.stream.read(chunk_size):
            logger.info('read %s Mb total %s Mb',
                        len(r)/1024**2, stats['total_written']/1024**2)
            stats['total_written'] += len(r)
            yield r

    r = upstream_session.put(url, data=generate(stats))

    logger.info('%s %s %s', url, r, r.text)

    if r.status_code not in [200, 201]:
        return f'Error: {r.status_code} {r.content.decode()}', r.status_code
    else:
        return {
            'status': 'uploaded',
            'path': upload_path,
            'total_written': stats['total_written']
        }

    # TODO: first simple and safe mechanism would be to let users upload only
    # to their own specialized directory with hashed name

    # return Response(stream_with_context(generate())), headers
    # TODO print useful logs for loki


@app.route(url_prefix + '/oauth_callback')
def oauth_callback():
    code = request.args.get('code', None)
    if code is None:
        return 'Error: oauth callback code', 403

    # validate state field
    arg_state = request.args.get('state', None)
    cookie_state = request.cookies.get(auth.state_cookie_name)
    if arg_state is None or arg_state != cookie_state:
        # state doesn't match
        return 'Error: oauth callback invalid state', 403

    token = auth.token_for_code(code)
    # store token in session cookie
    session['token'] = token
    next_url = auth.get_next_url(cookie_state) or url_prefix
    response = make_response(redirect(next_url))
    return response


webdav_methods = ['GET', 'HEAD',  'MKCOL', 'OPTIONS',
                  'PROPFIND', 'PROPPATCH', 'PUT', 'TRACE',
                  # 'LOCK', 'UNLOCK', 'POST', 'DELETE', 'MOVE',
                  ]


@app.route(url_prefix + '/webdav', defaults={'path': ''},
           methods=webdav_methods)
@app.route(url_prefix + '/webdav/<path:path>', methods=webdav_methods)
@authenticated
def webdav(user, path):
    API_HOST = urljoin_multipart(
        app.config['CTADS_UPSTREAM_ENDPOINT'],
        app.config['CTADS_UPSTREAM_BASEPATH'],
    )

    if request.method not in ['GET', 'HEAD', 'OPTIONS', 'PROPFIND', 'TRACE']:
        required_path_prefix = urljoin_multipart(
            app.config['CTADS_UPSTREAM_BASEFOLDER'],
            'users',
            user_to_path_fragment(user)) + '/'

        if not path.startswith(required_path_prefix):
            return 'Access denied', \
                f'403 Missing rights to write in : {path}, ' + \
                'you are only allowed to write in ' + \
                required_path_prefix

    # Exclude all "hop-by-hop headers" defined by RFC 2616
    # section 13.5.1 ref. https://www.rfc-editor.org/rfc/rfc2616#section-13.5.1
    excluded_headers = ['content-encoding', 'content-length',
                        'transfer-encoding', 'connection', 'keep-alive',
                        'proxy-authenticate', 'proxy-authorization', 'te',
                        'trailers', 'upgrade']

    def request_datastream():
        while (buf := request.stream.read(default_chunk_size)) != b'':
            yield buf

    upstream_session = get_upstream_session(user)
    res = upstream_session.request(
        method=request.method,
        url=urljoin_multipart(API_HOST, path),
        # exclude 'host' and 'authorization' header
        headers={k: v for k, v in request.headers
                 if k.lower() not in ['host', 'authorization'] and
                 k.lower() not in excluded_headers},
        data=request_datastream(),
        cookies=request.cookies,
        allow_redirects=False,
    )

    headers = [
        (k, v) for k, v in res.raw.headers.items()
        if k.lower() not in excluded_headers
    ]

    endpoint_prefix = url_prefix+'/webdav'

    def is_prop_method():
        return request.method in ['PROPFIND', 'PROPPATCH']

    def prop_content():
        return res.content\
            .replace(
                (':href>/' +
                 app.config['CTADS_UPSTREAM_BASEFOLDER'] + '/').encode(),
                (':href>'+endpoint_prefix+'/' +
                 app.config['CTADS_UPSTREAM_BASEFOLDER']+'/').encode()
            )

    return Response(
        prop_content() if is_prop_method else res.content,
        res.status_code,
        headers
    )
