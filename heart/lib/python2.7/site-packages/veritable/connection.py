"""Tools for connecting to a Veritable API server.

See also: https://dev.priorknowledge.com/docs/client/python

"""

import logging
import requests
import json
import sys
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from .exceptions import VeritableError
from .utils import _url_has_scheme
from .version import __version__

USER_AGENT = "veritable-python " + __version__


def _fully_qualify_url(f):
    # ensures that urls passed to the HTTP methods are fully qualified
    def g(*args, **kwargs):
        url = args[1]
        if not _url_has_scheme(url):
            url = args[0].api_base_url.rstrip("/") + "/" + url.lstrip("/")
        return f(args[0], url, *args[2:], **kwargs)
    return g


def _get_response_data(r, debug_log=None):
    # routes HTTP errors, if any, and translates JSON response data.
    if r.status_code == requests.codes.ok:
        if debug_log is not None:
            debug_log(json.loads(r.content.decode('utf-8')))
        return json.loads(r.content.decode('utf-8'))
    else:
        _handle_http_error(r, debug_log)


def _handle_http_error(r, debug_log=None):
    # handles HTTP errors.
    try:
        content = json.loads(r.content.decode('utf-8'))
        if debug_log is not None:
            debug_log(content)
        message = content["message"]
        code = content["code"]
    except:
        r.raise_for_status()
    else:
        if r.status_code == requests.codes.not_found:
            raise VeritableError("HTTP Error {0} Not Found -- {1}: " \
            "{2}".format(r.status_code, code, message), status=r.status_code,
                code=code, message=message)
        if r.status_code == requests.codes.bad_request:
            raise VeritableError("HTTP Error {0} Bad Request -- {1}: " \
            "{2}".format(r.status_code, code, message), status=r.status_code,
                code=code, message=message)
        r.raise_for_status()


def _mgzip(buf):
    # gzip middleware.
    wbuf = BytesIO()
    zbuf = GzipFile(
            mode='wb',
            compresslevel=5,
            fileobj=wbuf
            )
    zbuf.write(buf.encode('utf-8'))
    zbuf.close()
    result = wbuf.getvalue()
    wbuf.close()
    return result


class Connection:

    """Wraps the raw HTTP connection to the Veritable server.

    Users should not interact directly with Connection objects. Use
    veritable.connect as an entry point instead.

    Methods:
    get -- wraps GET requests
    post -- wraps POST requests
    put -- wraps PUT requests
    delete -- wraps DELETE requests

    See also: https://dev.priorknowledge.com/docs/client/python

    """

    def __init__(self, api_key, api_base_url, ssl_verify=None,
                 enable_gzip=True, debug=False):
        """Initializes a connection to a Veritable server.

        Users should not invoke directly -- use veritable.connect as the
        entry point instead. Note that unlike veritable.connect, this
        method does not check whether an actual Veritable server is present
        at its target.

        Arguments:
        api_key -- the API key to use for access
        api_base_url -- the base url of the API
        ssl_verify -- controls whether SSL keys are verified. (default: None)
        enable_gzip -- controls whether requests to and from the API server are
            gzipped. (default: True)
        debug -- controls the production of debug messages. (default: False)

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        if api_key is None:
            raise VeritableError("Must provide an API key to instantiate a " \
            "connection to a Veritable server.")
        if api_base_url is None:
            raise VeritableError("Must provide a base URL to instantiate a " \
            "connection to a Veritable server.")
        self.api_key = api_key
        self.api_base_url = api_base_url.rstrip("/")
        self.auth = HTTPBasicAuth(self.api_key, "")
        self.ssl_verify = ssl_verify
        self.disable_gzip = not(enable_gzip)
        self.debug = debug
        self.session = self._create_session()
        if self.debug:
            self.logger = logging.getLogger(__name__)
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            self.logger.addHandler(ch)
            self.logger.setLevel(logging.DEBUG)

    def __str__(self):
        return "<veritable.Connection url='" + self.api_base_url + "'>"

    def __repr__(self):
        return self.__str__()

    def _create_session(self):
        # Creates a requests session
        headers = {'User-Agent': USER_AGENT}
        return requests.session(auth=self.auth, headers=headers)

    def _debug_log(self, x):
        """Debug logging."""
        if self.debug:
            self.logger.debug(x)

    @_fully_qualify_url
    def get(self, url, **kwargs):
        """Wraps GET requests.

        Users should not invoke this method directly.

        Arguments:
        url -- the URL of the resource to GET

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        kwargs.update({'headers': {}, 'prefetch': True})
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['headers']['Accept-Encoding'] = 'gzip'
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.get(url, **kwargs)
        return _get_response_data(r, self._debug_log)

    @_fully_qualify_url
    def post(self, url, data, **kwargs):
        """Wraps POST requests.

        Users should not invoke this method directly.

        Arguments:
        url -- the URL of the resource to POST to
        data -- the data to POST (as a Python object)
        
        See also: https://dev.priorknowledge.com/docs/client/python

        """
        kwargs.update({'headers': {'Content-Type': 'application/json'},
                  'prefetch': True})
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['data'] = _mgzip(json.dumps(data))
            kwargs['headers']['Content-Encoding'] = 'gzip'
        else:
            kwargs['data'] = json.dumps(data)
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.post(url, **kwargs)
        return _get_response_data(r, self._debug_log)

    @_fully_qualify_url
    def put(self, url, data, **kwargs):
        """Wraps PUT requests.

        Users should not invoke this method directly.

        Arguments:
        url -- the URL of the resource to PUT to
        data -- the data to PUT (as a Python object)
        
        See also: https://dev.priorknowledge.com/docs/client/python

        """
        kwargs.update({'headers': {'Content-Type': 'application/json'},
                  'prefetch': True})
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['data'] = _mgzip(json.dumps(data))
            kwargs['headers']['Content-Encoding'] = 'gzip'
        else:
            kwargs['data'] = json.dumps(data)
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.put(url, **kwargs)
        return _get_response_data(r, self._debug_log)

    @_fully_qualify_url
    def delete(self, url, **kwargs):
        """Wraps DELETE requests.

        Users should not invoke this method directly.

        Arguments:
        url -- the URL of the resource to DELETE

        See also: https://dev.priorknowledge.com/docs/client/python

        """
        kwargs.update({'headers': {}, 'prefetch': True})
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.delete(url, **kwargs)
        try:
            res = _get_response_data(r, self._debug_log)
        except VeritableError as e:
            if not e.status == requests.codes.not_found:
                raise e
            res = None
        except:
            raise
        return res
