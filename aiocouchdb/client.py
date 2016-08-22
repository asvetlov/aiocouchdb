# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2016 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiohttp
import aiohttp.log
import io
import json
import types
import urllib.parse

from .authn import AuthProvider, NoAuthProvider
from .errors import maybe_raise_error
from .hdrs import (
    ACCEPT,
    ACCEPT_ENCODING,
    CONTENT_LENGTH,
    CONTENT_TYPE,
    LOCATION,
    METH_GET,
    SEC_WEBSOCKET_KEY1,
    TRANSFER_ENCODING,
    URI,
)
from .multipart import MultipartWriter


__all__ = (
    'HttpRequest',
    'HttpResponse',
    'HttpSession',
    'Resource',
    'extract_credentials',
    'urljoin'
)


# FIXME: workaround of decompressing empty payload.
# https://github.com/KeepSafe/aiohttp/pull/154
class HttpPayloadParser(aiohttp.HttpPayloadParser):

    def __call__(self, out, buf):
        # payload params
        length = self.message.headers.get(CONTENT_LENGTH, self.length)
        if SEC_WEBSOCKET_KEY1 in self.message.headers:
            length = 8

        # payload decompression wrapper
        if self.compression and self.message.compression:
            if self.response_with_body:  # the fix
                out = aiohttp.protocol.DeflateBuffer(out,
                                                     self.message.compression)

        # payload parser
        if not self.response_with_body:
            # don't parse payload if it's not expected to be received
            pass

        elif 'chunked' in self.message.headers.get(TRANSFER_ENCODING, ''):
            yield from self.parse_chunked_payload(out, buf)

        elif length is not None:
            try:
                length = int(length)
            except ValueError:
                raise aiohttp.errors.InvalidHeader(CONTENT_LENGTH) from None

            if length < 0:
                raise aiohttp.errors.InvalidHeader(CONTENT_LENGTH)
            elif length > 0:
                yield from self.parse_length_payload(out, buf, length)
        else:
            if self.readall and getattr(self.message, 'code', 0) != 204:
                yield from self.parse_eof_payload(out, buf)
            elif getattr(self.message, 'method', None) in ('PUT', 'POST'):
                aiohttp.log.internal_logger.warning(  # pragma: no cover
                    'Content-Length or Transfer-Encoding header is required')

        out.feed_eof()

aiohttp.HttpPayloadParser = HttpPayloadParser


@asyncio.coroutine
def request(method, url, *,
            allow_redirects=True,
            compress=None,
            connector=None,
            cookies=None,
            data=None,
            encoding='utf-8',
            expect100=False,
            headers=None,
            loop=None,
            max_redirects=10,
            params=None,
            read_until_eof=True,
            request_class=None,
            response_class=None,
            version=aiohttp.HttpVersion11):

    redirects = 0
    method = method.upper()
    connector = connector or aiohttp.TCPConnector(force_close=True, loop=loop)
    request_class = request_class or HttpRequest
    response_class = response_class or HttpResponse

    while True:
        req = request_class(method, url,
                            compress=compress,
                            cookies=cookies,
                            data=data,
                            encoding=encoding,
                            expect100=expect100,
                            headers=headers,
                            loop=loop,
                            params=params,
                            response_class=response_class,
                            version=version)

        conn = yield from connector.connect(req)
        try:
            resp = req.send(conn.writer, conn.reader)
            try:
                yield from resp.start(conn, read_until_eof)
            except:
                resp.close()
                conn.close()
                raise
        except (aiohttp.HttpProcessingError,
                aiohttp.ServerDisconnectedError) as exc:
            raise aiohttp.ClientResponseError() from exc
        except OSError as exc:
            raise aiohttp.ClientOSError() from exc

        # redirects
        if allow_redirects and resp.status in {301, 302, 303, 307}:
            redirects += 1
            if max_redirects and redirects >= max_redirects:
                resp.close(force=True)
                break

            # For 301 and 302, mimic IE behaviour, now changed in RFC.
            # Details: https://github.com/kennethreitz/requests/pull/269
            if resp.status != 307:
                method = METH_GET
                data = None

            r_url = (resp.headers.get(LOCATION) or
                     resp.headers.get(URI))

            scheme = urllib.parse.urlsplit(r_url)[0]
            if scheme not in ('http', 'https', ''):
                resp.close(force=True)
                raise ValueError('Can redirect only to http or https')
            elif not scheme:
                r_url = urllib.parse.urljoin(url, r_url)

            url = urllib.parse.urldefrag(r_url)[0]
            if url:
                yield from asyncio.async(resp.release(), loop=loop)
                continue

        break

    return resp


class HttpRequest(aiohttp.client_reqrep.ClientRequest):
    """:class:`aiohttp.client.ClientRequest` class with CouchDB specifics."""

    #: Default HTTP request headers.
    DEFAULT_HEADERS = {
        ACCEPT: 'application/json',
        ACCEPT_ENCODING: 'gzip, deflate',
        CONTENT_TYPE: 'application/json'
    }
    CHUNK_SIZE = 8192

    def update_body_from_data(self, data, skip_auto_headers):
        """Encodes ``data`` as JSON if `Content-Type`
        is :mimetype:`application/json`."""
        if data is None:
            return
        if self.headers.get(CONTENT_TYPE) == 'application/json':
            non_json_types = (types.GeneratorType, io.IOBase, MultipartWriter)
            if not (isinstance(data, non_json_types)):
                data = json.dumps(data)

        rv = super().update_body_from_data(data, None)
        if isinstance(data, MultipartWriter) and CONTENT_LENGTH in self.headers:
            self.chunked = False
        return rv

    def update_path(self, params):
        if isinstance(params, dict):
            params = params.copy()
            for key, value in params.items():
                if value is True:
                    params[key] = 'true'
                elif value is False:
                    params[key] = 'false'
        return super().update_path(params)


class HttpResponse(aiohttp.client_reqrep.ClientResponse):
    """Deviation from :class:`aiohttp.client.ClientResponse` class for
    CouchDB specifics. Prefers :class:`~aiohttp.streams.FlowControlChunksQueue`
    flow control which fits the best to handle chunked responses.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(force=True if exc_type else False)

    def maybe_raise_error(self):
        """Raises an :exc:`HttpErrorException` if response status code is
        greater or equal `400`."""
        return maybe_raise_error(self)

    @asyncio.coroutine
    def read(self):
        """Read response payload."""
        if self._content is None:
            data = bytearray()
            try:
                while not self.content.at_eof():
                    data.extend((yield from self.content.read()))
            except:
                self.close(True)
                raise
            else:
                self.close()

            self._content = data

        return self._content

    @asyncio.coroutine
    def json(self, *, encoding='utf-8', loads=json.loads):
        """Reads and decodes JSON response."""
        if self._content is None:
            yield from self.read()

        if not self._content.strip():
            return None

        return loads(self._content.decode(encoding))


class HttpSession(object):
    """HTTP client session which holds default :class:`Authentication Provider
    <aiocouchdb.authn.AuthProvider>` instance (if any) and :class:`TCP Connector
    <aiohttp.connector.TCPConnector>`."""

    request_class = HttpRequest
    response_class = HttpResponse

    def __init__(self, *, auth=None, connector=None, loop=None):
        self._auth = auth or NoAuthProvider()

        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        if connector is None:
            self.connector = aiohttp.TCPConnector(force_close=False, loop=loop)
        else:
            self.connector = connector

    @property
    def auth(self):
        """Default :class:`~aiocouchdb.authn.AuthProvider` instance to apply
        on the requests. By default, :class:`~aiocouchdb.authn.NoAuthProvider`
        is used assuming that actual provider will get passed with `auth` query
        parameter on :meth:`request` call, but user may freely override it
        with your own.

        .. warning::

            Try avoid to use :class:`~aiocouchdb.authn.CookieAuthProvider` here
            since currently :class:`HttpSession` cannot renew the cookie in case
            it get expired.

        """
        return self._auth

    @auth.setter
    def auth(self, value):
        if value is None:
            self._auth = NoAuthProvider()
        else:
            assert isinstance(value, AuthProvider)
            self._auth = value

    def request(self, method, url, *,
                allow_redirects=True,
                auth=None,
                compress=None,
                cookies=None,
                data=None,
                encoding='utf-8',
                expect100=False,
                headers=None,
                loop=None,
                max_redirects=10,
                params=None,
                read_until_eof=True,
                request_class=None,
                response_class=None,
                version=aiohttp.HttpVersion11):
        """Makes a HTTP request with applying authentication routines.

        :param str method: Request method
        :param str url: Requested URL

        :param bool allow_redirects: Whenever to follow redirects
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param str compress: `Content-Encoding` method
        :param cookies: Additional :class:`HTTP cookies
                        <http.cookies.SimpleCookie>`
        :param data: Payload data
        :param str encoding: Payload encoding in case if unicode string had
                             passed
        :param bool expect100: Whenever HTTP 100 response is expected
        :param dict headers: Request headers
        :param loop: AsyncIO event loop instance
        :param int max_redirects: Maximum redirect hops to pass before give up
        :param dict params: Request query parameters
        :param bool read_until_eof: Whenever need to read
        :param request_class: HTTP request maker class
        :param response_class: HTTP response processor class
        :param str version: HTTP protocol version

        :returns: :class:`aiocouchdb.client.HttpResponse` instance
        """

        auth = auth or self._auth
        headers = headers or {}
        params = params or {}
        request_class = request_class or self.request_class
        response_class = response_class or self.response_class

        return auth.wrap(request)(method, url,
                                  allow_redirects=allow_redirects,
                                  compress=compress,
                                  connector=self.connector,
                                  cookies=cookies,
                                  data=data,
                                  encoding=encoding,
                                  expect100=expect100,
                                  headers=headers,
                                  loop=loop or self._loop,
                                  max_redirects=max_redirects,
                                  params=params,
                                  read_until_eof=read_until_eof,
                                  request_class=request_class,
                                  response_class=response_class,
                                  version=version)


class Resource(object):
    """HTTP resource representation. Accepts full ``url`` as argument.

    >>> res = Resource('http://localhost:5984')
    >>> res  # doctest: +ELLIPSIS
    <aiocouchdb.client.Resource(http://localhost:5984) object at ...>

    Able to construct new Resource instance by assemble base URL and path
    sections on call:

    >>> new_res = res('foo', 'bar/baz')
    >>> assert new_res is not res
    >>> new_res.url
    'http://localhost:5984/foo/bar%2Fbaz'

    Also holds a :class:`HttpSession` instance and shares it with subresources:

    >>> res.session is new_res.session
    True
    """

    session_class = HttpSession

    def __init__(self, url, *, loop=None, session=None):
        self._loop = loop
        self.url = url
        self.session = session or self.session_class()

    def __call__(self, *path):
        return type(self)(urljoin(self.url, *path),
                          loop=self._loop,
                          session=self.session)

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.url,
            hex(id(self)))

    def head(self, path=None, **options):
        """Makes HEAD request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('HEAD', path, **options)

    def get(self, path=None, **options):
        """Makes GET request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('GET', path, **options)

    def post(self, path=None, **options):
        """Makes POST request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('POST', path, **options)

    def put(self, path=None, **options):
        """Makes PUT request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('PUT', path, **options)

    def delete(self, path=None, **options):
        """Makes DELETE request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('DELETE', path, **options)

    def copy(self, path=None, **options):
        """Makes COPY request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('COPY', path, **options)

    def options(self, path=None, **options):
        """Makes OPTIONS request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('OPTIONS', path, **options)

    def request(self, method, path=None, data=None, headers=None, params=None,
                auth=None, **options):
        """Makes a HTTP request to the resource.

        :param str method: HTTP method
        :param str path: Resource relative path
        :param bytes data: POST/PUT request payload data
        :param dict headers: Custom HTTP request headers
        :param dict params: Custom HTTP request query parameters
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param options: Additional options for :meth:`aiohttp.client.request`
                       function

        :returns: :class:`aiocouchdb.client.HttpResponse` instance
        """
        url = urljoin(self.url, path) if path else self.url

        return self.session.request(method, url,
                                    auth=auth,
                                    data=data,
                                    headers=headers,
                                    params=params,
                                    loop=options.pop('loop', self._loop),
                                    **options)


def urljoin(base, *path):
    """Assemble a URI based on a base, any number of path segments, and query
    string parameters.

    >>> urljoin('http://example.org', '_all_dbs')
    'http://example.org/_all_dbs'

    A trailing slash on the uri base is handled gracefully:

    >>> urljoin('http://example.org/', '_all_dbs')
    'http://example.org/_all_dbs'

    And multiple positional arguments become path parts:

    >>> urljoin('http://example.org/', 'foo', 'bar')
    'http://example.org/foo/bar'

    All slashes within a path part are escaped:

    >>> urljoin('http://example.org/', 'foo/bar')
    'http://example.org/foo%2Fbar'
    >>> urljoin('http://example.org/', 'foo', '/bar/')
    'http://example.org/foo/%2Fbar%2F'

    >>> urljoin('http://example.org/', None) #doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError: argument 2 to map() must support iteration
    """
    base = base.rstrip('/')
    if not path:
        return base
    return '/'.join([base] + [urllib.parse.quote(s, '') for s in path])


def extract_credentials(url):
    """Extract authentication (user name and password) credentials from the
    given URL.

    >>> extract_credentials('http://localhost:5984/_config/')
    ('http://localhost:5984/_config/', None)
    >>> extract_credentials('http://joe:secret@localhost:5984/_config/')
    ('http://localhost:5984/_config/', ('joe', 'secret'))
    >>> extract_credentials('http://joe%40example.com:secret@localhost:5984/_config/')
    ('http://localhost:5984/_config/', ('joe@example.com', 'secret'))
    """
    parts = urllib.parse.urlsplit(url)
    netloc = parts[1]
    if '@' in netloc:
        creds, netloc = netloc.split('@')
        credentials = tuple(urllib.parse.unquote(i)
                            for i in creds.split(':'))
        parts = list(parts)
        parts[1] = netloc
    else:
        credentials = None
    return urllib.parse.urlunsplit(parts), credentials
