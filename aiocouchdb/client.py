# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiohttp
import io
import json
import types
import urllib.parse
from .errors import maybe_raise_error


# FIXME: workaround of decompressing empty payload.
# https://github.com/KeepSafe/aiohttp/pull/154
class HttpPayloadParser(aiohttp.HttpPayloadParser):

    def __call__(self, out, buf):
        # payload params
        length = self.message.headers.get('CONTENT-LENGTH', self.length)
        if 'SEC-WEBSOCKET-KEY1' in self.message.headers:
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

        elif 'chunked' in self.message.headers.get('TRANSFER-ENCODING', ''):
            yield from self.parse_chunked_payload(out, buf)

        elif length is not None:
            try:
                length = int(length)
            except ValueError:
                raise aiohttp.errors.InvalidHeader('CONTENT-LENGTH') from None

            if length < 0:
                raise aiohttp.errors.InvalidHeader('CONTENT-LENGTH')
            elif length > 0:
                yield from self.parse_length_payload(out, buf, length)
        else:
            if self.readall and getattr(self.message, 'code', 0) != 204:
                yield from self.parse_eof_payload(out, buf)
            elif getattr(self.message, 'method', None) in ('PUT', 'POST'):
                aiohttp.log.internal_log.warning(  # pragma: no cover
                    'Content-Length or Transfer-Encoding header is required')

        out.feed_eof()

aiohttp.HttpPayloadParser = HttpPayloadParser


class HttpRequest(aiohttp.client.ClientRequest):
    """:class:`aiohttp.client.ClientRequest` class with CouchDB specifics."""

    #: Default HTTP request headers.
    DEFAULT_HEADERS = {
        'ACCEPT': 'application/json',
        'ACCEPT-ENCODING': 'gzip, deflate',
        'CONTENT-TYPE': 'application/json'
    }
    CHUNK_SIZE = 8192

    def update_body_from_data(self, data):
        """Encodes ``data`` as JSON if `Content-Type`
        is :mimetype:`application/json`."""
        if self.headers.get('CONTENT-TYPE') == 'application/json':
            if not (isinstance(data, (types.GeneratorType, io.IOBase))):
                data = json.dumps(data)
        return super().update_body_from_data(data)

    def update_path(self, params):
        if isinstance(params, dict):
            params = params.copy()
            for key, value in params.items():
                if value is True:
                    params[key] = 'true'
                elif value is False:
                    params[key] = 'false'
        return super().update_path(params)


class HttpResponse(aiohttp.client.ClientResponse):
    """Deviation from :class:`aiohttp.client.ClientResponse` class for
    CouchDB specifics. Prefers :class:`~aiohttp.streams.FlowControlChunksQueue`
    flow control which fits the best to handle chunked responses.
    """

    flow_control_class = aiohttp.FlowControlChunksQueue

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


class HttpStreamResponse(HttpResponse):
    """Like :class:`HttpResponse`, but uses
    :class:`~aiohttp.streams.FlowControlStreamReader` to handle nicely large
    non-chunked data streams."""

    flow_control_class = aiohttp.FlowControlStreamReader


class Resource(object):
    """HTTP resource representation. Accepts full ``url`` as argument.

    >>> res = Resource('http://localhost:5984')
    >>> res
    <Resource @ 'http://localhost:5984'>

    Able to construct new Resource instance by assemble base URL and path
    sections on call:

    >>> new_res = res('foo', 'bar/baz')
    >>> assert new_res is not res
    >>> new_res.url
    'http://localhost:5984/foo/bar%2Fbaz'
    """

    request_class = HttpRequest
    response_class = HttpResponse

    def __init__(self, url, *, request_class=None, response_class=None):
        self.url = url
        if request_class is not None:
            self.request_class = request_class
        if response_class is not None:
            self.response_class = response_class

    def __call__(self, *path):
        return type(self)(urljoin(self.url, *path),
                          request_class=self.request_class,
                          response_class=self.response_class)

    def __repr__(self):
        return '<{} @ {!r}>'.format(type(self).__name__, self.url)

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

    @asyncio.coroutine
    def request(self, method, path=None, data=None, headers=None, params=None,
                auth=None, **options):
        """Makes a HTTP request to the resource.

        :param str method: HTTP method
        :param str path: Resource relative path
        :param bytes data: POST/PUT request payload data
        :param dict headers: Custom HTTP request headers
        :param dict params: Custom HTTP request query parameters
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param options: Additional options for :func:`aiohttp.client.request`
                       function

        :returns: :class:`aiocouchdb.client.HttpResponse` instance
        """
        url = urljoin(self.url, path) if path else self.url
        headers = headers or {}
        params = params or {}

        if auth is not None:
            self.apply_auth(auth, url, headers)

        options.setdefault('request_class', self.request_class)
        options.setdefault('response_class', self.response_class)

        resp = yield from aiohttp.request(method, url,
                                          data=data,
                                          headers=headers,
                                          params=params,
                                          **options)
        if auth is not None:
            self.update_auth(auth, resp)

        return resp

    def apply_auth(self, auth_provider, url, headers):
        """Applies authentication routines on further request.

        :param auth_provider: :class:`aiocouchdb.authn.AuthProvider` instance
        :param str url: Request URL
        :param dict headers: Request headers
        """
        auth_provider.sign(url, headers)

    def update_auth(self, auth_provider, response):
        """Updates authentication provider state from the HTTP response data.

        :param auth_provider: :class:`aiocouchdb.authn.AuthProvider` instance
        :param response: :class:`aiocouchdb.client.HttpResponse` instance
        """
        auth_provider.update(response)


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
