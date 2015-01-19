# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json
import zlib

from aiohttp.helpers import parse_mimetype
from aiohttp.protocol import HttpParser
from .hdrs import (
    CONTENT_ENCODING,
    CONTENT_LENGTH,
    CONTENT_TYPE
)


class MultipartResponseWrapper(object):
    """Wrapper around the :class:`MultipartBodyReader` to take care about
    underlying connection and close it when it needs in."""

    def __init__(self, resp, stream):
        self.resp = resp
        self.stream = stream

    def at_eof(self):
        """Returns ``True`` when all response data had been read.

        :rtype: bool
        """
        return self.resp.content.at_eof()

    @asyncio.coroutine
    def next(self):
        """Emits next multipart reader object."""
        item = yield from self.stream.next()
        if self.stream.at_eof():
            yield from self.release()
        return item

    @asyncio.coroutine
    def release(self):
        """Releases the connection gracefully, reading all the content
        to the void."""
        yield from self.resp.release()


class BodyPartReader(object):
    """Multipart reader for single body part."""

    chunk_size = 8192

    def __init__(self, boundary, headers, content):
        self.headers = headers
        self._boundary = boundary
        self._content = content
        self._at_eof = False
        length = self.headers.get(CONTENT_LENGTH, None)
        self._length = int(length) if length is not None else None
        self._read_bytes = 0
        self._unread = []

    @asyncio.coroutine
    def next(self):
        item = yield from self.read()
        if not item:
            return None
        return item

    @asyncio.coroutine
    def read(self, *, decode=False):
        """Reads body part data.

        :param bool decode: Decodes data following by encoding
                            method from `Content-Encoding` header. If it missed
                            data remains untouched

        :rtype: bytearray
        """
        if self._at_eof:
            return b''
        data = bytearray()
        if self._length is None:
            while not self._at_eof:
                data.extend((yield from self.readline()))
        else:
            while not self._at_eof:
                data.extend((yield from self.read_chunk(self.chunk_size)))
            assert b'\r\n' == (yield from self._content.readline()), \
                'reader did not read all the data or it is malformed'
        if decode:
            return self.decode(data)
        return data

    @asyncio.coroutine
    def read_chunk(self, size=chunk_size):
        """Reads body part content chunk of the specified size.
        The body part must has `Content-Length` header with proper value.

        :param int size: chunk size

        :rtype: bytearray
        """
        if self._at_eof:
            return b''
        assert self._length is not None, \
            'Content-Length required for chunked read'
        chunk_size = min(size, self._length - self._read_bytes)
        chunk = yield from self._content.read(chunk_size)
        self._read_bytes += chunk_size
        if self._read_bytes == self._length:
            self._at_eof = True
        return chunk

    @asyncio.coroutine
    def readline(self):
        """Reads body part by line by line.

        :rtype: bytearray
        """
        if self._at_eof:
            return b''
        line = yield from self._content.readline()
        if line.startswith(self._boundary):
            # the very last boundary may not come with \r\n,
            # so set single rules for everyone
            sline = line.rstrip(b'\r\n')
            boundary = self._boundary
            last_boundary = self._boundary + b'--'
            # ensure that we read exactly the boundary, not something alike
            if sline == boundary or sline == last_boundary:
                self._at_eof = True
                self._unread.append(line)
                return b''
        return line

    @asyncio.coroutine
    def release(self):
        """Lke :meth:`read`, but reads all the data to the void.

        :rtype: None
        """
        if self._at_eof:
            return
        if self._length is None:
            while not self._at_eof:
                yield from self.readline()
        else:
            while not self._at_eof:
                yield from self.read_chunk(self.chunk_size)
            assert b'\r\n' == (yield from self._content.readline())

    @asyncio.coroutine
    def text(self, *, encoding=None):
        """Lke :meth:`read`, but assumes that body part contains text data.

        :param str encoding: Custom text encoding. Overrides specified
                             in charset param of `Content-Type` header

        :rtype: str
        """
        data = yield from self.read(decode=True)
        encoding = encoding or self.get_charset(default='latin1')
        return data.decode(encoding)

    @asyncio.coroutine
    def json(self, *, encoding=None):
        """Lke :meth:`read`, but assumes that body parts contains JSON data.

        :param str encoding: Custom JSON encoding. Overrides specified
                             in charset param of `Content-Type` header
        """
        data = yield from self.read(decode=True)
        if not data:
            return None
        encoding = encoding or self.get_charset(default='utf-8')
        return json.loads(data.decode(encoding))

    def at_eof(self):
        """Returns ``True`` if the boundary was reached or
        ``False`` otherwise.

        :rtype: bool
        """
        return self._at_eof

    def decode(self, data):
        """Decodes data from specified `Content-Encoding` header value.
        This method looks for the handler within bounded instance for
        the related encoding. For instance, to decode ``gzip`` encoding it looks
        for :meth:`decode_gzip` one. Otherwise, if handler wasn't found
        :exc:`AttributeError` will get raised.

        :param bytearray data: Data to decode.

        :rtype: bytes
        """
        encoding = self.headers.get(CONTENT_ENCODING)
        if not encoding:
            return data
        return getattr(self, 'decode_%s' % encoding)(data)

    def decode_deflate(self, data):
        """Decodes data for ``Content-Encoding: deflate``."""
        return zlib.decompress(data, -zlib.MAX_WBITS)

    def decode_gzip(self, data):
        """Decodes data for ``Content-Encoding: gzip``."""
        return zlib.decompress(data, 16 + zlib.MAX_WBITS)

    def get_charset(self, default=None):
        """Returns charset parameter from ``Content-Type`` header or default."""
        ctype = self.headers.get(CONTENT_TYPE, '')
        *_, params = parse_mimetype(ctype)
        return params.get('charset', default)


class MultipartReader(object):
    """Multipart body reader."""

    #: Response wrapper, used when multipart readers constructs from response.
    response_wrapper_cls = MultipartResponseWrapper
    #: Multipart reader class, used to handle multipart/* body parts.
    #: None points to type(self)
    multipart_reader_cls = None
    #: Body part reader class for non multipart/* content types.
    part_reader_cls = BodyPartReader

    def __init__(self, headers, content):
        self.headers = headers
        self._boundary = ('--' + self._get_boundary()).encode()
        self._content = content
        self._last_part = None
        self._at_eof = False
        self._unread = []

    @classmethod
    def from_response(cls, response):
        """Constructs reader instance from HTTP response.

        :param response: :class:`~aiocouchdb.client.HttpResponse` instance
        """
        obj = cls.response_wrapper_cls(response, cls(response.headers,
                                                     response.content))
        return obj

    def at_eof(self):
        """Returns ``True`` if the final boundary was reached or
        ``False`` otherwise.

        :rtype: bool
        """
        return self._at_eof

    @asyncio.coroutine
    def next(self):
        """Emits the next multipart body part."""
        if self._at_eof:
            return
        yield from self._maybe_release_last_part()
        yield from self._read_boundary()
        if self._at_eof:  # we just read the last boundary, nothing to do there
            return
        self._last_part = yield from self.fetch_next_part()
        return self._last_part

    @asyncio.coroutine
    def release(self):
        """Reads all the body parts to the void till the final boundary."""
        while not self._at_eof:
            item = yield from self.next()
            if item is None:
                break
            yield from item.release()

    @asyncio.coroutine
    def fetch_next_part(self):
        """Returns the next body part reader."""
        headers = yield from self._read_headers()
        return self._get_part_reader(headers)

    def _get_part_reader(self, headers):
        """Dispatches the response by the `Content-Type` header, returning
        suitable reader instance.

        :param dict headers: Response headers
        """
        ctype = headers.get(CONTENT_TYPE, '')
        mtype, *_ = parse_mimetype(ctype)
        if mtype == 'multipart':
            if self.multipart_reader_cls is None:
                return type(self)(headers, self._content)
            return self.multipart_reader_cls(headers, self._content)
        else:
            return self.part_reader_cls(self._boundary, headers, self._content)

    def _get_boundary(self):
        mtype, *_, params = parse_mimetype(self.headers[CONTENT_TYPE])
        assert mtype == 'multipart'
        return params['boundary']

    @asyncio.coroutine
    def _readline(self):
        if self._unread:
            return self._unread.pop()
        return (yield from self._content.readline())

    @asyncio.coroutine
    def _read_boundary(self):
        chunk = (yield from self._readline()).rstrip()
        if chunk == self._boundary:
            pass
        elif chunk == self._boundary + b'--':
            self._at_eof = True
        else:
            raise ValueError('Invalid boundary %r, expected %r'
                             % (chunk, self._boundary))

    @asyncio.coroutine
    def _read_headers(self):
        lines = ['']
        while True:
            chunk = yield from self._content.readline()
            chunk = chunk.decode().strip()
            lines.append(chunk)
            if not chunk:
                break
        parser = HttpParser()
        headers, *_ = parser.parse_headers(lines)
        return headers

    @asyncio.coroutine
    def _maybe_release_last_part(self):
        """Ensures that the last read body part is read completely."""
        if self._last_part is not None:
            if not self._last_part.at_eof():
                yield from self._last_part.release()
            self._unread.extend(self._last_part._unread)
            self._last_part = None
