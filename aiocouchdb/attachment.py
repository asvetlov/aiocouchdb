# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import base64
from .client import Resource, HttpStreamResponse
from io import RawIOBase


class Attachment(object):
    """Implementation of :ref:`CouchDB Attachment API <api/doc/attachment>`."""

    def __init__(self, url_or_resource, *, name=None):
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource
        self._name = name

    @property
    def name(self):
        return self._name

    @asyncio.coroutine
    def exists(self, rev=None, *, auth=None, **request_options):
        """Checks if `attachment exists`_. Assumes success on receiving response
        with `200 OK` status.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _attachment exists: http://docs.couchdb.org/en/latest/api/document/attachments.html#head--db-docid-attname
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        resp = yield from self.resource.head(auth=auth, params=params, **request_options)
        yield from resp.read()
        return resp.status == 200

    @asyncio.coroutine
    def modified(self, digest, *, auth=None, **request_options):
        """Checks if `attachment was modified`_ by known MD5 digest.

        :param bytes digest: Attachment MD5 digest. Optionally,
                             may be passed in base64 encoding form
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _attachment was modified: http://docs.couchdb.org/en/latest/api/document/attachments.html#head--db-docid-attname
        """
        if isinstance(digest, bytes):
            if len(digest) != 16:
                raise ValueError('MD5 digest has 16 bytes')
            digest = base64.b64encode(digest).decode()
        elif isinstance(digest, str):
            if not (len(digest) == 24 and digest.endswith('==')):
                raise ValueError('invalid base64 encoded MD5 digest')
        else:
            raise TypeError('invalid `digest` type {}, bytes or str expected'
                            ''.format(type(digest)))
        qdigest = '"%s"' % digest
        resp = yield from self.resource.head(auth=auth,
                                             headers={'IF-NONE-MATCH': qdigest}, **request_options)
        yield from resp.maybe_raise_error()
        yield from resp.read()
        return resp.status != 304

    @asyncio.coroutine
    def accepts_range(self, rev=None, *, auth=None, **request_options):
        """Returns ``True`` if attachments accepts bytes range requests.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        resp = yield from self.resource.head(auth=auth, params=params, **request_options)
        yield from resp.read()
        return resp.headers.get('ACCEPT-RANGES') == 'bytes'

    @asyncio.coroutine
    def get(self, rev=None, *, auth=None, range=None, **request_options):
        """`Returns an attachment`_ reader object.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param slice range: Bytes range. Could be :func:`slice`
                            or two-element iterable object like :class:`list`
                            etc or just :func:`int`

        :rtype: :class:`~aiocouchdb.attachments.AttachmentReader`

        .. _Returns an attachment: http://docs.couchdb.org/en/latest/api/document/attachments.html#get--db-docid-attname
        """
        headers = {}
        params = {}
        if rev is not None:
            params['rev'] = rev

        if range is not None:
            if isinstance(range, slice):
                start, stop = range.start, range.stop
            elif isinstance(range, int):
                start, stop = 0, range
            else:
                start, stop = range
            headers['RANGE'] = 'bytes={}-{}'.format(start or 0, stop)
        resp = yield from self.resource.get(auth=auth,
                                            headers=headers,
                                            params=params,
                                            response_class=HttpStreamResponse, **request_options)
        yield from resp.maybe_raise_error()
        return AttachmentReader(resp)

    @asyncio.coroutine
    def update(self, fileobj, *,
               auth=None,
               content_encoding=None,
               content_type='application/octet-stream',
               rev=None,
               **request_options):
        """`Attaches a file`_ to document.

        :param file fileobj: File object, should be readable

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param str content_encoding: Content encoding: ``gzip`` or ``identity``
        :param str content_type: Attachment :mimetype:`Content-Type` header
        :param str rev: Document revision

        :rtype: dict

        .. _Attaches a file: http://docs.couchdb.org/en/latest/api/document/attachments.html#put--db-docid-attname
        """
        assert hasattr(fileobj, 'read')

        params = {}
        if rev is not None:
            params['rev'] = rev

        headers = {
            'CONTENT-TYPE': content_type
        }
        if content_encoding is not None:
            headers['CONTENT-ENCODING'] = content_encoding

        resp = yield from self.resource.put(auth=auth,
                                            data=fileobj,
                                            headers=headers,
                                            params=params,
                                            **request_options)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def delete(self, rev, *, auth=None, **request_options):
        """`Deletes an attachment`_.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Deletes an attachment: http://docs.couchdb.org/en/latest/api/document/attachments.html#delete--db-docid-attname
        """
        resp = yield from self.resource.delete(auth=auth,
                                               params={'rev': rev},
                                               **request_options)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())


class AttachmentReader(RawIOBase):
    """Attachment reader implements :class:`io.RawIOBase` interface
    with the exception that all I/O bound methods are coroutines."""

    def __init__(self, resp):
        super().__init__()
        self._resp = resp

    def close(self):
        """Closes attachment reader and underlying connection.

        This method has no effect if the attachment is already closed.
        """
        if not self.closed:
            self._resp.close()

    @property
    def closed(self):
        """Return a bool indicating whether object is closed."""
        return self._resp.content.at_eof()

    def readable(self):
        """Return a bool indicating whether object was opened for reading."""
        return True

    @asyncio.coroutine
    def read(self, size=-1):
        """Read and return up to n bytes, where `size` is an :func:`int`.

        Returns an empty bytes object on EOF, or None if the object is
        set not to block and has no data to read.
        """
        return (yield from self._resp.content.read(size))

    @asyncio.coroutine
    def readall(self, size=8192):
        """Read until EOF, using multiple :meth:`read` call."""
        acc = bytearray()
        while not self.closed:
            acc.extend((yield from self.read(size)))
        return acc

    @asyncio.coroutine
    def readline(self):
        """Read and return a line of bytes from the stream.

        If limit is specified, at most limit bytes will be read.
        Limit should be an :func:`int`.

        The line terminator is always ``b'\\n'`` for binary files; for text
        files, the newlines argument to open can be used to select the line
        terminator(s) recognized.
        """
        return (yield from self._resp.content.readline())

    @asyncio.coroutine
    def readlines(self, hint=None):
        """Return a list of lines from the stream.

        `hint` can be specified to control the number of lines read: no more
        lines will be read if the total size (in bytes/characters) of all
        lines so far exceeds `hint`.
        """
        if hint is None or hint <= 0:
            acc = []
            while not self.closed:
                line = yield from self.readline()
                if line:
                    acc.append(line)
            return acc
        n = 0
        acc = []
        while not self.closed:
            line = yield from self.readline()
            if not line:
                continue
            acc.append(line)
            n += len(line)
            if n >= hint:
                break
        return acc
