# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2016 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json
import io
import uuid
from collections.abc import MutableMapping

from multidict import CIMultiDict
from aiocouchdb.client import Resource
from aiocouchdb.hdrs import (
    ACCEPT,
    CONTENT_LENGTH,
    CONTENT_TYPE,
    DESTINATION,
    ETAG,
    IF_NONE_MATCH
)
from aiocouchdb.multipart import MultipartReader, MultipartWriter

from .attachment import Attachment


__all__ = (
    'Document',
)


class Document(object):
    """Implementation of :ref:`CouchDB Document API <api/doc>`."""

    attachment_class = Attachment

    def __init__(self, url_or_resource, *,
                 attachment_class=None,
                 docid=None,
                 loop=None):
        if attachment_class is not None:
            self.attachment_class = attachment_class
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource, loop=loop)
        self.resource = url_or_resource
        self._docid = docid

    def __getitem__(self, attname):
        resource = self.resource(*attname.split('/'))
        return self.attachment_class(resource, name=attname)

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

    @property
    def id(self):
        """Returns a document id specified in class constructor."""
        return self._docid

    @asyncio.coroutine
    def att(self, attname, *, auth=None):
        """Returns :class:`~aiocouchdb.v1.attachment.Attachment` instance
        against specified attachment.

        If attachment isn't accessible for auth provided credentials,
        this method raises :exc:`aiocouchdb.errors.HttpErrorException`
        with the related response status code.

        :param str attname: Attachment name
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: :attr:`aiocouchdb.v1.document.Document.attachment_class`
        """
        att = self[attname]
        resp = yield from att.resource.head(auth=auth)
        if resp.status != 404:
            yield from resp.maybe_raise_error()
        yield from resp.release()
        return att

    @asyncio.coroutine
    def exists(self, rev=None, *, auth=None):
        """Checks if `document exists`_ in the database. Assumes success
        on receiving response with `200 OK` status.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _document exists: http://docs.couchdb.org/en/latest/api/document/common.html#head--db-docid
        """
        params = {}
        if rev is not None:
            params['rev'] = rev
        resp = yield from self.resource.head(auth=auth, params=params)
        yield from resp.release()
        return resp.status == 200

    @asyncio.coroutine
    def modified(self, rev, *, auth=None):
        """Checks if `document was modified`_ in database since specified
        revision.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _document was modified: http://docs.couchdb.org/en/latest/api/document/common.html#head--db-docid
        """
        qrev = '"%s"' % rev
        resp = yield from self.resource.head(auth=auth,
                                             headers={IF_NONE_MATCH: qrev})
        yield from resp.maybe_raise_error()
        yield from resp.release()
        return resp.status != 304

    @asyncio.coroutine
    def rev(self, *, auth=None):
        """Returns current document revision by using `HEAD request`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: str

        .. _HEAD request: http://docs.couchdb.org/en/latest/api/document/common.html#head--db-docid
        """
        resp = yield from self.resource.head(auth=auth)
        yield from resp.maybe_raise_error()
        yield from resp.release()
        return resp.headers[ETAG].strip('"')

    @asyncio.coroutine
    def get(self, rev=None, *,
            auth=None,
            att_encoding_info=None,
            attachments=None,
            atts_since=None,
            conflicts=None,
            deleted_conflicts=None,
            local_seq=None,
            meta=None,
            open_revs=None,
            revs=None,
            revs_info=None):
        """`Returns a document`_ object.

        :param str rev: Document revision

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes the Base64-encoded content of an
                                 attachments in the documents
        :param list atts_since: Includes attachments that was added since
                                the specified revisions
        :param bool conflicts: Includes conflicts information in the documents
        :param bool deleted_conflicts: Includes information about deleted
                                       conflicted revisions in the document
        :param bool local_seq: Includes local sequence number in the document
        :param bool meta: Includes meta information in the document
        :param list open_revs: Returns the specified leaf revisions
        :param bool revs: Includes information about all known revisions
        :param bool revs_info: Includes information about all known revisions
                               and their status

        :rtype: dict or list if `open_revs` specified

        .. _Returns a document: http://docs.couchdb.org/en/latest/api/document/common.html#get--db-docid
        """
        params = dict((key, value)
                      for key, value in locals().items()
                      if (key not in {'self', 'auth'} and
                          value is not None))

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        if open_revs is not None and open_revs != 'all':
            params['open_revs'] = json.dumps(open_revs)

        resp = yield from self.resource.get(auth=auth, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def get_open_revs(self, *open_revs,
                      auth=None,
                      att_encoding_info=None,
                      atts_since=None,
                      latest=None,
                      local_seq=None,
                      revs=None):
        """Returns reader for multiple document conflicted revisions with their
        attachments.

        Unlike :func:`get(open_revs=[...])
        <aiocouchdb.v1.document.Document.get>`, this method works with
        :mimetype:`multipart/mixed` response returning multipart reader which is
        more optimized to handle large data sets with lesser memory footprint.

        Note, that this method always returns attachments along with leaf
        revisions.

        :param list open_revs: Leaf revisions to return. If omitted, all leaf
                               revisions will be returned

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachments stubs
        :param list atts_since: Includes attachments that was added since
                                the specified revisions
        :param bool latest: Ensure latest revision is included in response
        :param bool local_seq: Includes local sequence number in each document
        :param bool revs: Includes information about all known revisions in
                          each document

        :rtype: :class:`~aiocouchdb.v1.document.OpenRevsMultipartReader`
        """
        params = dict((key, value)
                      for key, value in locals().items()
                      if (key not in {'self', 'auth'} and
                          value is not None))

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        params['open_revs'] = json.dumps(open_revs) if open_revs else 'all'

        resp = yield from self.resource.get(auth=auth,
                                            headers={ACCEPT: 'multipart/mixed'},
                                            params=params)
        yield from resp.maybe_raise_error()
        reader = OpenRevsMultipartReader.from_response(resp)
        return reader

    @asyncio.coroutine
    def get_with_atts(self, rev=None, *,
                      auth=None,
                      att_encoding_info=None,
                      atts_since=None,
                      conflicts=None,
                      deleted_conflicts=None,
                      local_seq=None,
                      meta=None,
                      revs=None,
                      revs_info=None):
        """Returns document with attachments.

        This method is more optimal than :func:`get(attachments=true)
        <aiocouchdb.v1.document.Document.get>` since it uses multipart API and
        doesn't requires to read all the attachments, extract then from JSON
        document and decode from base64.

        :param str rev: Document revision

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param list atts_since: Includes attachments that was added since
                                the specified revisions
        :param bool conflicts: Includes conflicts information in the documents
        :param bool deleted_conflicts: Includes information about deleted
                                       conflicted revisions in the document
        :param bool local_seq: Includes local sequence number in the document
        :param bool meta: Includes meta information in the document
        :param bool revs: Includes information about all known revisions
        :param bool revs_info: Includes information about all known revisions
                               and their status

        :rtype: :class:`~aiocouchdb.v1.document.DocAttachmentsMultipartReader`
        """
        params = dict((key, value)
                      for key, value in locals().items()
                      if (key not in {'self', 'auth'} and
                          value is not None))
        params['attachments'] = True

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        resp = yield from self.resource.get(
            auth=auth,
            headers={ACCEPT: 'multipart/related, application/json'},
            params=params)

        yield from resp.maybe_raise_error()

        if resp.headers[CONTENT_TYPE].startswith('application/json'):
            # WARNING! Here be Hacks!
            # If document has no attachments, CouchDB returns it as JSON
            # so we have to fake multipart response in the name of consistency.
            # However, this hack may not lasts for too long.
            data = yield from resp.read()
            boundary = str(uuid.uuid4())
            headers = dict(resp.headers.items())
            headers[CONTENT_TYPE] = 'multipart/related;boundary=%s' % boundary
            resp.headers = CIMultiDict(**headers)
            resp.content._buffer.extend(
                b'--' + boundary.encode('latin1') + b'\r\n'
                b'Content-Type: application/json\r\n'
                b'\r\n' + data.rstrip() + b'\r\n'
                b'--' + boundary.encode('latin1') + b'--\r\n'
            )

        return DocAttachmentsMultipartReader.from_response(resp)

    @asyncio.coroutine
    def update(self, doc, *,
               atts=None,
               auth=None,
               batch=None,
               new_edits=None,
               rev=None):
        """`Updates a document`_ on server.

        :param dict doc: Document object. Should implement
                        :class:`~collections.abc.MutableMapping` interface

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param dict atts: Attachments mapping where keys are represents
                          attachment name and value is file-like object or
                          bytes
        :param str batch: Updates in batch mode (asynchronously)
                          This argument accepts only ``"ok"`` value.
        :param bool new_edits: Signs about new document edition. When ``False``
                               allows to create conflicts manually
        :param str rev: Document revision. Optional, since document ``_rev``
                        field is also respected

        :rtype: dict

        .. _Updates a document: http://docs.couchdb.org/en/latest/api/document/common.html#put--db-docid
        """
        params = dict((key, value)
                      for key, value in locals().items()
                      if (key not in {'self', 'doc', 'auth', 'atts'} and
                          value is not None))

        if not isinstance(doc, MutableMapping):
            raise TypeError('MutableMapping instance expected, like a dict')

        if '_id' in doc and doc['_id'] != self.id:
            raise ValueError('Attempt to store document with different ID: '
                             '%r ; expected: %r. May be you want to .copy() it?'
                             % (doc['_id'], self.id))

        if atts:
            writer = MultipartWriter('related')
            writer.append_json(doc)

            doc.setdefault('_attachments', {})

            # A little hack to sync the order of attachments definition
            # between JSON and multipart body parts
            for name in atts:
                doc['_attachments'][name] = {}

            for name, stub in doc['_attachments'].items():
                if stub:
                    continue
                att = atts[name]
                if not isinstance(att, (bytes, io.BytesIO, io.BufferedIOBase)):
                    raise TypeError('attachment payload should be a source of'
                                    ' binary data (bytes, BytesIO, file '
                                    ' opened in binary mode), got %r' % att)
                part = writer.append(att)
                part.set_content_disposition('attachment', filename=name)
                doc['_attachments'][name] = {
                    'length': int(part.headers[CONTENT_LENGTH]),
                    'follows': True,
                    'content_type': part.headers[CONTENT_TYPE]
                }

            writer.parts[0].headers[CONTENT_LENGTH] = \
                str(len(json.dumps(doc).encode('utf-8')))

            # workaround of COUCHDB-2295
            writer.headers[CONTENT_LENGTH] = str(writer.calc_content_length())

            resp = yield from self.resource.put(auth=auth,
                                                data=writer,
                                                params=params)

            for info in doc['_attachments'].values():
                info.pop('follows')
                info['stub'] = True
        else:
            resp = yield from self.resource.put(auth=auth,
                                                data=doc,
                                                params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def delete(self, rev, *, auth=None, preserve_content=None):
        """`Deletes a document`_ from server.

        By default document will be deleted using `DELETE` HTTP method.
        On this request CouchDB removes all document fields, leaving only
        system ``_id`` and ``_rev`` and adding ``"_deleted": true`` one. When
        `preserve_content` set to ``True``, document will be marked as deleted
        (by adding ``"_deleted": true`` field without removing existed ones)
        via `PUT` request. This feature costs two requests to fetch and update
        the document and also such documents consumes more space by oblivious
        reasons.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param bool preserve_content: Whenever to preserve document content
                                      on deletion

        :rtype: dict

        .. _Deletes a document: http://docs.couchdb.org/en/latest/api/document/common.html#delete--db-docid
        """
        params = {'rev': rev}
        if preserve_content:
            doc = yield from self.get(rev=rev)
            doc['_deleted'] = True
            resp = yield from self.resource.put(auth=auth, data=doc,
                                                params=params)
        else:
            resp = yield from self.resource.delete(auth=auth, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def copy(self, newid, rev=None, *, auth=None):
        """`Copies a document`_ with the new ID within the same database.

        :param str newid: New document ID
        :param str rev: New document ID revision. Used for copying over existed
                        document
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Copies a document: http://docs.couchdb.org/en/latest/api/document/common.html#copy--db-docid
        """
        dest = newid
        if rev is not None:
            dest += '?rev=' + rev
        headers = {DESTINATION: dest}
        resp = yield from self.resource.copy(auth=auth, headers=headers)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())


class DocAttachmentsMultipartReader(MultipartReader):
    """Special multipart reader optimized for requesting single document with
    attachments. Matches output with :class:`OpenRevsMultipartReader`."""

    @asyncio.coroutine
    def next(self):
        """Emits a tuple of document object (:class:`dict`) and multipart reader
        of the followed attachments (if any).

        :rtype: tuple
        """
        # WARNING! Here be Hacks!
        part = self._last_part
        if part is not None and part.at_eof():
            self._at_eof = True

        reader = yield from super().next()

        if self._at_eof:
            return None, None

        attsreader = MultipartReader(self.headers,
                                     self.content)  # pylint: disable=no-member
        self._last_part = attsreader
        attsreader._unread = reader._unread

        doc = yield from reader.json()

        return doc, attsreader


class OpenRevsMultipartReader(MultipartReader):
    """Special multipart reader optimized for reading document`s open revisions
    with attachments."""

    multipart_reader_cls = MultipartReader

    @asyncio.coroutine
    def next(self):
        """Emits a tuple of document object (:class:`dict`) and multipart reader
        of the followed attachments (if any).

        :rtype: tuple
        """
        reader = yield from super().next()

        if self._at_eof:
            return None, None

        if isinstance(reader, self.multipart_reader_cls):
            part = yield from reader.next()
            doc = yield from part.json()
        else:
            doc = yield from reader.json()

        return doc, reader
