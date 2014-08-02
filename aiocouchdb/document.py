# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

import json
from collections.abc import Mapping
from .client import Resource


class Document(object):
    """Implementation of :ref:`CouchDB Document API <api/doc>`."""

    def __init__(self, url_or_resource):
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource

    @property
    def id(self):
        """Returns associated document ID."""
        return self.resource.url.rsplit('/', 1)[-1]

    @asyncio.coroutine
    def exists(self, rev=None, *, auth=None):
        """Checks if `document exists`_ in the database. Assumes success
        on receiving response with `200 OK` status.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _document exists: http://docs.couchdb.org/en/latest/api/document/common.html#head--db-docid
        """
        resp = yield from self.resource.head(auth=auth, params={'rev': rev})
        yield from resp.read()
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
                                             headers={'IF-NONE-MATCH': qrev})
        yield from resp.maybe_raise_error()
        return resp.status != 304

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
        :param bool meta: Includes meta information in the document.
        :param list open_revs: Returns the specified leaf revisions.
        :param bool revs: Includes information about all known revisions
        :param bool revs_info: Includes information about all known revisions
                               and their status

        :rtype: dict or list if `open_revs` specified

        .. _Returns a document: http://docs.couchdb.org/en/latest/api/document/common.html#get--db-docid
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('att_encoding_info', att_encoding_info)
        maybe_set_param('attachments', attachments)
        maybe_set_param('atts_since', atts_since)
        maybe_set_param('conflicts', conflicts)
        maybe_set_param('deleted_conflicts', deleted_conflicts)
        maybe_set_param('local_seq', local_seq)
        maybe_set_param('meta', meta)
        maybe_set_param('open_revs', open_revs)
        maybe_set_param('rev', rev)
        maybe_set_param('revs', revs)
        maybe_set_param('revs_info', revs_info)

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        if open_revs is not None and open_revs != 'all':
            params['open_revs'] = json.dumps(open_revs)

        resp = yield from self.resource.get(auth=auth, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def update(self, doc, *, auth=None, batch=None, new_edits=None, rev=None):
        """`Updates a document`_ on server.

        :param dict doc: Document object. Should implement
                        :class:`~collections.abc.Mapping` interface

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param str batch: Updates in batch mode (asynchronously).
                          This argument accepts only ``"ok"`` value.
        :param bool new_edits: Signs about new document edition. When ``False``
                               allows to create conflicts manually
        :param str rev: Document revision. Optional, since document ``_rev``
                        field is also respected

        :rtype: dict

        .. _Updates a document: http://docs.couchdb.org/en/latest/api/document/common.html#put--db-docid
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('batch', batch)
        maybe_set_param('new_edits', new_edits)
        maybe_set_param('rev', rev)

        if not isinstance(doc, Mapping):
            raise TypeError('Mapping instance expected, dict - preferred')

        if '_id' in doc and doc['_id'] != self.id:
            raise ValueError('Attempt to store document with different ID: '
                             '%r ; expected: %r. May you want to .copy() it?'
                             % (doc['_id'], self.id))

        resp = yield from self.resource.put(auth=auth, data=doc, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def remove(self, rev, *, auth=None, preserve_content=None):
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
    def copy(self, newid, *, auth=None):
        """`Copies a document`_ with the new ID within the same database.

        :param str newid: New document ID
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Copies a document: http://docs.couchdb.org/en/latest/api/document/common.html#copy--db-docid
        """
        headers = {'DESTINATION': newid}
        resp = yield from self.resource.copy(auth=auth, headers=headers)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())