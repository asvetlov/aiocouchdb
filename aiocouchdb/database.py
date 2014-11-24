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
import uuid

from .client import Resource
from .document import Document
from .designdoc import DesignDocument
from .feeds import (
    ChangesFeed, LongPollChangesFeed,
    ContinuousChangesFeed, EventSourceChangesFeed
)
from .views import View


class Database(object):
    """Implementation of :ref:`CouchDB Database API <api/db>`."""

    #: Default :class:`~aiocouchdb.document.Document` instance class
    document_class = Document
    #: Default :class:`~aiocouchdb.designdoc.DesignDocument` instance class
    design_document_class = DesignDocument
    #: :class:`Views requesting  helper<aiocouchdb.views.Views>`
    view_class = View

    def __init__(self, url_or_resource, *,
                 dbname=None,
                 document_class=None,
                 design_document_class=None,
                 view_class=None):
        if document_class is not None:
            self.document_class = document_class
        if design_document_class is not None:
            self.design_document_class = design_document_class
        if view_class is not None:
            self.view_class = view_class
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource
        self._security = Security(self.resource)
        self._dbname = dbname

    def __getitem__(self, docid):
        if docid.startswith('_design/'):
            resource = self.resource(*docid.split('/', 1))
            return self.design_document_class(resource, docid=docid)
        else:
            return self.document_class(self.resource(docid), docid=docid)

    @property
    def name(self):
        """Returns a database name specified in class constructor."""
        return self._dbname

    @asyncio.coroutine
    def doc(self, docid=None, *, auth=None, idfun=uuid.uuid4, **request_options):
        """Returns :class:`~aiocouchdb.document.Document` instance against
        specified document ID.

        If document ID wasn't specified, the ``idfun`` function will be used
        to generate it.

        If document isn't accessible for auth provided credentials, this method
        raises :exc:`aiocouchdb.errors.HttpErrorException` with the related
        response status code.

        :param str docid: Document ID
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param idfun: Document ID generation function.
                      Should return ``str`` or other object which could be
                      translated into string

        :rtype: :attr:`aiocouchdb.database.Database.document_class`
        """
        if docid is None:
            docid = str(idfun())
        doc = self[docid]
        resp = yield from doc.resource.head(auth=auth, **request_options)
        if resp.status != 404:
            yield from resp.maybe_raise_error()
        yield from resp.read()
        return doc

    @asyncio.coroutine
    def ddoc(self, docid, *, auth=None, **request_options):
        """Returns :class:`~aiocouchdb.designdoc.DesignDocument` instance
        against specified document ID. This ID may startswith with ``_design/``
        prefix and if it's not prefix will be added automatically.

        If document isn't accessible for auth provided credentials, this method
        raises :exc:`aiocouchdb.errors.HttpErrorException` with the related
        response status code.

        :param str docid: Document ID
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: :attr:`aiocouchdb.database.Database.design_document_class`
        """
        if not docid.startswith('_design/'):
            docid = '_design/' + docid
        ddoc = self[docid]
        resp = yield from ddoc.resource.head(auth=auth, **request_options)
        if resp.status != 404:
            yield from resp.maybe_raise_error()
        yield from resp.read()
        return ddoc

    @asyncio.coroutine
    def exists(self, *, auth=None, **request_options):
        """Checks if `database exists`_ on server. Assumes success on receiving
        response with `200 OK` status.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _database exists: http://docs.couchdb.org/en/latest/api/database/common.html#head--db
        """
        resp = yield from self.resource.head(auth=auth, **request_options)
        yield from resp.read()
        return resp.status == 200

    @asyncio.coroutine
    def info(self, *, auth=None, **request_options):
        """Returns `database information`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _database information: http://docs.couchdb.org/en/latest/api/database/common.html#get--db
        """
        resp = yield from self.resource.get(auth=auth, **request_options)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def create(self, *, auth=None, **request_options):
        """`Creates a database`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _Creates a database: http://docs.couchdb.org/en/latest/api/database/common.html#put--db
        """
        resp = yield from self.resource.put(auth=auth, **request_options)
        yield from resp.maybe_raise_error()
        status = yield from resp.json()
        return status['ok']

    @asyncio.coroutine
    def delete(self, *, auth=None):
        """`Deletes a database`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _Deletes a database: http://docs.couchdb.org/en/latest/api/database/common.html#delete--db
        """
        resp = yield from self.resource.delete(auth=auth, **request_options)
        yield from resp.maybe_raise_error()
        status = yield from resp.json()
        return status['ok']

    @asyncio.coroutine
    def all_docs(self, *keys,
                 auth=None,
                 feed_buffer_size=None,
                 att_encoding_info=None,
                 attachments=None,
                 conflicts=None,
                 descending=None,
                 endkey=...,
                 endkey_docid=None,
                 include_docs=None,
                 inclusive_end=None,
                 limit=None,
                 skip=None,
                 stale=None,
                 startkey=...,
                 startkey_docid=None,
                 update_seq=None,
                 **request_options):
        """Iterates over :ref:`all documents view <api/db/all_docs>`.

        :param str keys: List of document ids to fetch. This method is smart
                         enough to use `GET` or `POST` request depending on
                         amount of ``keys``

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param int feed_buffer_size: Internal buffer size for fetched feed items

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes attachments content into documents.
                                 **Warning**: use with caution!
        :param bool conflicts: Includes conflicts information into documents
        :param bool descending: Return rows in descending by key order
        :param str endkey: Stop fetching rows when the specified key is reached
        :param str endkey_docid: Stop fetching rows when the specified
                                 document ID is reached
        :param str include_docs: Include document body for each row
        :param bool inclusive_end: When ``False``, doesn't includes ``endkey``
                                   in returned rows
        :param int limit: Limits the number of the returned rows by
                          the specified number
        :param int skip: Skips specified number of rows before starting
                         to return the actual result
        :param str stale: Allow to fetch the rows from a stale view, without
                          triggering index update. Supported values: ``ok``
                          and ``update_after``
        :param str startkey: Return rows starting with the specified key
        :param str startkey_docid: Return rows starting with the specified
                                   document ID
        :param bool update_seq: Include an ``update_seq`` value into view
                                results header

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        params = locals()
        for key in ('self', 'auth', 'feed_buffer_size'):
            params.pop(key)
        view = self.view_class(self.resource('_all_docs'))
        return (yield from view.request(auth=auth,
                                        feed_buffer_size=feed_buffer_size,
                                        params=params,
                                        **request_options))

    def bulk_docs(self, docs, *, auth=None, all_or_nothing=None,
                  new_edits=None, **request_options):
        """:ref:`Updates multiple documents <api/db/bulk_docs>` using a single
        request.

        :param Iterable docs: Sequence of document objects (:class:`dict`)
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param bool all_or_nothing: Sets the database commit mode to use
            :ref:`all-or-nothing <api/db/bulk_docs/semantics>` semantics
        :param bool new_edits: If `False`, prevents the database from
                               assigning them new revision for updated documents

        :rtype: list
        """
        def chunkify(docs, all_or_nothing):
            # stream docs one by one to reduce footprint from jsonifying all
            # of them in single shot. useful when docs is generator of docs
            if all_or_nothing is True:
                yield b'{"all_or_nothing": true, "docs": ['
            else:
                yield b'{"docs": ['
            idocs = iter(docs)
            yield json.dumps(next(idocs)).encode('utf-8')
            for doc in idocs:
                yield b',' + json.dumps(doc).encode('utf-8')
            yield b']}'
        params = {} if new_edits is None else {'new_edits': new_edits}
        chunks = chunkify(docs, all_or_nothing)
        resp = yield from self.resource.post(
            '_bulk_docs', auth=auth, data=chunks, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    def changes(self, *doc_ids,
                auth=None,
                feed_buffer_size=None,
                att_encoding_info=None,
                attachments=None,
                conflicts=None,
                descending=None,
                feed=None,
                filter=None,
                headers=None,
                heartbeat=None,
                include_docs=None,
                limit=None,
                params=None,
                since=None,
                style=None,
                timeout=None,
                view=None):
        """Emits :ref:`database changes events<api/db/changes>`.

        :param str doc_ids: Document IDs to filter for. This method is smart
                            enough to use `GET` or `POST` request depending
                            if any ``doc_ids`` were provided or not and
                            automatically sets ``filter`` param to ``_doc_ids``
                            value.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param int feed_buffer_size: Internal buffer size for fetched feed items

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes the Base64-encoded content of an
                                 attachments in the documents
        :param bool conflicts: Includes conflicts information in the documents
        :param bool descending: Return changes in descending order
        :param str feed: :ref:`Changes feed type <changes>`
        :param str filter: Filter function name
        :param dict headers: Custom request headers
        :param int heartbeat: Period in milliseconds after which an empty
                              line will be sent from server as the result
                              to keep connection alive
        :param bool include_docs: Includes the associated document for each
                                  emitted event
        :param int limit: Limits a number of returned events by the specified
                          value
        :param since: Starts listening changes feed since given
                      `update sequence` value
        :param dict params: Custom request query parameters
        :param str style: Changes feed output style: ``all_docs``, ``main_only``
        :param int timeout: Period in milliseconds to await for new changes
                            before close the feed. Works for continuous feeds
        :param str view: View function name which would be used as filter.
                         Implicitly sets ``filter`` param to ``_view`` value

        :rtype: :class:`aiocouchdb.feeds.ChangesFeed`
        """
        print(locals())
        params = dict(params or {})
        params.update((key, value)
                      for key, value in locals().items()
                      if key not in {'self', 'doc_ids', 'auth', 'headers',
                                     'params'}
                      and value is not None)

        if doc_ids:
            data = {'doc_ids': doc_ids}
            if 'filter' not in params:
                params['filter'] = '_doc_ids'
            else:
                assert params['filter'] == '_doc_ids'
            request = self.resource.post
        else:
            data = None
            request = self.resource.get

        if 'view' in params:
            if 'filter' not in params:
                params['filter'] = '_view'
            else:
                assert params['filter'] == '_view'

        resp = yield from request('_changes', auth=auth, data=data,
                                  headers=headers, params=params)
        yield from resp.maybe_raise_error()

        if feed == 'continuous':
            return ContinuousChangesFeed(resp, buffer_size=feed_buffer_size)
        elif feed == 'eventsource':
            return EventSourceChangesFeed(resp, buffer_size=feed_buffer_size)
        elif feed == 'longpoll':
            return LongPollChangesFeed(resp, buffer_size=feed_buffer_size)
        else:
            return ChangesFeed(resp, buffer_size=feed_buffer_size)

    @asyncio.coroutine
    def compact(self, ddoc_name=None, *, auth=None):
        """Initiates :ref:`database <api/db/compact>`
        or :ref:`view index <api/db/compact/ddoc>` compaction.

        :param str ddoc_name: Design document name. If specified initiates
                              view index compaction instead of database
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        path = ['_compact']
        if ddoc_name is not None:
            path.append(ddoc_name)
        resp = yield from self.resource(*path).post(auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def ensure_full_commit(self, *, auth=None):
        """Ensures that all bits are :ref:`committed on disk
        <api/db/ensure_full_commit>`.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_ensure_full_commit', auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def missing_revs(self, id_revs, *, auth=None):
        """Returns :ref:`document missed revisions <api/db/missing_revs>`
        in the database by given document-revisions mapping.

        :param dict id_revs: Mapping between document ID and list of his
                             revisions to search for.
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_missing_revs',
                                             auth=auth, data=id_revs)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def purge(self, id_revs, *, auth=None):
        """:ref:`Permanently removes specified document revisions
        <api/db/purge>` from the database.

        :param dict id_revs: Mapping between document ID and list of his
                             revisions to purge
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_purge',
                                             auth=auth, data=id_revs)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def revs_diff(self, id_revs, *, auth=None):
        """Returns :ref:`document revisions difference <api/db/revs_diff>`
        in the database by given document-revisions mapping.

        :param dict id_revs: Mapping between document ID and list of his
                             revisions to compare
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_revs_diff',
                                             auth=auth, data=id_revs)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def revs_limit(self, count=None, *, auth=None):
        """Returns the :ref:`limit of database revisions <api/db/revs_limit>`
        to store or updates it if ``count`` parameter was specified.

        :param int count: Amount of revisions to store
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: int or dict
        """
        if count is None:
            resp = yield from self.resource.get('_revs_limit', auth=auth)
        else:
            resp = yield from self.resource.put('_revs_limit',
                                                auth=auth, data=count)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @property
    def security(self):
        """Proxy to the related :class:`~aiocouchdb.database.Security`
        instance."""
        return self._security

    @asyncio.coroutine
    def temp_view(self, map_fun, red_fun=None, language=None, *,
                  auth=None,
                  feed_buffer_size=None,
                  att_encoding_info=None,
                  attachments=None,
                  conflicts=None,
                  descending=None,
                  endkey=...,
                  endkey_docid=None,
                  group=None,
                  group_level=None,
                  include_docs=None,
                  inclusive_end=None,
                  keys=...,
                  limit=None,
                  reduce=None,
                  skip=None,
                  stale=None,
                  startkey=...,
                  startkey_docid=None,
                  update_seq=None):
        """Executes :ref:`temporary view <api/db/temp_view>` and returns
        it results according specified parameters.

        :param str map_fun: Map function source code
        :param str red_fun: Reduce function source code
        :param str language: Query server language to process the view

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param int feed_buffer_size: Internal buffer size for fetched feed items

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes attachments content into documents.
                                 **Warning**: use with caution!
        :param bool conflicts: Includes conflicts information into documents
        :param bool descending: Return rows in descending by key order
        :param endkey: Stop fetching rows when the specified key is reached
        :param str endkey_docid: Stop fetching rows when the specified
                                 document ID is reached
        :param bool group: Reduces the view result grouping by unique keys
        :param int group_level: Reduces the view result grouping the keys
                                with defined level
        :param str include_docs: Include document body for each row
        :param bool inclusive_end: When ``False``, doesn't includes ``endkey``
                                   in returned rows
        :param list keys: List of view keys to fetch
        :param int limit: Limits the number of the returned rows by
                          the specified number
        :param bool reduce: Defines is the reduce function needs to be applied
                            or not
        :param int skip: Skips specified number of rows before starting
                         to return the actual result
        :param str stale: Allow to fetch the rows from a stale view, without
                          triggering index update. Supported values: ``ok``
                          and ``update_after``
        :param startkey: Return rows starting with the specified key
        :param str startkey_docid: Return rows starting with the specified
                                   document ID
        :param bool update_seq: Include an ``update_seq`` value into view
                                results header

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        params = locals()
        for key in ('self', 'auth', 'map_fun', 'red_fun', 'language',
                    'feed_buffer_size'):
            params.pop(key)

        data = {'map': map_fun}
        if red_fun is not None:
            data['reduce'] = red_fun
        if language is not None:
            data['language'] = language

        view = self.view_class(self.resource('_temp_view'))
        return (yield from view.request(auth=auth,
                                        feed_buffer_size=feed_buffer_size,
                                        data=data,
                                        params=params))

    @asyncio.coroutine
    def view_cleanup(self, *, auth=None):
        """:ref:`Removes outdated views <api/db/view_cleanup>` index files.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_view_cleanup', auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())


class Security(object):
    """Provides set of methods to work with :ref:`database security API
    <api/db/security>`. Should be used via :attr:`database.security
    <aiocouchdb.database.Database.security>` property."""

    def __init__(self, resource):
        self.resource = resource('_security')

    @asyncio.coroutine
    def get(self, *, auth=None):
        """`Returns database security object`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Returns database security object: http://docs.couchdb.org/en/latest/api/database/security.html#get--db-_security
        """
        resp = yield from self.resource.get(auth=auth)
        yield from resp.maybe_raise_error()
        secobj = (yield from resp.json())
        if not secobj:
            secobj = {
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    'names': [],
                    'roles': []
                }
            }
        return secobj

    @asyncio.coroutine
    def update(self, *, auth=None, admins=None, members=None, merge=False):
        """`Updates database security object`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param dict admins: Mapping of administrators users/roles
        :param dict members: Mapping of members users/roles
        :param bool merge: Merges admins/members mappings with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict

        .. _Updates database security object: http://docs.couchdb.org/en/latest/api/database/security.html#put--db-_security
        """
        secobj = yield from self.get(auth=auth)
        for role, section in [('admins', admins), ('members', members)]:
            if section is None:
                continue
            if merge:
                for key, group in section.items():
                    items = secobj[role][key]
                    for item in group:
                        if item in items:
                            continue
                        items.append(item)
            else:
                secobj[role].update(section)
        resp = yield from self.resource.put(auth=auth, data=secobj)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    def update_admins(self, *, auth=None, names=None, roles=None, merge=False):
        """Helper for :meth:`~aiocouchdb.database.Security.update` method to
        update only database administrators leaving members as is.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param list names: List of user names
        :param list roles: List of role names
        :param bool merge: Merges user/role lists with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict
        """
        admins = {
            'names': [] if names is None else names,
            'roles': [] if roles is None else roles
        }
        return self.update(auth=auth, admins=admins, merge=merge)

    def update_members(self, *, auth=None, names=None, roles=None, merge=False):
        """Helper for :meth:`~aiocouchdb.database.Security.update` method to
        update only database members leaving administrators as is.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param list names: List of user names
        :param list roles: List of role names
        :param bool merge: Merges user/role lists with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict
        """
        members = {
            'names': [] if names is None else names,
            'roles': [] if roles is None else roles
        }
        return self.update(auth=auth, members=members, merge=merge)
