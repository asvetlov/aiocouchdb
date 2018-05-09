# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

"""
Exception hierarchy
-------------------

.. code::

  BaseException
  +-- Exception
      +-- aiohttp.errors.HttpProcessingError
          +-- aiocouchdb.errors.HttpErrorException
              +-- aiocouchdb.errors.BadRequest
              +-- aiocouchdb.errors.Unauthorized
              +-- aiocouchdb.errors.Forbidden
              +-- aiocouchdb.errors.ResourceNotFound
              +-- aiocouchdb.errors.MethodNotAllowed
              +-- aiocouchdb.errors.ResourceConflict
              +-- aiocouchdb.errors.PreconditionFailed
              +-- aiocouchdb.errors.RequestedRangeNotSatisfiable
              +-- aiocouchdb.errors.ServerError
"""


import asyncio
import aiohttp.errors


__all__ = (
    'HttpErrorException',
    'BadRequest',
    'Unauthorized',
    'Forbidden',
    'ResourceNotFound',
    'MethodNotAllowed',
    'ResourceConflict',
    'PreconditionFailed',
    'RequestedRangeNotSatisfiable',
    'ServerError',
    'maybe_raise_error'
)


class HttpErrorException(aiohttp.client_exceptions.ClientResponseError):
    """Extension of :exc:`aiohttp.errors.HttpErrorException` for CouchDB related
    errors."""

    error = ''
    reason = ''

    def __init__(self, error, reason, headers=None):
        self.error = error
        self.message = reason
        self.headers = headers

    def __str__(self):
        return '[{}] {}'.format(self.error or 'unknown_error', self.reason)


class BadRequest(HttpErrorException):
    """The request could not be understood by the server due to malformed
    syntax."""

    code = 400


class Unauthorized(HttpErrorException):
    """The request requires user authentication."""

    code = 401


class Forbidden(HttpErrorException):
    """The server understood the request, but is refusing to fulfill it."""

    code = 403


class ResourceNotFound(HttpErrorException):
    """The server has not found anything matching the Request-URI."""

    code = 404


class MethodNotAllowed(HttpErrorException):
    """The method specified in the Request-Line is not allowed for
    the resource identified by the Request-URI."""

    code = 405


class ResourceConflict(HttpErrorException):
    """The request could not be completed due to a conflict with the current
    state of the resource."""

    code = 409


class PreconditionFailed(HttpErrorException):
    """The precondition given in one or more of the Request-Header fields
    evaluated to false when it was tested on the server."""

    code = 412


class RequestedRangeNotSatisfiable(HttpErrorException):
    """The client has asked for a portion of the file, but the server
    cannot supply that portion."""

    code = 416


class ServerError(HttpErrorException):
    """The server encountered an unexpected condition which prevented it from
    fulfilling the request."""

    code = 500


HTTP_ERROR_BY_CODE = {
    err.code: err
    for err in HttpErrorException.__subclasses__()  # pylint: disable=no-member
}


@asyncio.coroutine
def maybe_raise_error(resp):
    """Raises :exc:`aiohttp.errors.HttpErrorException` exception in case of
    ``>=400`` response status code."""
    if resp.status < 400:
        return
    exc_cls = HTTP_ERROR_BY_CODE[resp.status]
    data = yield from resp.json()
    if isinstance(data, dict):
        error, reason = data.get('error', ''), data.get('reason', '')
        exc = exc_cls(error, reason, resp.headers)
    else:
        exc = exc_cls('', data, resp.headers)
    raise exc
