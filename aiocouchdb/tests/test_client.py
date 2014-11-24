# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import io
import types
import unittest.mock as mock

import aiocouchdb.authn
import aiocouchdb.client

from . import utils


class ResourceTestCase(utils.TestCase):

    target = 'mock'

    def test_head_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.head()
        self.assert_request_called_with('HEAD')

    def test_get_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.get()
        self.assert_request_called_with('GET')

    def test_post_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.post()
        self.assert_request_called_with('POST')

    def test_put_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.put()
        self.assert_request_called_with('PUT')

    def test_delete_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.delete()
        self.assert_request_called_with('DELETE')

    def test_copy_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.copy()
        self.assert_request_called_with('COPY')

    def test_options_request(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.options()
        self.assert_request_called_with('OPTIONS')

    def test_to_str(self):
        self.assertEqual("<Resource @ '%s'>" % self.url,
                         str(aiocouchdb.client.Resource(self.url)))

    def test_on_call(self):
        res = aiocouchdb.client.Resource(self.url)
        new_res = res('foo', 'bar/baz')
        self.assertIsNot(res, new_res)
        self.assertEqual('http://localhost:5984/foo/bar%2Fbaz', new_res.url)

    def test_empty_call(self):
        res = aiocouchdb.client.Resource(self.url)
        new_res = res()
        self.assertIsNot(res, new_res)
        self.assertEqual('http://localhost:5984', new_res.url)

    def test_request_with_path(self):
        res = aiocouchdb.client.Resource(self.url)
        yield from res.request('get', 'foo/bar')
        self.assert_request_called_with('get', 'foo/bar')

    def test_dont_sign_request_none_auth(self):
        res = aiocouchdb.client.Resource(self.url)
        res.apply_auth = mock.Mock()
        yield from res.request('get')
        self.assertFalse(res.apply_auth.called)

    def test_dont_update_none_auth(self):
        res = aiocouchdb.client.Resource(self.url)
        res.update_auth = mock.Mock()
        yield from res.request('get')
        self.assertFalse(res.update_auth.called)

    def test_sign_request(self):
        res = aiocouchdb.client.Resource(self.url)
        auth = mock.Mock(spec=aiocouchdb.authn.AuthProvider)
        yield from res.request('get', auth=auth)
        self.assertTrue(auth.sign.called)

    def test_update_auth(self):
        res = aiocouchdb.client.Resource(self.url)
        auth = mock.Mock(spec=aiocouchdb.authn.AuthProvider)
        yield from res.request('get', auth=auth)
        self.assertTrue(auth.update.called)

    def test_override_request_class(self):
        class Thing(object):
            pass
        res = aiocouchdb.client.Resource(self.url)
        yield from res.request('get', request_class=Thing)
        self.assert_request_called_with('get', request_class=Thing)

    def test_override_response_class(self):
        class Thing(object):
            pass
        res = aiocouchdb.client.Resource(self.url)
        yield from res.request('get', response_class=Thing)
        self.assert_request_called_with('get', response_class=Thing)


class HttpRequestTestCase(utils.TestCase):

    target = 'mock'

    def test_encode_json_body(self):
        req = aiocouchdb.client.HttpRequest('post', self.url,
                                            data={'foo': 'bar'})
        self.assertEqual(b'{"foo": "bar"}', req.body)

    def test_correct_encode_boolean_params(self):
        req = aiocouchdb.client.HttpRequest('get', self.url,
                                            params={'foo': True})
        self.assertEqual('/?foo=true', req.path)

        req = aiocouchdb.client.HttpRequest('get', self.url,
                                            params={'bar': False})
        self.assertEqual('/?bar=false', req.path)

    def test_encode_chunked_json_body(self):
        req = aiocouchdb.client.HttpRequest(
            'post', self.url, data=('{"foo": "bar"}' for _ in [0]))
        self.assertIsInstance(req.body, types.GeneratorType)

    def test_encode_readable_object(self):
        req = aiocouchdb.client.HttpRequest(
            'post', self.url, data=io.BytesIO(b'foobarbaz'))
        self.assertIsInstance(req.body, io.IOBase)


class HttpResponseTestCase(utils.TestCase):

    target = 'mock'

    def test_read_body(self):
        with self.response(data=b'{"couchdb": "Welcome!"}') as resp:
            result = yield from resp.read()
        self.assertEqual(b'{"couchdb": "Welcome!"}', result)

    def test_decode_json_body(self):
        with self.response(data=b'{"couchdb": "Welcome!"}') as resp:
            result = yield from resp.json()
        self.assertEqual({'couchdb': 'Welcome!'}, result)

    def test_decode_json_from_empty_body(self):
        with self.response(data=b'') as resp:
            result = yield from resp.json()
        self.assertEqual(None, result)
