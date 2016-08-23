# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiocouchdb.feeds

from . import utils


class FeedTestCase(utils.TestCase):

    def test_read_chunks(self):
        resp = self.prepare_response(data=[b'foo\r\n', b'bar\r\n'])

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(b'foo\r\n', result)

        result = yield from feed.next()
        self.assertEqual(b'bar\r\n', result)

        result = yield from feed.next()
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_ignore_empty_chunks(self):
        resp = self.prepare_response(data=[
            b'foo\r\n', b'\n',  b'\n',
            b'\n',  b'\n', b'bar\r\n'
        ])

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(b'foo\r\n', result)

        result = yield from feed.next()
        self.assertEqual(b'bar\r\n', result)

        result = yield from feed.next()
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_empty_json_feed(self):
        resp = self.prepare_response()

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(None, result)

        self.assertFalse(feed.is_active())

    def test_calling_inactive_feed_returns_none(self):
        resp = self.prepare_response()

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        yield from feed.next()

        self.assertFalse(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(None, result)

    def test_close_resp_on_feed_end(self):
        resp = self.prepare_response()

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        yield from feed.next()

        self.assertFalse(feed.is_active())
        self.assertTrue(resp.close.called)

    def test_force_close_resp_on_error(self):
        resp = self.prepare_response(err=ValueError)

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        with self.assertRaises(ValueError):
            yield from feed.next()

        self.assertFalse(feed.is_active())
        resp.close.assert_called_with(force=True)

    def test_buffer_workflow(self):
        resp = self.prepare_response(data=[
            b'foo\r\n', b'bar\r\n',
            b'baz\r\n', b'boo\r\n'
        ])

        buf_size = 2
        feed = aiocouchdb.feeds.Feed(resp, buffer_size=buf_size, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(b'foo\r\n', result)

        result = yield from feed.next()
        self.assertEqual(b'bar\r\n', result)

        result = yield from feed.next()
        self.assertEqual(feed._queue.qsize(), buf_size - 1)
        self.assertEqual(b'baz\r\n', result)

        result = yield from feed.next()
        self.assertEqual(b'boo\r\n', result)

        result = yield from feed.next()
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())


class JsonFeedTestCase(utils.TestCase):

    def test_read_json_chunks(self):
        resp = self.prepare_response(data=[
            b'{"foo": true}\r\n',
            b'"foo"\r\n',
            b'{"bar": null}\r\n'
        ])

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'foo': True}, result)

        result = yield from feed.next()
        self.assertEqual('foo', result)

        result = yield from feed.next()
        self.assertEqual({'bar': None}, result)

        result = yield from feed.next()
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())


class ViewFeedTestCase(utils.TestCase):

    def test_read_empty_view(self):
        resp = self.prepare_response(data=[
            b'{"total_rows": 0, "offset": 0, "rows": [\r\n',
            b'\n',
            b']}\r\n'
        ])

        feed = aiocouchdb.feeds.ViewFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_reduced_empty_view(self):
        resp = self.prepare_response(data=[
            b'{"rows": [\r\n',
            b'\r\n]}'
        ])

        feed = aiocouchdb.feeds.ViewFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_view(self):
        resp = self.prepare_response(data=[
            b'{"total_rows": 3, "offset": 0, "rows": [\r\n',
            b'{"id": "foo", "key": null, "value": false}',
            b',\r\n{"id": "bar", "key": null, "value": false}',
            b',\r\n{"id": "baz", "key": null, "value": false}',
            b'\r\n]}'
        ])

        feed = aiocouchdb.feeds.ViewFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        for idx in ('foo', 'bar', 'baz'):
            row = yield from feed.next()
            self.assertEqual({'id': idx, 'key': None, 'value': False}, row)
        row = yield from feed.next()
        self.assertEqual(None, row)
        self.assertFalse(feed.is_active())

    def test_view_header(self):
        resp = self.prepare_response(data=[
            b'{"total_rows": 3, "offset": 0, "rows": [\r\n',
            b'{"id": "foo", "key": null, "value": false}',
            b',\r\n{"id": "bar", "key": null, "value": false}',
            b',\r\n{"id": "baz", "key": null, "value": false}',
            b'\r\n]}'
        ])

        feed = aiocouchdb.feeds.ViewFeed(resp, loop=self.loop)

        self.assertIsNone(feed.total_rows)
        self.assertIsNone(feed.offset)
        self.assertIsNone(feed.update_seq)

        yield from feed.next()

        self.assertEqual(3, feed.total_rows)
        self.assertEqual(0, feed.offset)
        self.assertIsNone(feed.update_seq)

    def test_reduced_view_header(self):
        resp = self.prepare_response(data=[
            b'{"rows": [\r\n',
            b'{"key": null, "value": 1}',
            b',\r\n{"key": true, "value": 2}',
            b'\r\n]}'
        ])

        feed = aiocouchdb.feeds.ViewFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        self.assertIsNone(feed.total_rows)
        self.assertIsNone(feed.offset)
        self.assertIsNone(feed.update_seq)

        yield from feed.next()

        self.assertIsNone(feed.total_rows)
        self.assertIsNone(feed.offset)
        self.assertIsNone(feed.update_seq)


class EventSourceFeedTestCase(utils.TestCase):

    def test_read_event(self):
        resp = self.prepare_response(data=[
            b'data: {"type":"updated","db_name":"db"}\n\n',
        ])
        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'data': {'db_name': 'db', 'type': 'updated'}}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())

    def test_read_empty_data(self):
        resp = self.prepare_response(data=[
            b'event: heartbeat\ndata: \n\n'
        ])

        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'event': 'heartbeat', 'data': None}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())

    def test_read_multiple_data(self):
        resp = self.prepare_response(data=[
            b'data: [\ndata:"foo",\ndata: "bar"\ndata:] \n\n'
        ])

        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'data': ['foo', 'bar']}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())

    def test_no_colon(self):
        resp = self.prepare_response(data=[
            b'id\n\n'
        ])

        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'id': '', 'data': None}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())

    def test_leading_colon(self):
        resp = self.prepare_response(data=[
            b':id\n\n'
        ])

        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'data': None}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())

    def test_decode_retry_with_int(self):
        resp = self.prepare_response(data=[
            b'retry: 10\n\n'
        ])

        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'retry': 10, 'data': None}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())

    def test_ignore_unknown_field(self):
        resp = self.prepare_response(data=[
            b'data: "foo"\nfoo: bar\n\n'
        ])

        feed = aiocouchdb.feeds.EventSourceFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = yield from feed.next()
        self.assertEqual({'data': 'foo'}, result)

        result = yield from feed.next()
        self.assertIsNone(result)
        self.assertFalse(feed.is_active())


class ChangesFeedTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.output = [
            {'seq': 77, 'id': 'foo', 'changes': [{'rev': '9-CDE'}]},
            {'seq': 90, 'id': 'bar', 'changes': [{'rev': '12-ABC'}]},
            {'seq': 91, 'id': 'baz', 'changes': [{'rev': '11-EFG'}],
             'deleted': True}
        ]

    def test_read_changes(self):
        resp = self.prepare_response(data=[
            b'{"results":[\n',
            b'{"seq":77,"id":"foo","changes":[{"rev":"9-CDE"}]}',
            b',\n{"seq":90,"id":"bar","changes":[{"rev":"12-ABC"}]}',
            b',\n{"seq":91,"id":"baz","changes":[{"rev":"11-EFG"}],'
            b'"deleted":true}',
            b'\n],\n"last_seq":91}\n'
        ])
        feed = aiocouchdb.feeds.ChangesFeed(resp, loop=self.loop)
        yield from self.check_feed_output(feed, self.output)

    def test_read_changes_longpoll(self):
        resp = self.prepare_response(data=[
            b'{"results":[\n',
            b'{"seq":77,"id":"foo","changes":[{"rev":"9-CDE"}]}',
            b',\n{"seq":90,"id":"bar","changes":[{"rev":"12-ABC"}]}',
            b',\n{"seq":91,"id":"baz","changes":[{"rev":"11-EFG"}],'
            b'"deleted":true}',
            b'\n],\n"last_seq":91}\n'
        ])
        feed = aiocouchdb.feeds.LongPollChangesFeed(resp, loop=self.loop)
        yield from self.check_feed_output(feed, self.output)

    def test_read_changes_continuous(self):
        resp = self.prepare_response(data=[
            b'{"seq":77,"id":"foo","changes":[{"rev":"9-CDE"}]}\n',
            b'{"seq":90,"id":"bar","changes":[{"rev":"12-ABC"}]}\n',
            b'{"seq":91,"id":"baz","changes":[{"rev":"11-EFG"}],'
            b'"deleted":true}\n',
            b'{"last_seq":91}\n',
        ])
        feed = aiocouchdb.feeds.ContinuousChangesFeed(resp, loop=self.loop)
        yield from self.check_feed_output(feed, self.output)

    def test_read_changes_eventsource(self):
        resp = self.prepare_response(data=[
            b'data: {"seq":77,"id":"foo","changes":[{"rev":"9-CDE"}]}\n'
            b'id: 77\n\n',
            b'event: heartbeat\ndata: \n\n',
            b'data: {"seq":90,"id":"bar","changes":[{"rev":"12-ABC"}]}\n'
            b'id: 90\n\n',
            b'data: {"seq":91,"id":"baz","changes":[{"rev":"11-EFG"}],'
            b'"deleted":true}\nid: 91\n\n',
        ])
        feed = aiocouchdb.feeds.EventSourceChangesFeed(resp, loop=self.loop)
        yield from self.check_feed_output(feed, self.output)

    @asyncio.coroutine
    def check_feed_output(self, feed, output):
        self.assertTrue(feed.is_active())
        self.assertIsNone(feed.last_seq)
        for expected in output:
            event = yield from feed.next()
            self.assertEqual(expected, event)
            self.assertEqual(expected['seq'], feed.last_seq)
        event = yield from feed.next()
        self.assertIsNone(event)
        self.assertFalse(feed.is_active())
        self.assertIsNotNone(feed.last_seq)
