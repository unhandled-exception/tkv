#!/usr/bin/env python3

import json
import unittest
import uuid

import requests


class TestTarantoolKVStorageAPI(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:8080/kv'

    def test_base_flow(self):
        key = str(uuid.uuid4())
        value = {'key1': 'value1', 'data': {'key2': 'value2'}}
        value2 = {'key2': 'value4'}

        key_url = '{}/{}'.format(self.url, key)

        # Создаем новый ключ
        r = requests.post(self.url, json=dict(key=key, value=value))
        self.assertEqual(r.status_code, 200)
        self.assertDictEqual(r.json(), dict(key=key, value=value))

        # Пытаемся создать уже существующий ключ
        r = requests.post(self.url, json=dict(key=key, value=value))
        self.assertEqual(r.status_code, 409)

        # Получаем данные по ключу
        r = requests.get(key_url)
        self.assertEqual(r.status_code, 200)
        self.assertDictEqual(r.json(), value)

        # Обновляем значение ключа
        r = requests.put(key_url, json=dict(value=value2))
        self.assertEqual(r.status_code, 200)
        self.assertDictEqual(r.json(), {'status': 'ok'})

        # Проеряем, что записали правильный ключ
        r = requests.get(key_url)
        self.assertEqual(r.status_code, 200)
        self.assertDictEqual(r.json(), value2)

        # Удаляем ключ
        r = requests.delete(key_url)
        self.assertEqual(r.status_code, 200)
        self.assertDictEqual(r.json(), {'status': 'ok'})

        # Проеряем, что ключ удалили
        r = requests.get(key_url)
        self.assertEqual(r.status_code, 404)

    def test_unknown_keys(self):
        unknown_key = str(uuid.uuid4())

        r = requests.get('{}/{}'.format(self.url, unknown_key))
        self.assertEqual(r.status_code, 404)

        r = requests.put('{}/{}'.format(self.url, unknown_key), json=dict(value=''))
        self.assertEqual(r.status_code, 404)

        r = requests.delete('{}/{}'.format(self.url, unknown_key))
        self.assertEqual(r.status_code, 404)

    def test_bad_post_requests(self):
        key = str(uuid.uuid4())
        bad_json = [
            '',
            'simple text',
            json.dumps('value'),
            json.dumps(dict(value='value')),
            json.dumps(dict(key='key')),
            json.dumps(dict(key={'name': 'test'}, value='test')),
            json.dumps(dict(key=1234, value='test')),
        ]
        for q in bad_json:
            r = requests.post(self.url, data=q)
            self.assertEqual(r.status_code, 400, q)

    def test_bad_put_requests(self):
        key = str(uuid.uuid4())
        requests.post(self.url, json=dict(key=key, value='value'))
        key_url = '{}/{}'.format(self.url, key)

        bad_json = [
            '',
            'simple text',
            json.dumps('value'),
            json.dumps(dict(key='key')),
        ]
        for q in bad_json:
            r = requests.put(key_url, data=q)
            self.assertEqual(r.status_code, 400, q)


if __name__ == "__main__":
    unittest.main()
