from __future__ import annotations

import http.client
import json
import socket
import unittest
from pathlib import Path

BACKEND_HOST = '127.0.0.1'
BACKEND_PORT = 8000
BOUNDARY = '----TSGenSmokeBoundary'
EXAMPLE_CSV = Path(__file__).with_name('example.csv')


def backend_is_available() -> bool:
    try:
        with socket.create_connection((BACKEND_HOST, BACKEND_PORT), timeout=1):
            return True
    except OSError:
        return False


class GenerateApiSmokeTests(unittest.TestCase):
    def test_generate_endpoint_accepts_example_csv(self) -> None:
        if not backend_is_available():
            self.skipTest('Backend is not running on 127.0.0.1:8000')

        file_content = EXAMPLE_CSV.read_text(encoding='utf-8')
        target_json = json.dumps({'customerName': '', 'amount': 0, 'createdAt': ''}, ensure_ascii=False)

        body = [
            f'--{BOUNDARY}',
            'Content-Disposition: form-data; name="file"; filename="example.csv"',
            'Content-Type: text/csv',
            '',
            file_content,
            f'--{BOUNDARY}',
            'Content-Disposition: form-data; name="target_json"',
            '',
            target_json,
            f'--{BOUNDARY}',
            'Content-Disposition: form-data; name="user_id"',
            '',
            'test-user',
            f'--{BOUNDARY}--',
            '',
        ]
        body_bytes = '\r\n'.join(body).encode('utf-8')
        headers = {
            'Content-Type': f'multipart/form-data; boundary={BOUNDARY}',
            'Content-Length': str(len(body_bytes)),
        }

        connection = http.client.HTTPConnection(BACKEND_HOST, BACKEND_PORT, timeout=10)
        try:
            connection.request('POST', '/api/generate', body_bytes, headers)
            response = connection.getresponse()
            payload = json.loads(response.read().decode('utf-8'))
        finally:
            connection.close()

        self.assertEqual(response.status, 200)
        self.assertEqual(payload['mode'], 'authorized')
        self.assertIn('generated_typescript', payload)
        self.assertIn('preview', payload)
        self.assertIn('warnings', payload)


if __name__ == '__main__':
    unittest.main()
