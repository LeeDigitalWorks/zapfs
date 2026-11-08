#!/usr/bin/env python3
"""Black-box S3 protocol smoke test for the local Rust server.

Run from the repository root after building the binary:
    python3 rust/tests/protocol.py

The test uses synthetic credentials and a temporary data directory. It has no
third-party Python dependencies.
"""

import hashlib
import hmac
import http.client
import os
from pathlib import Path
import re
import selectors
import subprocess
import tempfile
import time
import unittest
from datetime import datetime, timezone
from urllib.parse import quote
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BINARY = ROOT / "target" / "debug" / "zapfs-rs"
ACCESS_KEY = "smoke-access-key"
SECRET_KEY = "smoke-secret-key-for-sigv4"
REGION = "us-east-1"
SERVICE = "s3"


def _hmac(key, value):
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()


def _canonical_query(raw_query):
    # Query strings are already encoded at the call site. Preserve each encoded
    # value byte-for-byte while sorting parameter pairs as SigV4 requires.
    if not raw_query:
        return ""
    return "&".join(sorted(raw_query.split("&")))


class S3Client:
    def __init__(self, endpoint, access_key=ACCESS_KEY, secret_key=SECRET_KEY):
        match = re.fullmatch(r"http://(127\.0\.0\.1):(\d+)", endpoint)
        if not match:
            raise AssertionError("unexpected local endpoint: %r" % endpoint)
        self.host, self.port = match.group(1), int(match.group(2))
        self.access_key, self.secret_key = access_key, secret_key

    @staticmethod
    def path(bucket=None, key=None):
        path = "/"
        if bucket is not None:
            path += quote(bucket, safe="-_.~")
        if key is not None:
            path += "/" + quote(key, safe="/-_.~")
        return path

    @staticmethod
    def query(**params):
        # Keep empty values for S3 subresources such as ?uploads and
        # ?versioning. Encode UTF-8 and reserved characters before signing.
        return "&".join(
            quote(str(name), safe="-_.~") + "=" + quote(str(value), safe="-_.~")
            for name, value in params.items()
        )

    def request(self, method, path="/", query="", body=b"", headers=None,
                signed=True, payload_hash=None, corrupt_signature=False):
        headers = dict(headers or {})
        body = body or b""
        if signed:
            now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            date = now[:8]
            payload_hash = payload_hash or hashlib.sha256(body).hexdigest()
            headers["Host"] = "%s:%d" % (self.host, self.port)
            headers["x-amz-date"] = now
            headers["x-amz-content-sha256"] = payload_hash

            canonical_headers = {}
            for name, value in headers.items():
                lower = name.lower().strip()
                # Header values are normalized exactly as SigV4 requires.
                canonical_headers[lower] = " ".join(str(value).strip().split())
            signed_names = ";".join(sorted(canonical_headers))
            header_block = "".join(
                name + ":" + canonical_headers[name] + "\n"
                for name in sorted(canonical_headers)
            )
            # s3s includes a blank line between canonical and signed headers.
            canonical_request = (
                method + "\n" + path + "\n" + _canonical_query(query) + "\n"
                + header_block + "\n" + signed_names + "\n" + payload_hash
            )
            scope = "%s/%s/%s/aws4_request" % (date, REGION, SERVICE)
            to_sign = "\n".join((
                "AWS4-HMAC-SHA256", now, scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ))
            key = _hmac(("AWS4" + self.secret_key).encode("utf-8"), date)
            key = _hmac(key, REGION)
            key = _hmac(key, SERVICE)
            key = _hmac(key, "aws4_request")
            signature = hmac.new(key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
            if corrupt_signature:
                signature = ("0" if signature[0] != "0" else "1") + signature[1:]
            headers["Authorization"] = (
                "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                % (self.access_key, scope, signed_names, signature)
            )

        target = path + ("?" + query if query else "")
        connection = http.client.HTTPConnection(self.host, self.port, timeout=5)
        try:
            connection.request(method, target, body=body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            response_headers = {name.lower(): value for name, value in response.getheaders()}
            return response.status, response_headers, response_body
        finally:
            connection.close()


def _local_name(tag):
    return tag.rsplit("}", 1)[-1]


def _xml_child_text(root, name):
    for element in root.iter():
        if _local_name(element.tag) == name:
            return element.text or ""
    raise AssertionError("XML element %r missing" % name)


class ProtocolSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        binary = Path(os.environ.get("ZAPFS_RS_BINARY", str(DEFAULT_BINARY))).resolve()
        if not binary.is_file():
            raise unittest.SkipTest("Rust binary not found; build it first: %s" % binary)
        cls.tempdir = tempfile.TemporaryDirectory(prefix="zapfs-rs-protocol-")
        cls.data_dir = Path(cls.tempdir.name) / "data"
        cls.data_dir.mkdir()
        cls.binary = binary
        cls.process = None
        cls.client = None
        cls.start_server()

    @classmethod
    def tearDownClass(cls):
        cls.stop_server()
        if hasattr(cls, "tempdir"):
            cls.tempdir.cleanup()

    @classmethod
    def start_server(cls):
        env = os.environ.copy()
        env["ZAPFS_ACCESS_KEY_ID"] = ACCESS_KEY
        env["ZAPFS_SECRET_ACCESS_KEY"] = SECRET_KEY
        cls.process = subprocess.Popen(
            [str(cls.binary), "--data-dir", str(cls.data_dir),
             "--listen", "127.0.0.1:0", "--admin-listen", "127.0.0.1:0"],
            cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        deadline = time.monotonic() + 10
        selector = selectors.DefaultSelector()
        selector.register(cls.process.stderr, selectors.EVENT_READ)
        try:
            while time.monotonic() < deadline:
                if cls.process.poll() is not None:
                    raise AssertionError("Rust server exited during startup")
                ready = selector.select(min(0.2, max(0, deadline - time.monotonic())))
                if not ready:
                    continue
                line = cls.process.stderr.readline()
                if not line:
                    continue
                match = re.search(r"S3 endpoint: (http://127\.0\.0\.1:\d+)", line)
                if match:
                    cls.client = S3Client(match.group(1))
                    return
            raise AssertionError("timed out waiting for S3 endpoint startup line")
        except BaseException:
            cls.stop_server()
            raise
        finally:
            selector.close()

    @classmethod
    def stop_server(cls):
        process = getattr(cls, "process", None)
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stderr:
            process.stderr.close()
        cls.process = None
        cls.client = None

    def s3(self, method, bucket=None, key=None, query="", **kwargs):
        return self.client.request(method, self.client.path(bucket, key), query, **kwargs)

    def assert_status(self, expected, actual):
        status, _headers, body = actual
        self.assertEqual(expected, status, body[:1000].decode("utf-8", "replace"))

    def test_protocol_and_restart_persistence(self):
        c = self.client
        status, _, body = c.request("GET")
        self.assert_status(200, (status, {}, body))
        root = ET.fromstring(body)
        self.assertEqual("ListAllMyBucketsResult", _local_name(root.tag))

        bucket = "protocol-smoke-bucket"
        self.assert_status(200, self.s3("PUT", bucket))
        self.assert_status(200, self.s3("HEAD", bucket))
        self.assert_status(409, self.s3("PUT", bucket))  # duplicate bucket

        # Basic object headers, byte range, and body round trip.
        key = "docs/readme.txt"
        payload = b"protocol smoke payload\n"
        headers = {"Content-Type": "text/plain; charset=utf-8", "x-amz-meta-color": "blue"}
        self.assert_status(200, self.s3("PUT", bucket, key, body=payload, headers=headers))
        status, head, _ = self.s3("HEAD", bucket, key)
        self.assert_status(200, (status, head, b""))
        self.assertEqual(b"", self.s3("HEAD", bucket, key)[2])
        self.assertEqual(str(len(payload)), head.get("content-length"))
        self.assertEqual("text/plain; charset=utf-8", head.get("content-type"))
        self.assertEqual("blue", head.get("x-amz-meta-color"))
        status, get_headers, got = self.s3("GET", bucket, key)
        self.assert_status(200, (status, get_headers, got))
        self.assertEqual(payload, got)
        status, range_headers, ranged = self.s3("GET", bucket, key, headers={"Range": "bytes=2-7"})
        self.assert_status(206, (status, range_headers, ranged))
        self.assertEqual(payload[2:8], ranged)
        self.assertEqual("bytes 2-7/%d" % len(payload), range_headers.get("content-range"))

        # Empty objects and difficult path/XML keys must remain literal keys.
        self.assert_status(200, self.s3("PUT", bucket, "empty", body=b""))
        status, empty_headers, empty = self.s3("GET", bucket, "empty")
        self.assert_status(200, (status, empty_headers, empty))
        self.assertEqual(b"", empty)
        self.assert_status(416, self.s3("GET", bucket, "empty", headers={"Range": "bytes=0-0"}))
        weird_key = "pfx/雪&x"
        self.assert_status(200, self.s3("PUT", bucket, weird_key, body=b"unicode"))
        traversal_key = "../literal"
        self.assert_status(200, self.s3("PUT", bucket, traversal_key, body=b"dot segments"))
        status, _, got = self.s3("GET", bucket, traversal_key)
        self.assert_status(200, (status, {}, got))
        self.assertEqual(b"dot segments", got)

        # V2 listing: encoded prefix/start-after parameters, pagination token,
        # and XML escaping of the Unicode and ampersand key.
        for name in ("pfx/a", "pfx/b", "pfx/c", "pfx/d"):
            self.assert_status(200, self.s3("PUT", bucket, name, body=name.encode()))
        query = S3Client.query(**{"list-type": 2, "prefix": "pfx/", "max-keys": 2})
        status, _, page1 = self.s3("GET", bucket, query=query)
        self.assert_status(200, (status, {}, page1))
        xml1 = ET.fromstring(page1)
        keys1 = [_xml_child_text(node, "Key") for node in xml1.iter() if _local_name(node.tag) == "Contents"]
        self.assertEqual(["pfx/a", "pfx/b"], keys1)
        self.assertEqual("true", _xml_child_text(xml1, "IsTruncated"))
        token = _xml_child_text(xml1, "NextContinuationToken")
        query2 = S3Client.query(**{
            "list-type": 2, "prefix": "pfx/", "max-keys": 2,
            "continuation-token": token,
        })
        status, _, page2 = self.s3("GET", bucket, query=query2)
        self.assert_status(200, (status, {}, page2))
        xml2 = ET.fromstring(page2)
        keys2 = [_xml_child_text(node, "Key") for node in xml2.iter() if _local_name(node.tag) == "Contents"]
        self.assertEqual(["pfx/c", "pfx/d"], keys2)
        query_after = S3Client.query(**{
            "list-type": 2, "prefix": "pfx/", "start-after": "pfx/a&", "max-keys": 10,
        })
        status, _, after_xml = self.s3("GET", bucket, query=query_after)
        self.assert_status(200, (status, {}, after_xml))
        after_root = ET.fromstring(after_xml)
        self.assertEqual("pfx/a&", _xml_child_text(after_root, "StartAfter"))
        listed = [_xml_child_text(node, "Key") for node in after_root.iter() if _local_name(node.tag) == "Contents"]
        self.assertIn(weird_key, listed)
        status, _, zero_xml = self.s3("GET", bucket, query=S3Client.query(**{"list-type": 2, "max-keys": 0}))
        self.assert_status(200, (status, {}, zero_xml))
        zero_root = ET.fromstring(zero_xml)
        self.assertEqual("0", _xml_child_text(zero_root, "KeyCount"))
        self.assertEqual([], [_xml_child_text(node, "Key") for node in zero_root.iter() if _local_name(node.tag) == "Contents"])

        # Delimiter pagination treats a common prefix as one entry. The first
        # lexical object immediately after "folders/group/" must appear on the
        # next page without repeating that common prefix.
        folder_keys = (
            "folders/group/child-a", "folders/group/child-b",
            "folders/group0", "folders/other/child",
        )
        for folder_key in folder_keys:
            self.assert_status(200, self.s3("PUT", bucket, folder_key, body=folder_key.encode()))
        folder_query = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "/", "max-keys": 1,
        })
        status, _, folder_page1 = self.s3("GET", bucket, query=folder_query)
        self.assert_status(200, (status, {}, folder_page1))
        folder_root1 = ET.fromstring(folder_page1)
        self.assertEqual(["folders/group/"], [
            _xml_child_text(node, "Prefix") for node in folder_root1.iter()
            if _local_name(node.tag) == "CommonPrefixes"
        ])
        self.assertEqual([], [
            _xml_child_text(node, "Key") for node in folder_root1.iter()
            if _local_name(node.tag) == "Contents"
        ])
        folder_token1 = _xml_child_text(folder_root1, "NextContinuationToken")
        self.assertEqual("true", _xml_child_text(folder_root1, "IsTruncated"))

        folder_query2 = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "/", "max-keys": 1,
            "continuation-token": folder_token1,
        })
        status, _, folder_page2 = self.s3("GET", bucket, query=folder_query2)
        self.assert_status(200, (status, {}, folder_page2))
        folder_root2 = ET.fromstring(folder_page2)
        self.assertEqual(["folders/group0"], [
            _xml_child_text(node, "Key") for node in folder_root2.iter()
            if _local_name(node.tag) == "Contents"
        ])
        self.assertEqual([], [
            _xml_child_text(node, "Prefix") for node in folder_root2.iter()
            if _local_name(node.tag) == "CommonPrefixes"
        ])
        folder_token2 = _xml_child_text(folder_root2, "NextContinuationToken")
        folder_query3 = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "/", "max-keys": 1,
            "continuation-token": folder_token2,
        })
        status, _, folder_page3 = self.s3("GET", bucket, query=folder_query3)
        self.assert_status(200, (status, {}, folder_page3))
        folder_root3 = ET.fromstring(folder_page3)
        self.assertEqual(["folders/other/"], [
            _xml_child_text(node, "Prefix") for node in folder_root3.iter()
            if _local_name(node.tag) == "CommonPrefixes"
        ])

        # A start-after key within a grouped prefix excludes that whole group.
        after_group_query = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "/",
            "start-after": "folders/group/child-a", "max-keys": 10,
        })
        status, _, after_group_xml = self.s3("GET", bucket, query=after_group_query)
        self.assert_status(200, (status, {}, after_group_xml))
        after_group_root = ET.fromstring(after_group_xml)
        after_group_prefixes = [
            _xml_child_text(node, "Prefix") for node in after_group_root.iter()
            if _local_name(node.tag) == "CommonPrefixes"
        ]
        self.assertNotIn("folders/group/", after_group_prefixes)

        # Continuation tokens are authenticated and bound to the full listing
        # identity, including bucket, prefix, and delimiter.
        self.assert_status(200, self.s3("PUT", "protocol-smoke-other-bucket"))
        changed_prefix = S3Client.query(**{
            "list-type": 2, "prefix": "folders/g", "delimiter": "/", "max-keys": 1,
            "continuation-token": folder_token1,
        })
        self.assert_status(400, self.s3("GET", bucket, query=changed_prefix))
        changed_delimiter = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "!", "max-keys": 1,
            "continuation-token": folder_token1,
        })
        self.assert_status(400, self.s3("GET", bucket, query=changed_delimiter))
        changed_bucket = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "/", "max-keys": 1,
            "continuation-token": folder_token1,
        })
        self.assert_status(400, self.s3("GET", "protocol-smoke-other-bucket", query=changed_bucket))
        changed_token = ("A" if folder_token1[0] != "A" else "B") + folder_token1[1:]
        tampered_query = S3Client.query(**{
            "list-type": 2, "prefix": "folders/", "delimiter": "/", "max-keys": 1,
            "continuation-token": changed_token,
        })
        self.assert_status(400, self.s3("GET", bucket, query=tampered_query))

        # Unsupported features fail explicitly before replacing existing data.
        self.assert_status(501, self.s3("GET", bucket, query="versioning="))
        self.assert_status(501, self.s3("POST", bucket, query="uploads="))
        self.assert_status(501, self.s3("PUT", bucket, key, body=b"should not replace",
                                       headers={"x-amz-tagging": "k=v"}))
        status, _, got = self.s3("GET", bucket, key)
        self.assert_status(200, (status, {}, got))
        self.assertEqual(payload, got)

        # Invalid checksums must reject before the previous object is committed.
        bad_md5 = {"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="}
        self.assert_status(400, self.s3("PUT", bucket, key, body=b"bad md5", headers=bad_md5))
        status, _, got = self.s3("GET", bucket, key)
        self.assert_status(200, (status, {}, got))
        self.assertEqual(payload, got)
        incorrect_hash = hashlib.sha256(b"declared payload").hexdigest()
        self.assert_status(400, c.request("PUT", c.path(bucket, key), body=b"different payload",
                                          payload_hash=incorrect_hash))
        status, _, got = self.s3("GET", bucket, key)
        self.assert_status(200, (status, {}, got))
        self.assertEqual(payload, got)

        self.assert_status(404, self.s3("GET", bucket, "missing-object"))
        self.assert_status(403, c.request("GET", c.path(bucket), signed=False))
        self.assert_status(403, c.request("GET", c.path(bucket), corrupt_signature=True))
        self.assert_status(409, self.s3("DELETE", bucket))  # bucket still has objects

        # Delete one object, restart against the same directory, and ensure the
        # current generation is durable and overwriting did not lose its value.
        self.assert_status(204, self.s3("DELETE", bucket, "empty"))
        replacement = b"persisted overwrite"
        self.assert_status(200, self.s3("PUT", bucket, key, body=replacement, headers=headers))
        self.stop_server()
        self.start_server()
        status, persisted_headers, persisted = self.s3("GET", bucket, key)
        self.assert_status(200, (status, persisted_headers, persisted))
        self.assertEqual(replacement, persisted)
        self.assertEqual("blue", persisted_headers.get("x-amz-meta-color"))
        for leftover in (key, "empty", weird_key, traversal_key,
                         "pfx/a", "pfx/b", "pfx/c", "pfx/d", *folder_keys):
            self.assert_status(204, self.s3("DELETE", bucket, leftover))
        self.assert_status(204, self.s3("DELETE", bucket))
        self.assert_status(204, self.s3("DELETE", "protocol-smoke-other-bucket"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
