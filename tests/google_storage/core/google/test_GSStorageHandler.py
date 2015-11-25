import os
import tempfile
import urllib

import pytest

import google_storage.core.utils as gh


TEST_BUCKET_NAME = u"pi-test-bucket"


# @pytest.mark.slow
# def test_delete_bucket(test_bucket, google_auth_key_path):
#     gs = gh.GSStorageHandler(google_auth_key_path)
#     gs.delete_bucket(TEST_BUCKET_NAME, delete_content=True)
#     assert not gs.bucket_exists(TEST_BUCKET_NAME)


# @pytest.mark.slow
# def test_delete_bucket_full(test_bucket_with_content, google_auth_key_path):
#     gs = gh.GSStorageHandler(google_auth_key_path)
#     gs.delete_bucket(TEST_BUCKET_NAME, delete_content=True)
#     assert not gs.bucket_exists(TEST_BUCKET_NAME)


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            {
                u'kind': u'storage#bucket',
                u'name': TEST_BUCKET_NAME,
                # u'timeCreated': u'2015-07-08T13:44:19.837Z',
                u'location': u'EU',
                u'id': TEST_BUCKET_NAME,
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/%s' % (
                    TEST_BUCKET_NAME
                ),
                u'storageClass': u'STANDARD'
            }
        ),
    ]
)
def test_details(google_auth_key_path, test_bucket, bucket, expected):
    gs = gh.GSStorageHandler(google_auth_key_path)
    d = gs.details(bucket)
    del d[u'timeCreated']
    del d[u'etag']
    del d[u'metageneration']
    del d[u'updated']
    del d[u'owner']
    del d[u'projectNumber']

    assert d == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            True
        ),
        (
            'no-bucket',
            False
        ),
    ]
)
def test_bucket_exists(google_auth_key_path, test_bucket, bucket, expected):
    gs = gh.GSStorageHandler(google_auth_key_path)
    d = gs.bucket_exists(bucket)
    assert d == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            {
                u'kind': u'storage#object',
                u'contentType': u'text/plain',
                # u'md5Hash': u'1B2M2Y8AsgTpgAmY7PhCfg==',
                u'bucket': TEST_BUCKET_NAME,
                u'owner': {
                    u'entityId': u'00b4903a97a407ba9033c0aed75ab67fd5678df91e'
                    '74afae7263d75b70d3da11',
                    u'entity': u'user-00b4903a97a407ba9033c0aed75ab67fd5678df9'
                    '1e74afae7263d75b70d3da11'
                },
                # u'crc32c': u'AAAAAA==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/%s'
                '/o/%s' % (TEST_BUCKET_NAME, "%s"),
                u'size': u'26'
            }
        ),
    ]
)
def test_upload(
    google_auth_key_path, test_bucket, bucket, dummy_file, expected
):
    gs = gh.GSStorageHandler(google_auth_key_path)
    d = gs.upload(bucket, dummy_file)
    fname = os.path.split(dummy_file.name)[1]

    expected['name'] = fname
    del d['etag']
    del d['generation']
    del d['id']
    del d['md5Hash']
    del d['crc32c']
    del d['updated']
    del d['mediaLink']
    del d['timeCreated']
    expected['selfLink'] = expected['selfLink'] % urllib.quote(
        fname, safe=''
    )

    assert d == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'object', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            None,
            [
                {
                    u'kind': u'storage#object',
                    u'contentType': u'text/plain',
                    # u'md5Hash': u'1B2M2Y8AsgTpgAmY7PhCfg==',
                    u'bucket': TEST_BUCKET_NAME,
                    # u'crc32c': u'AAAAAA==',
                    u'metageneration': u'1',
                    u'storageClass': u'STANDARD',
                    u'selfLink': u'https://www.googleapis.com/storage/v1/b/%s'
                    '/o/%s' % (TEST_BUCKET_NAME, "%s"),
                    u'size': u'26'
                }
            ]
        ),
    ]
)
def test_list_bucket_content(
    google_auth_key_path, test_bucket_with_content, bucket, object, dummy_file,
    expected
):
    gs = gh.GSStorageHandler(google_auth_key_path)
    d = gs.list_bucket_content(bucket)
    print dummy_file.name
    fname = os.path.split(dummy_file.name)[1]
    expected[0][u'name'] = fname
    del d[0]['etag']
    del d[0]['generation']
    del d[0]['id']
    del d[0]['crc32c']
    del d[0]['md5Hash']
    del d[0]['updated']
    del d[0]['mediaLink']
    del d[0][u'timeCreated']
    del d[0][u'owner']
    expected[0]['selfLink'] = expected[0]['selfLink'] % urllib.quote(
        fname, safe=''
    )

    assert d == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            "This is a test file dummy."
        ),
    ]
)
def test_download(
    google_auth_key_path, bucket, dummy_file, test_bucket_with_content,
    expected
):
    gs = gh.GSStorageHandler(google_auth_key_path)

    with tempfile.NamedTemporaryFile() as tmpfile:
        gs.download(bucket, os.path.split(dummy_file.name)[1], tmpfile)

        tmpfile.seek(0)
        assert tmpfile.read() == expected
