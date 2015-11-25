import os
import urllib
import datetime
import logging
import tempfile
import json

import pytest

import google_storage.core.utils as gh
import google_storage.core.handlers as gs


TEST_BUCKET_NAME = u"pi-test-bucket"


ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")


@pytest.fixture(scope='function')
def map_files(request):
    out = []

    for i in xrange(5):
        f = tempfile.NamedTemporaryFile()
        f.write("""
<svg  xmlns="http://www.w3.org/2000/svg"
      xmlns:xlink="http://www.w3.org/1999/xlink">
    <rect x="10" y="10" height="100" width="100"
          style="stroke:#ff0000; fill: #%s000ff"/>
</svg>
""" % i)
        f.flush()
        f.seek(0)
        out.append(('dummysite/%s/' % ts, f))

    def fin():
        for i in out:
            i[1].close()

    request.addfinalizer(fin)
    return out


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            [{
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'CMDjvdSn2MYCEAE=',
                # u'generation': u'1436797465752000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:25.751Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2Ftmp5350Ol?generation=1436797465752000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmp5350Ol/1436797465752000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'COj5zdSn2MYCEAE=',
                # u'generation': u'1436797466017000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:26.016Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpN1ka7Y?generation=1436797466017000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpN1ka7Y/1436797466017000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'CMDVmNSn2MYCEAE=',
                # u'generation': u'1436797465144000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:25.143Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpPF2wTE?generation=1436797465144000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpPF2wTE/1436797465144000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'COiOx9Sn2MYCEAE=',
                # u'generation': u'1436797465905000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:25.904Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpgEeo34?generation=1436797465905000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpgEeo34/1436797465905000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'CLj01NSn2MYCEAE=',
                # u'generation': u'1436797466131000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:26.130Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpiT85F1?generation=1436797466131000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpiT85F1/1436797466131000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }]
        ),
    ]
)
def test_upload(
    google_auth_key_path, bucket, map_files, test_bucket, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
    ) as gm:
        gm.mimetype = 'image/svg+xml'
        gm.upload(map_files, bucket)

        gst = gh.GSStorageHandler(google_auth_key_path)

        map_list = gst.list_bucket_content(TEST_BUCKET_NAME)

        for i in xrange(len(expected)):
            fname = "dummysite/%s/%s" % (
                ts, os.path.split(map_files[i][1].name)[1]
            )
            expected[i]['name'] = fname
            del map_list[i]['etag']
            del map_list[i]['generation']
            del map_list[i]['id']
            del map_list[i]['updated']
            del map_list[i]['mediaLink']
            del map_list[i]['md5Hash']
            del map_list[i]['crc32c']
            del map_list[i]['timeCreated']
            del map_list[i]['owner']
            expected[i]['selfLink'] = expected[i]['selfLink'] % urllib.quote(
                fname, safe=''
            )

        for i in expected:
            assert i in map_list


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket', 'expected'),
    [
        (
            TEST_BUCKET_NAME,
            [{
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'CMDjvdSn2MYCEAE=',
                # u'generation': u'1436797465752000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:25.751Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2Ftmp5350Ol?generation=1436797465752000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmp5350Ol/1436797465752000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'COj5zdSn2MYCEAE=',
                # u'generation': u'1436797466017000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:26.016Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpN1ka7Y?generation=1436797466017000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpN1ka7Y/1436797466017000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'CMDVmNSn2MYCEAE=',
                # u'generation': u'1436797465144000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:25.143Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpPF2wTE?generation=1436797465144000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpPF2wTE/1436797465144000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'COiOx9Sn2MYCEAE=',
                # u'generation': u'1436797465905000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:25.904Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpgEeo34?generation=1436797465905000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpgEeo34/1436797465905000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }, {
                u'kind': u'storage#object',
                u'contentType': u'image/svg+xml',
                # u'etag': u'CLj01NSn2MYCEAE=',
                # u'generation': u'1436797466131000',
                # u'md5Hash': u'q6kMKbku3rrJFraM8T3IXQ==',
                u'bucket': u'pi-test-bucket',
                # u'updated': u'2015-07-13T14:24:26.130Z',
                # u'crc32c': u'f4bawg==',
                u'metageneration': u'1',
                u'storageClass': u'STANDARD',
                # u'mediaLink': u'https://www.googleapis.com/download/storage/v1/b/pi-test-bucket/o/dummysite%2F20150713142422%2FtmpiT85F1?generation=1436797466131000&alt=media',
                # u'id': u'pi-test-bucket/dummysite/20150713142422/tmpiT85F1/1436797466131000',
                u'selfLink': u'https://www.googleapis.com/storage/v1/b/pi-test-bucket/o/%s',
                u'size': u'198'
            }]
        ),
    ]
)
def test_upload_public(
    google_auth_key_path, bucket, map_files, test_bucket, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
    ) as gm:
        gm.mimetype = 'image/svg+xml'
        file_details = gm.upload(map_files, bucket, public=True)

        gst = gh.GSStorageHandler(google_auth_key_path)

        map_list = gst.list_bucket_content(TEST_BUCKET_NAME)
        from pprint import pprint
        pprint(file_details)

        acls = [
            (i['entity'], i['role'])
            for f in file_details
            for i in f['acl']
        ]
        pprint(acls)
        assert acls.count((u'allUsers', u'READER')) == len(file_details)


@pytest.mark.slow
@pytest.mark.parametrize(
    ('bucket',),
    [
        (
            TEST_BUCKET_NAME,
        ),
    ]
)
def test_download(
    google_auth_key_path, bucket, map_files, test_bucket
):
    expected = list(zip(*map_files)[1])

    with gs.Base(
        "dummysite",
        google_auth_key_path,
    ) as gm:
        gm.mimetype = 'image/svg+xml'
        print map_files

        gm.upload(map_files, bucket)

        map_files = [
            (os.path.join(map_path, os.path.split(map_file.name)[1]), None)
            for map_path, map_file in map_files
        ]

        out = list(gm.download(map_files, bucket))

        matches = [False] * len(expected)

        for i, item in enumerate(expected):
            c1 = item.read()
            for j in out:
                c2 = j.read()
                j.seek(0)
                if c1 == c2:
                    matches[i] = True

        assert all(matches)


@pytest.mark.slow
@pytest.mark.parametrize(
    ('content', 'expected'),
    [
        (
            {"test": 123},
            ['test_file.json']
        ),
    ]
)
def test_store_local(
    google_auth_key_path, content, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
    ) as gb:

        gb.store_local(content, "test_file.json")

        assert os.listdir(gb.tmpdir) == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ('content', 'subfolder', 'expected'),
    [
        (
            {"test": 123},
            'doge',
            ['test_file.json']
        ),
    ]
)
def test_store_local_subf(
    google_auth_key_path, content, subfolder, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
        datetime.datetime(2014, 01, 01, 01, 01, 01),
        archived=True
    ) as gb:

        gb.store_local(content, "test_file.json")

        assert os.path.exists(
            os.path.join(gb.tmpdir, "20140101010101", "test_file.json")
        )


@pytest.mark.slow
@pytest.mark.parametrize(
    ('date', 'expected'),
    [
        (
            datetime.datetime(2014, 01, 01, 01, 01, 01),
            'dummysite/20140101010101'
        ),
    ]
)
def test_get_location(
    google_auth_key_path, date, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
        date
    ) as gb:
        assert gb.get_location() == expected


@pytest.mark.slow
@pytest.mark.parametrize(
    ('date', 'content', 'expected'),
    [
        (
            datetime.datetime(2014, 01, 01, 01, 01, 01),
            {"test": 123},
            ['20140101010101.tar.gz']
        ),
    ]
)
def test_make_tar(
    google_auth_key_path, date, content, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
        date,
        archived=True
    ) as gb:
        gb.store_local(content, "test_file.json")

        gb.make_tar()
        assert os.listdir(gb.tmpdir) == expected


@pytest.mark.parametrize(
    ('date', 'content', 'subfolder', 'expected'),
    [
        (
            datetime.datetime(2014, 01, 01, 01, 01, 01),
            {"test": 123},
            'doge',
            ['doge.tar.gz']
        ),
    ]
)
def test_store_gs(
    google_auth_key_path, date, content, subfolder, expected
):
    with gs.Base(
        "dummysite",
        google_auth_key_path,
        date
    ) as gb:
        gb.bucket = TEST_BUCKET_NAME

        gb.store_local(content, "test_file.json")

        gb.store_gs()

        fs = list(gb.download(
            [(os.path.join(gb.get_location(), 'test_file.json'), None)]
        ))

        assert json.load(fs[0]) == content
