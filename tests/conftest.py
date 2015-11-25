import os
import sys
import shutil
import logging

import tempfile
from distutils.dir_util import copy_tree

import pytest

import google_storage.core.utils as gh

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, module_path + '/../')

TEST_BUCKET_NAME = u"pi-test-bucket"
TEST_EMPTY_BUCKET_NAME = u"pi-test-bucket-empty"


logger = logging.getLogger(__name__)


def setup_bucket(gs, bucket):
    if gs.bucket_exists(bucket):
        logger.info("Removing existing buckets content: %s" % bucket)
        gs.delete_bucket_content(bucket)
        logger.info("Buckets content %s deleted!" % bucket)
    else:

        logger.info("Creating new test bucket: %s" % bucket)
        gs.create_bucket(bucket)
        logger.info("Test bucket %s created!" % bucket)


@pytest.fixture
def google_auth_key_path():
    here = os.path.abspath(os.path.dirname(__file__))
    gjson_key_path = os.getenv(
        'GOOGLE_KEY_JSON',
        os.path.join(
            here, 'aux', 'google-service.json'
        ),
    )

    return gjson_key_path


@pytest.fixture(scope="function")
def test_bucket(request, google_auth_key_path):
    gs = gh.GSStorageHandler(google_auth_key_path)
    setup_bucket(gs, TEST_BUCKET_NAME)

    def fin():
        if gs.bucket_exists(TEST_BUCKET_NAME):
            gs.delete_bucket_content(TEST_BUCKET_NAME)

    request.addfinalizer(fin)


@pytest.fixture(scope="function")
def dummy_file(request):
    f = tempfile.NamedTemporaryFile()
    f.write("This is a test file dummy.")
    f.flush()
    f.seek(0)

    def fin():
        f.close()
    request.addfinalizer(fin)

    return f


@pytest.fixture(scope="function")
def test_bucket_with_content(google_auth_key_path, dummy_file):
    gs = gh.GSStorageHandler(google_auth_key_path)
    setup_bucket(gs, TEST_BUCKET_NAME)

    gs.upload(TEST_BUCKET_NAME, dummy_file)

    def fin():
        gs.delete_bucket_content(TEST_BUCKET_NAME)


@pytest.fixture(scope="function")
def test_folder(request):
    logger.info('Craeting test folder...')
    tmpdir_path = tempfile.mkdtemp()
    logger.info('Done: %s' % tmpdir_path)

    test_data_path = os.path.join(
        os.path.dirname(__file__), 'data'
    )

    logger.info("Copying test data from %s to %s..." % (
        test_data_path, tmpdir_path
    ))
    copy_tree(test_data_path, tmpdir_path)

    def fin():
        logger.info("Deleting temp folder: %s" % tmpdir_path)
        shutil.rmtree(tmpdir_path)
        logger.info("Temp folder %s deleted!" % tmpdir_path)

    request.addfinalizer(fin)

    logger.info("Done. Returning path: %s" % tmpdir_path)
    return tmpdir_path


@pytest.fixture
def process_gs(request, google_auth_key_path, test_folder):
    logger.info("Preparing test setup on Google Storage...")

    gs = gh.GSStorageHandler(google_auth_key_path)
    setup_bucket(gs, TEST_BUCKET_NAME)

    location = 'dummysite_process/20141216172035'

    logger.info("Uploading initial test data...")
    for root, dirs, files in os.walk(test_folder):
        if 'dummysite_process' in root:
            for fp in files:
                path = os.path.join(root, fp)
                logger.info("Uploading %s to %s..." % (path, location))
                gs.upload(
                    TEST_BUCKET_NAME,
                    open(path, 'r'),
                    location=location
                )

    logger.info("Done uploading initial test data!")

    def fin():
        logger.info("Deleting test bucket content %s..." % TEST_BUCKET_NAME)
        gs.delete_bucket_content(TEST_BUCKET_NAME)
        logger.info("Test buckets content %s deleted!" % TEST_BUCKET_NAME)

    # request.addfinalizer(fin)

    logger.info("Done preparing Google Storage test setup.")
    logger.info("Returning local temp folder path: %s" % test_folder)

    return test_folder


def configure_log(level=None, name=None):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_handler = logging.FileHandler('/tmp/root_log.log', 'w', 'utf-8')
    file_handler.setLevel(level)
    file_format = logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s '
    )

    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

configure_log(logging.DEBUG)