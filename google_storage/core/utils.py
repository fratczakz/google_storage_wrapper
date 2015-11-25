import os
import json
import datetime
import logging
import time
import random

import httplib2

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import BatchHttpRequest

from oauth2client.client import SignedJwtAssertionCredentials

logger = logging.getLogger(__name__)

CHUNKSIZE = 2 * 1024 * 1024
NUM_RETRIES = 5
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)
DEFAULT_MIMETYPE = 'application/octet-stream'


class OAuth2(object):
    '''Class handling authentication.
    '''

    @staticmethod
    def authenticate(service, json_key_path):
        '''Returns credentials to authenticate the requests.
        :param json_key_path: path to the stored json_key
        :type json_key_path: str
        '''
        json_key = json.load(open(json_key_path, 'r'))
        scope = ['https://www.googleapis.com/auth/%s' % service]
        credentials = SignedJwtAssertionCredentials(
            json_key['client_email'], json_key['private_key'], scope)
        return credentials.authorize(httplib2.Http())


class GSHandler(object):
    '''Class handling authentication to google services.
    '''
    def __init__(
        self, json_key_path, auth_mode, service, api_ver,
        project='piinfrastucture'
    ):
        '''Constructor
        :param json_key_path: path to the stored json_key
        :type json_key_path: str
        :param auth_mode: authentication mode
        :type auth_mode: str
        :param service: service to auth with
        :type service: str
        :param service: api version
        :type service: str
        '''

        self.http_auth = OAuth2.authenticate(auth_mode, json_key_path)
        self.service = build(service, api_ver, http=self.http_auth)
        self.project = project

    def get_json_key_path(self):
        '''Get path for the json key with credentails.
        :retunrs: Key path
        :rtype: str
        '''

        json_key_path = os.getenv('GOOGLE_JSON_KEYPATH')
        if not json_key_path:
            raise ValueError("Envvar GOOGLE_JSON_KEYPATH hasn't been set.")

        if not os.path.exists(json_key_path):
            raise IOError("Couldn't access the key at: %s" % (json_key_path))

        return json_key_path


class GSStorageHandler(GSHandler):
    '''Rough Google storage wrapper.
    '''
    def __init__(self, json_key_path=None):
        '''Constructor.
        :param json_key_path: path to the stored json_key
        :type json_key_path: str
        :retunrs: Response JSON str listing bucket contents
        :rtype: json
        '''

        if not json_key_path:
            json_key_path = self.get_json_key_path()

        super(GSStorageHandler, self).__init__(
            json_key_path, 'devstorage.full_control', 'storage', 'v1'
        )

    def details(self, bucket):
        '''Get bucket details.
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :retunrs: Response JSON str listing bucket contents
        :rtype: json
        '''
        logger.info("Pulling info for %s" % bucket)
        req = self.service.buckets().get(bucket=bucket)
        resp = req.execute()
        return resp

    def bucket_exists(self, bucket):
        '''Check if bucket exists.
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :retunrs: True or False
        :rtype: bool
        '''
        try:
            self.details(bucket)
            logger.info("Bucket %s already exists." % bucket)
            return True
        except HttpError, e:
            if e.resp.status == 404:
                logger.info("Bucket %s doen't exist." % bucket)
                return False
            else:
                raise e

        return False

    def list_bucket_content(self, bucket):
        '''List existing bucket.
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :retunrs: Response JSON str listing bucket contents
        :rtype: json
        '''

        logger.info("Getting a list of buckets contents %s" % bucket)
        resp = self.service.objects().list(bucket=bucket).execute()
        logger.info("Got the content: %s" % resp)
        return resp["items"] if "items" in resp else None

    def delete_bucket_content(self, bucket):
        '''Delete content of existing bucket.
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :retunrs: Response JSON str
        :rtype: json
        '''

        objects = self.list_bucket_content(bucket)
        if objects:
            logger.info("Createing batch for objects deletion...")
            batch = BatchHttpRequest(callback=self.delete_object_cb)
            for o in objects:
                logger.debug(
                    "Adding 'delete request' to batch: %s" % o['name']
                )
                batch.add(self.service.objects().delete(
                    object=o['name'], bucket=bucket
                ))
            logger.info("Executing batch...")
            resp = batch.execute(http=self.http_auth)
            logger.info("Response content from batch: %s" % resp)

            return resp

        return None

    def delete_object_cb(self, request_id, response, exception):
        if exception is not None:
            logger.warning("Exception while deleting object: %s" % exception)

    def create_bucket(self, bucket):
        '''Create bucket.
        :param bucket: Name of the bucket in google storage to create
        :type bucket: str
        :retunrs: Response JSON str
        :rtype: json
        '''

        body = {
            "kind": "storage#bucket",
            "name": bucket,
            "timeCreated": datetime.datetime.utcnow().isoformat()[0:-3]+"Z",
            "id": bucket,
            "storageClass": "STANDARD",
            "metageneration": "1",
            "etag": "CAE=",
            'selfLink': u'https://www.googleapis.com/storage/v1/b/%s' % bucket,
            "location": "EU",
        }

        logger.info("Createing bucket %s with settings: %s." % (bucket, body))

        req = self.service.buckets().insert(project=self.project, body=body)
        resp = None
        progressless_iters = 0
        while resp is None:
            logger.info("Response try: ... %s" % (
                progressless_iters
            ))
            try:
                resp = req.execute()
            except HttpError, e:
                logger.info(e)
                if e.resp.status == 503:
                    self.__handle_progressless_iter(e, progressless_iters)
                    progressless_iters += 1
                if e.resp.status == 404:
                    logger.info("Resource not Found! Error: %s" % e)
                    break

        logger.info("Response content: %s" % resp)
        return resp

    def delete_bucket(self, bucket, delete_content=False):
        '''Delete existing bucket.
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :param deleting_content: If bucket non empty you want to delete the
                                 content and delete bucket?
        :type deleting_content: bool
        :retunrs: Response JSON str
        :rtype: json
        '''

        logger.info("Deleting bucket %s" % bucket)
        resp = None
        deleting_content = False
        try:
            resp = None
            progressless_iters = 0
            while resp is None:
                logger.info("Response try: %s ... %s" % (
                    deleting_content, progressless_iters
                ))
                try:
                    resp = self.service.buckets().delete(
                        bucket=bucket
                    ).execute()
                    return resp
                except HttpError, e:
                    logger.info(e)
                    if e.resp.status == 503:
                        self.__handle_progressless_iter(e, progressless_iters)
                        progressless_iters += 1
                    if e.resp.status == 404:
                        logger.info("Resource not Found! Error: %s" % e)
                        break

        except HttpError, e:
            if e.resp.status == 409 and delete_content:
                deleting_content = True
                logger.info(
                    "Bucket %s is not empty. Deleteing content..." % bucket
                )
                resp = self.delete_bucket_content(bucket)
            else:
                raise e
        if deleting_content:
            resp = None
            progressless_iters = 0
            while resp is None:
                logger.info("Response try: %s ... %s" % (
                    deleting_content, progressless_iters
                ))
                try:
                    self.service.buckets().delete(bucket=bucket).execute()
                    resp = 1
                except HttpError, e:
                    logger.info(e)
                    if e.resp.status == 503:
                        self.__handle_progressless_iter(e, progressless_iters)
                        progressless_iters += 1
                    if e.resp.status == 404:
                        logger.info("Resource not Found! Error: %s" % e)
                        break

        logger.info("Response content: %s" % resp)
        return resp

    def __handle_progressless_iter(self, error, progressless_iters):
        '''Handle retries when HTTP call fails.
        :param error: Error to be raised when retry counter hits threshold.
        :type error: HttpError
        :param progressless_iters: Numer of iterations
        :type progressless_iters: int
        :raises: Error passed as param
        '''

        if progressless_iters > NUM_RETRIES:
            logger.info(
                'Failed to make progress for too many consecutive '
                'iterations.'
            )
            raise error

        sleeptime = random.random() * (2**progressless_iters)
        logger.info(
            'Caught exception (%s). Sleeping for %s seconds before retry #%d.'
            % (str(error), sleeptime, progressless_iters)
        )
        time.sleep(sleeptime)

    def upload(
            self,
            bucket,
            fileobject,
            location='',
            mimetype='text/plain',
            public=False
    ):
        '''Uploads files to a bucket in google storage.
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :param fileobject: File to store in google storage.
        :type fileobject: file
        :param location: Location in google storage bucket to store object
        :type location: str
        :param mimetype: type of the data to store.
        :type mimetype: str
        :param public: If you want data to be public or not.
        :type public: bool
        :retunrs: Response JSON str
        :rtype: json
        '''

        logger.info('Building upload request...')

        gs_path = os.path.join(location, os.path.split(fileobject.name)[1])

        logger.info(gs_path)

        media = MediaFileUpload(
            fileobject.name, mimetype=mimetype
            # resumable=True
        )
        if public:
            request = self.service.objects().insert(
                bucket=bucket,
                name=gs_path,
                media_body=media,
                predefinedAcl="publicRead"
            ).execute()
        else:
            request = self.service.objects().insert(
                bucket=bucket,
                name=gs_path,
                media_body=media,
            ).execute()
        response = request

        logger.info('Uploading file: %s to bucket: %s object: %s ' % (
            fileobject.name, bucket, gs_path
        ))

        logger.info('\nUpload complete!')
        logger.info('Uploaded Object: %s' % response)
        return response

    def download(self, bucket, object_name, fileout):
        '''Download object from a given bucket and store in fileout..
        :param bucket: Name of the bucket.
        :type bucket: str
        :param object_name: path to the item in google storage
        :type object_name: str
        :param fileout: File to store the object as
        :type fileout: file
        :retunrs: Returnf fileout file object
        :rtype: file
        '''

        logger.info('Building download request...')
        request = self.service.objects().get_media(
            bucket=bucket, object=object_name
        )
        logger.info(request)

        media = MediaIoBaseDownload(fileout, request, chunksize=CHUNKSIZE)

        logger.info('Downloading bucket: %s object: %s to file: %s' % (
            bucket, object_name, fileout.name
        ))

        progressless_iters = 0
        done = False
        while not done:
            error = None
            try:
                progress, done = media.next_chunk(num_retries=NUM_RETRIES)
            except HttpError, err:
                error = err
                if err.resp.status < 500 and err.resp.status != 416:
                    raise
            except RETRYABLE_ERRORS, err:
                error = err

            if error:
                progressless_iters += 1
                self.__handle_progressless_iter(error, progressless_iters)
            else:
                progressless_iters = 0

        logger.info('\nDownload complete!')

        return fileout
