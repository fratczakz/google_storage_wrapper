import os
import datetime
import tempfile
import tarfile
import json
import shutil
import csv
import logging

import distutils.dir_util as du

import google_storage.core.utils as g

DATE_FOLDER_FORMAT = '%Y%m%d%H%M%S'

logger = logging.getLogger()


class Base(object):
    '''
    Base class handling google storage logging and functionality
    '''

    bucket = None
    mimetype = None

    def __init__(
        self,
        sitename,
        json_key_path,
        date=datetime.datetime.now(),
        tmpdir=None,
        archived=False
    ):
        '''Constructor method
        :param sitename: Name of the site.
        :type sitename: str
        :param json_key_path: Path to the Json key for authentication
        :type json_key_path: str
        :param date: Date for which data.
        :type date: datetime.datetime.
        :param tmpdir: Path to the temp dir for temprary storage.
        :type tmpdir: str
        :param archived: Flag If the data.
        :type archived: bool.
        '''
        self.sitename = sitename
        self.json_key_path = json_key_path
        self.date = date
        if not tmpdir:
            tmpdir = tempfile.mkdtemp("_%s" % self.__class__.__name__)
        self.tmpdir = tmpdir
        self.archived = archived

    def __enter__(self):
        '''With operator handler
        '''
        return self

    def __exit__(self, type, vale, traceback):
        '''With operator handler
        '''
        self.clean()

    def download(self, files, bucket=None):
        '''Generator containing downloaded files
        :param files: Lost of tuples where 1st el is a google storage filepath,
                      2nd is a tmp file or None. If file is non temp file will
                      be created
        :type files: list of tuples [(google_filepath, file_object)]
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :retunrs: Generator with file objects
        :rtype: generator with file objects
        '''
        if not bucket:
            bucket = self.bucket

        gs = g.GSStorageHandler(self.json_key_path)
        for fpath, f in files:
            if f is None:
                f = tempfile.NamedTemporaryFile()

            f = gs.download(bucket, fpath, f)
            f.seek(0)
            yield f

    def upload(self, files, bucket=None, public=False):
        '''Uploads files to a bucket in google storage.
        :param files: Lost of tuples where 1st el is a google storage filepath,
                      2nd is a file to upload.
        :type files: list of tuples [(google_filepath, file_object)]
        :param bucket: Name of the bucket in google storage to access
        :type bucket: str
        :param public: Flag if file exposed as public
        :type public: bool
        :retunrs: List of responses from google_storage
        :rtype: generator with file objects
        '''
        r = []
        if not bucket:
            bucket = self.bucket

        gs = g.GSStorageHandler(self.json_key_path)
        for fpath, map_file in files:
            f = gs.upload(
                bucket, map_file, fpath, mimetype=self.mimetype, public=public
            )
            r.append(f)
        return r

    def store_local(
        self, content, filename
    ):
        '''Store content locally. This has been mainly put in place for tests.
        :param content: Either list of items for csv or structure for json file
        :type content: list or dict
        :param filename: filename for the new file
        :type filename: string
        '''
        path = self.tmpdir

        if self.archived:
            path = os.path.join(
                self.tmpdir, self.date.strftime(DATE_FOLDER_FORMAT)
            )
            if not os.path.exists(path):
                os.makedirs(path)

        with open(os.path.join(path, filename), 'w') as fp:
            base, ext = os.path.splitext(filename)
            if ext == '.json':
                json.dump(content, fp)
            elif ext == '.csv':
                csvw = csv.writer(fp)
                csvw.writerows(content)

    def copy_into(self, src_path):
        '''Copy contents of the src_path location to temp folder
        :param src_path: Folder path
        :type files: str
        '''
        path = self.tmpdir
        if self.archived:
            path = os.path.join(
                self.tmpdir, self.date.strftime(DATE_FOLDER_FORMAT)
            )
            if not os.path.exists(path):
                os.makedirs(path)
        du.copy_tree(src_path, path)

    def make_tar(self):
        '''Tar up content of the temporary location.
        '''
        path = os.path.join(
            self.tmpdir, self.date.strftime(DATE_FOLDER_FORMAT)
        )
        with tarfile.open("%s.tar.gz" % path, "w:gz") as tar:
            tar.add(path, arcname=os.path.basename(path))

        shutil.rmtree(path)

    def get_location(self):
        '''Method returning path in google storage. Defined in order to provide
        ability to override in children.
        '''
        return os.path.join(
            self.sitename, self.date.strftime(DATE_FOLDER_FORMAT)
        )

    def store_gs(self, location=None):
        '''Store contents of the tmpdir in google storage.
        :param location: location in google storage
        :type files: str
        '''

        if not location:
            location = self.get_location()

        if self.archived:
            self.make_tar()

        fmap = [
            (location, open(os.path.join(self.tmpdir, fpath), 'r'))
            for fpath in os.listdir(self.tmpdir)
        ]
        self.upload(fmap)

    def clean(self):
        '''Remove temporary location.
        '''
        logger.info("Removing temp folder: %s" % self.tmpdir)
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)


class Maps(Base):
    '''Class for storing Maps in google storage
    '''
    bucket = 'pi-maps'
    mimetype = 'image/svg+xml'


class Lookups(Base):
    '''Class for storing Lookup structures in google storage
    '''
    bucket = 'pi-wifi-location-lookups'
    mimetype = 'text/json'


class Outputs(Base):
    '''Class for storing Processing Outputs in google storage
    '''

    bucket = 'pi-wifi-location-outputs'
    mimetype = None

    def get_location(self):
        '''Overriding method from parent
        '''
        return self.sitename


class CalibrationRefData(Base):
    '''Class for storing Reference Calibration data in google storage
    '''

    bucket = 'pi-wifi-location-calibration-ref-data'
    mimetype = None
