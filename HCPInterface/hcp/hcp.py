#!/usr/bin/env python

"""
Module for simple interfacing with the HCP cloud storage.
"""

import json
import os
import sys
import time
import boto3
import urllib3
import botocore
import threading

from functools import wraps
from botocore.utils import fix_s3_host
from botocore.client import Config
from boto3.s3.transfer import TransferConfig

from HCPInterface.preproc import preproc
from HCPInterface.hcp.helpers import calculate_etag
from HCPInterface.hcp.errors import (UnattachedBucketError, LocalFileExistsError,
                                     UnknownSourceTypeError, MismatchChecksumError, 
                                     ConnectionError, MissingCredentialsError)
from HCPInterface.hcp.config import get_config
from HCPInterface import log


config = get_config()

class ProgressPercentage(object):
    """Progressbar for both upload and download of files."""
    def __init__(self, source):
        self._source = source

        if isinstance(self._source, str):  # Local file
            self._size = os.path.getsize(self._source)
        elif hasattr(self._source, 'size'):  # Object summary
            self._size = self._source.size
        elif hasattr(self._source, 'content_length'):  # Object
            self._size = self._source.content_length
        else:
            raise UnknownSourceTypeError(f'Unknown source format {source}')

        self._seen_so_far = 0
        self._lock = threading.Lock()

        self._previous_time = time.time()
        self._previous_bytesize = self._seen_so_far
        self._interval = 1
        self._speed = 0

    def _calculate_speed(self):
        curr_time = time.time()
        if curr_time - self._interval > self._previous_time:
            speed = (self._seen_so_far - self._previous_bytesize) / (curr_time - self._previous_time)
            self._speed = round(speed / (1024 ** 2), 2)
            self._previous_time = curr_time
            self._previous_bytesize = self._seen_so_far

        return self._speed

    def _trim_text(self, text):
        """Trim text to fit current terminal size."""
        terminal_width = os.get_terminal_size()[0]

        if len(text) > terminal_width:
            diff = len(text) - terminal_width
            text = text[:len(text)-diff-3] + '...'

        return text

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            speed = self._calculate_speed()
            percentage = (self._seen_so_far / self._size) * 100
            text = "\r%s  %s / %s  %s  (%.2f%%)" % (self._source,
                                                    self._seen_so_far,
                                                    self._size,
                                                    f'{speed}MB/s',
                                                    percentage)
            text = self._trim_text(text)

            sys.stdout.write(text)
            sys.stdout.flush()

    def __exit__(self):
        sys.stdout.flush()


def bucketcheck(fn):
    """Checks to see that bucket is attached before executing."""
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        if hasattr(self, 'bucket'):
            return fn(self, *args, **kwargs)
        else:
            raise UnattachedBucketError('Attempted work on unattached bucket. Aborting...')

    return wrapped


class HCPManager:
    def __init__(self, endpoint="", aws_access_key_id="", aws_secret_access_key="", bucket=None,credentials_path="", debug=False):
        self.bucket = bucket
        
        if credentials_path != "":
            [ep, aid, key] = preproc.read_credentials(credentials_path)
            self.set_credentials(ep, aid, key)  
        else:
            self.set_credentials(endpoint, aws_access_key_id, aws_secret_access_key)

        # Very verbose. Use with care.
        if debug:
            boto3.set_stream_logger(name='botocore')

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # Disable warnings about missing SLL certificate.

        session = boto3.session.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

        s3_config = Config(s3={'addressing_style': 'path',
                               'payload_signing_enabled': True},
                           signature_version='s3v4')

        self.s3 = session.resource('s3',
                                   endpoint_url=self.endpoint,
                                   verify=False,  # Checks for SLL certificate. Disables because of already "secure" solution.
                                   config=s3_config)

        self.transfer_config = TransferConfig(multipart_threshold=config.getint('hcp', 'size_threshold'),
                                              max_concurrency=config.getint('hcp', 'max_concurrency'),
                                              multipart_chunksize=config.getint('hcp', 'chunk_size'))

        self.s3.meta.client.meta.events.unregister('before-sign.s3', fix_s3_host)

        self.test_connection()

    def list_buckets(self):
        """List all available buckets at endpoint."""
        return [bucket.name for bucket in self.s3.buckets.all()]

    def test_connection(self):
        """Validate the connection works with as little overhead as possible."""
        try:
            if self.bucket is None:
                raise ConnectionError("No bucket assigned")
            self.s3.meta.client.head_bucket(Bucket=self.bucket)
        except ConnectionError:
            log.error("Invalid access, credentials or bucket")

    def attach_bucket(self, bucket):
        """Attempt to attach to the given bucket."""
        if bucket is None:
            log.error("Attempted to attach bucket. But no bucket named.")
        self.bucket = self.s3.Bucket(bucket)
        if hasattr(self, 'objects'):
            delattr(self, 'objects')  # Incase of already attached bucket

    def set_credentials(self, endpoint, aws_access_key_id, aws_secret_access_key):
        """Mounts credentials to HCPManager object"""
        self.endpoint = endpoint
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    @bucketcheck
    def get_object(self, key):
        """Return object with exact matching key."""
        obj = self.bucket.Object(key)
        try:
            obj.content_length  # Good enough?
        except botocore.exceptions.ClientError:
            return None
        return obj

    @bucketcheck
    def get_objects(self):
        """Return all objects in bucket."""
        if hasattr(self, 'objects'):
            return self.objects
        else:
            self.objects = list(self.bucket.objects.all())
            return self.objects

    @bucketcheck
    def reload_objects(self):
        """Reload and return all objects in bucket."""
        self.objects = list(self.bucket.objects.all())
        return self.objects

    @bucketcheck
    def search_objects(self, string):
        """Return all objects whose keys contain the given string."""
        if not hasattr(self, 'objects'):
            self.get_objects()

        return [obj for obj in self.objects if string in obj.key]

    @bucketcheck
    def upload_file(self, local_path, remote_key, metadata={},silent=False):
        """Upload local file to remote as key with associated metadata."""
        # Force has been intentionally left out from upload functionality due to risk of overwriting clinical data. 
        # Should the need arise to remove erroneous data then it must be manually (and therefore fully intentionally) 
        # deleted prior to uploading
        prev_remote_obj = self.get_object(remote_key)

        #if force and prev_remote_obj is not None:
        #    self.delete_object(prev_remote_obj)
        #    log.info("Removed remote file prior to upload of local file.")
        cb = ProgressPercentage(local_path)
        if silent:
            cb = ""
        self.bucket.upload_file(local_path,
                                remote_key,
                                ExtraArgs={'Metadata': metadata},
                                Config=self.transfer_config,
                                Callback=cb)
        print('')  # Post progressbar correction for stdout

        remote_obj = self.get_object(remote_key)
        calculated_etag = calculate_etag(local_path)

        if calculated_etag != remote_obj.e_tag:
            self.delete_object(remote_obj)
            raise MismatchChecksumError('Local and remote file checksums differ. Removing remote file.')

    @bucketcheck
    def download_file(self, obj, local_path, force=False, silent=False):
        """Download objects file to specified local file."""
        if isinstance(obj, str):
            obj = self.get_object(obj)

        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, os.path.basename(obj.key))

        if os.path.exists(local_path):
            if not force:
                raise LocalFileExistsError(f'Local file already exists: {local_path}')

        cb = ProgressPercentage(obj)
        if silent:
            cb = ""
        self.bucket.download_file(obj.key,
                                  local_path,
                                  Callback=cb)
        print('')  # Post progressbar correction for stdout


    @bucketcheck
    def delete_object(self, obj):
        """Delete the provided object."""
        self.bucket.delete_objects(Delete={'Objects': [{'Key': obj.key}]})

    @bucketcheck
    def read_object(self, obj):
        """Read the object content. Unwise for large files"""
        if obj.content_length < 100000:  # NOTE Arbitrarily set
            return obj.get()['Body'].read().decode('utf-8')
        else:
            return ''
