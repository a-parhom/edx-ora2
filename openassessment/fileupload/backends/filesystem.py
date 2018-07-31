from .base import BaseBackend, Settings
from .. import exceptions

from django.conf import settings
import django.core.cache
from django.core.urlresolvers import reverse
from django.utils.encoding import smart_text

import os


class Backend(BaseBackend):
    """
    Upload openassessment student files to a local filesystem. Note
    that in order to use this file storage backend, you need to include the
    urls from openassessment.fileupload in your urls.py file:

    E.g:
        url(r'^openassessment/storage', include(openassessment.fileupload.urls)),

    The ORA2_FILEUPLOAD_CACHE_NAME setting will also have to be defined for the
    name of the django.core.cache instance which will maintain the list of
    active storage URLs.

    E.g:

        ORA2_FILEUPLOAD_CACHE_NAME = "ora2-storage"
        CACHES = {
            ...
            'ora2-storage': {
                'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
                ...
            },
            ...
        }
    """

    def get_upload_url(self, key, content_type):
        make_upload_url_available(self._get_key_name(key), self.UPLOAD_URL_TIMEOUT)
        return self._get_url(key)

    def get_download_url(self, key):
        file_path = self._get_file_path(self._get_key_name(key))
        if not os.path.exists(file_path):
            return ''
        else:
            make_download_url_available(self._get_key_name(key), self.DOWNLOAD_URL_TIMEOUT)
            return self._get_url(key)

    def remove_file(self, key):
        from openassessment.fileupload.views_filesystem import safe_remove, get_file_path
        return safe_remove(get_file_path(self._get_key_name(key)))

    def _get_url(self, key):
        key_name = self._get_key_name(key)
        key_name = key_name.split("|", 1)
        if len(key_name) == 1:
            url = reverse("openassessment-filesystem-storage", kwargs={'key': key_name[0]})
        else:
            url = reverse("openassessment-filesystem-storage", kwargs={'key': key_name[0], 'filename': key_name[1]})
        return url

    def _get_file_path(self, key):
        return os.path.join(self._get_data_path(key), "content")

    def _get_data_path(self, key):
        return os.path.join(self._get_bucket_path(), key)

    def _get_bucket_path(self):
        """
        Returns the path to the bucket directory.
        """
        dir_path = os.path.join(
            self._get_root_directory_path(),
            Settings.get_bucket_name(),
        )
        return os.path.abspath(dir_path)

    def _get_root_directory_path(self):
        """
        Returns the path to the root directory in which bucket directories are stored.

        Raises:
            FileUploadInternalError if the root directory setting does not exist.
        """
        root_dir = getattr(settings, "ORA2_FILEUPLOAD_ROOT", None)
        if not root_dir:
            raise exceptions.FileUploadInternalError("Undefined file upload root directory setting")
        return root_dir


def get_cache():
    """
    Returns a django.core.cache instance in charge of maintaining the
    authorized upload and download URL.

    Raises:
        FileUploadInternalError if the cache name setting is not defined.
        InvalidCacheBackendError if the corresponding cache backend has not
        been configured.
    """
    cache_name = getattr(settings, "ORA2_FILEUPLOAD_CACHE_NAME", None)
    if cache_name is None:
        raise exceptions.FileUploadInternalError("Undefined cache backend for file upload")
    return django.core.cache.caches[cache_name]


def make_upload_url_available(url_key_name, timeout):
    """
    Authorize an upload URL.

    Arguments:
        url_key_name (str): key that uniquely identifies the upload url
        timeout (int): time in seconds before the url expires
    """
    get_cache().set(
        smart_text(get_upload_cache_key(url_key_name)),
        1, timeout
    )


def make_download_url_available(url_key_name, timeout):
    """
    Authorize a download URL.

    Arguments:
        url_key_name (str): key that uniquely identifies the url
        timeout (int): time in seconds before the url expires
    """
    get_cache().set(
        smart_text(get_download_cache_key(url_key_name)),
        1, timeout
    )


def is_upload_url_available(url_key_name):
    """
    Return True if the corresponding upload URL is available.
    """
    return get_cache().get(smart_text(get_upload_cache_key(url_key_name))) is not None


def is_download_url_available(url_key_name):
    """
    Return True if the corresponding download URL is available.
    """
    return get_cache().get(smart_text(get_download_cache_key(url_key_name))) is not None


def get_upload_cache_key(url_key_name):
    return "upload/" + url_key_name


def get_download_cache_key(url_key_name):
    return "download/" + url_key_name
