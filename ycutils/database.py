"""Creates database object."""
from typing import Union, Iterable, Optional, Dict

import bson.raw_bson
import collections
import pymongo.database
import boto3
from . import utils

Document = Union[collections.MutableMapping, Dict, bson.raw_bson.RawBSONDocument]


class Connector:
    """Once we decide what integrations we would like to use
    it will be possible to implement
    """
    _required_keys: Optional[Iterable] = ("git", "config", "s3")

    def __init__(
            self,
            username: str,
            password: str,
            database: str,
            host: Union[str, Iterable[str]],
            port: int = 27018,
            replicaSet: str = "rs01",
            tlsCAFile: str = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
    ):
        self._db: pymongo.database.Database = utils.make_db(
            username, password, database, host, port, replicaSet, True, tlsCAFile
        )
        self._s3: boto3.client = utils.make_s3(aws_access_key_id, aws_secret_access_key)

    def push_experiment(self,
                        name: str,
                        config: Document,
                        experiment: Document,
                        git_parent_dir: utils.Path = '.',
                        s3: str = None,
                        collection: str = 'loglake'
                        ):
        _collection = self._db.get_collection(collection)
        git = utils.scan_git(git_parent_dir)
        _document = collections.OrderedDict(
            name=name,
            config=config,
            experiment=experiment,
            git=git,
            s3=s3
        )
        return _collection.insert_one(_document)

    @property
    def database(self) -> pymongo.database.Database:
        return self._db

    @property
    def s3(self) -> boto3.Session:
        return self._s3
