"""Creates database object."""
from typing import Union, Iterable, Optional

import boto3
import pymongo.database
from . import utils, entities


class Connector:
    """Once it will be decided what integrations we would like to use
    more useful implementation will be possible.

    DVC, S3, MongoDB...
    """
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
        self._db: pymongo.database.Database = utils.make_mongo_client(
            username=username,
            password=password,
            authSource=database,
            host=host,
            port=port,
            replicaSet=replicaSet,
            tls=True,
            tlsCAFile=tlsCAFile)[database]
        self._s3: boto3.client = utils.make_s3(aws_access_key_id, aws_secret_access_key)

    def push_experiment(
            self,
            name: str,
            config: entities.Document,
            metrics: entities.Document,
            git_parent_dir: utils.Path = '.',
            requirements_file: Optional[utils.Path] = None,
            s3: Optional[str] = None,
    ):
        """Push experiment to the database."""
        #todo: add artifacts support
        _runs_collection = self.database.runs
        _metrics_collection = self.database.metrics
        last = _runs_collection.find_one(sort=[("_id", 1)])
        _id = last["_id"] + 1 if last else 1
        runs_entry, metrics_entries, _ = entities.make_log_entries(
            _id=_id,
            name=name,
            config=config,
            metrics=metrics,
            git_parent_dir=git_parent_dir,
            requirements_file=requirements_file
        )
        if s3:
            runs_entry.update(s3=s3)
        id_runs = _runs_collection.insert_one(runs_entry)
        ids_metrics = _metrics_collection.insert_many(metrics_entries)
        return id_runs, ids_metrics

    @property
    def database(self) -> pymongo.database.Database:
        return self._db

    @property
    def s3(self) -> boto3.Session:
        return self._s3
