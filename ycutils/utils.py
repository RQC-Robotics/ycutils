"""Helper functions."""
from typing import Union, Iterable, Optional, Dict, Any

import os
from bson.son import SON
import pathlib
from urllib.parse import quote_plus

import pymongo
import pymongo.database
import boto3
from ruamel.yaml import YAML


Path = Union[str, pathlib.Path]


def make_s3(
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = 'https://storage.yandexcloud.net'
) -> boto3.client:
    """Nothing more than specifying endpoint on boto3 client creation."""
    return boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )


def make_db(
            username: str,
            password: str,
            database: str,
            host: Union[str, Iterable[str]],
            port: int = 27018,
            replicaSet: str = "rs01",
            tls=True,
            tlsCAFile: str ="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
) -> pymongo.database.Database:
    """Straightforward database creation."""
    username, password = map(quote_plus, (username, password))
    tlsCAFile = tlsCAFile if tls else None
    return pymongo.MongoClient(
        host,
        username=username,
        password=password,
        tls=tls,
        tlsCAFile=tlsCAFile,
        replicaSet=replicaSet,
        authSource=database,
        port=port,
    )[database]


def scan_git(parent_dir: Path = '.') -> Dict[str, Any]:
    """Infer useful git info from the repository."""
    wdir = os.getcwd()
    os.chdir(parent_dir)
    remotes = os.popen('git remote -v').read().split()
    remotes = set(filter(lambda s: s.endswith('.git'), remotes))
    commit = os.popen('git rev-parse HEAD').read().strip()
    os.chdir(wdir)
    # todo: validate output.
    return {'remotes': list(remotes), 'commit': commit}


def bsonify_yaml(file_path: Path):
    """Parse YAML file to valid document format."""
    yaml = YAML()
    with open(file_path, 'r', encoding='utf-8') as file:
        mapping = yaml.load(file)
    return SON(mapping)


def parse_requirements(requirements_path):
    """Might be handy for db entry."""
    return open(requirements_path).read().split()


def gather_paths(cursor: pymongo.cursor.Cursor) -> str:
    """Concatenate S3 paths of log_dirs in cursor's documents
    so that can be displayed via tensorboard --logdir_spec.
    """
    return ','.join((document['s3'] for document in cursor))


