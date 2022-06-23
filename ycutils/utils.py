"""Helper functions."""
from typing import Union, Iterable, Optional, Dict, Any, List

import os
import csv
import pathlib
from urllib.parse import quote_plus

import boto3
import pymongo
import pymongo.database
from bson.son import SON
from ruamel.yaml import YAML


Path = Union[str, pathlib.Path]


def make_mongo_client(
            username: str,
            password: str,
            authSource: str,
            host: Union[str, Iterable[str]],
            port: int = 27018,
            replicaSet: str = "rs01",
            tls=True,
            tlsCAFile: str ="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
) -> pymongo.MongoClient:
    """Construct proper url and MongoClient from the inputs.
    While not all arguments are always required (ex. db is public),
    here the initialization is stricter.

    Arguments are inherited from the corresponding pymongo.Client fields.
    """
    username, password = map(quote_plus, (username, password))
    url = f"mongodb://{username}:{password}@" \
          f"{host}/?replicaSet={replicaSet}&authSource={authSource}"
    if tls:
        url += f"&tls=true&tlsCAFile={tlsCAFile}"
    else:
        tlsCAFile = None
    return pymongo.MongoClient(
        host,
        username=username,
        password=password,
        tls=tls,
        tlsCAFile=tlsCAFile,
        replicaSet=replicaSet,
        authSource=authSource,
        port=port,
    )


def make_s3(
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = "https://storage.yandexcloud.net"
) -> boto3.client:
    """Nothing more than specifying endpoint on boto3 client creation."""
    return boto3.client(
        service_name="s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )


def scan_git(git_dir: Path = ".") -> Dict[str, Any]:
    """Infer useful git info from the directory."""
    wdir = os.getcwd()
    os.chdir(git_dir)
    is_git_dir = os.popen("git rev-parse --is-inside-work-tree").read().strip() == "true"
    if not is_git_dir:
        return None
    remotes = os.popen("git remote -v").read().split()
    remotes = set(filter(lambda s: s.endswith(".git"), remotes))
    commit = os.popen("git rev-parse HEAD").read().strip()
    os.chdir(wdir)
    # todo: validate output.
    return [{
        "url": list(remotes),
        "commit": commit,
        "dirty": bool(os.system("git diff --quiet"))
    }]


def bsonify_yaml(file_path: Path) -> SON:
    """Parse YAML file to valid document format.
    Can be used to parse config files written im YAML.
    ruamel.YAML is used instead of outdated PyYAML."""
    yaml = YAML()
    with open(file_path, "r", encoding="utf-8") as file:
        mapping = yaml.load(file)
    return SON(mapping)


def bsonify_csv(file_path: Path) -> List[SON[str, Any]]:
    """Load csv file in the valid document format."""
    rows = []
    with open(file_path, encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            rows.append(SON(row))
        return rows


def parse_requirements(requirements_path: Path):
    """List of requirements. Might be handy as db entry."""
    return open(requirements_path, encoding="utf-8").read().split()


def gather_paths(cursor: pymongo.cursor.Cursor) -> str:
    """Concatenate S3 paths of log_dirs in cursor"s documents
    so that can be displayed via tensorboard --logdir_spec.
    """
    return ",".join((document["s3"] for document in cursor))
