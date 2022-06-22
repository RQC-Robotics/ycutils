"""Python objects describing entries that should present in database."""
from typing import TypedDict, Optional, List, TypeVar, Tuple

import datetime
import collections
import bson.raw_bson
from bson.objectid import ObjectId

from . import utils

Document = TypeVar('Document', collections.MutableMapping, bson.raw_bson.RawBSONDocument)


class Experiment(TypedDict):
    name: str
    repositories: List[Document]
    dependencies: List[str]


class ScalarMetric(TypedDict):
    _id: int
    run_id: int
    name: str
    steps: List[int]
    timestamps: List[datetime.datetime]
    values: List[float]


# Those fields are present in the sacred.
# Ideally they should be filled programmaticly,
#   but for now they are hardcoded.
_RUNS_PLACEHOLDER=dict(
    format="PseudoSacred-0.0.1",
    status="COMPLETED",
    result=None,
    resources=[],
    artifacts=[],  # The only useful field.
    captured_out=None,
    host=[]
)


class RunEntry(TypedDict):
    _id: int
    config: Document
    experiment: Experiment
    info: Document

    # host: Optional[Document]
    # start_time: Optional[datetime.datetime]
    # stop_time: Optional[datetime.datetime]


def make_log_entries(
        _id: int,
        name: str,
        config: Document,
        metrics: Document,
        git_parent_dir: str = '.',
        requirements_file: Optional[utils.Path] = None,
) -> Tuple[Document, Document, Document]:
    """Mimic MongoObserver in creating 3 documents.

    Documents:
        db.runs <- general_info.
        db.metrics <- metrics corresponding to the experiment.
        db.files, db.chunks <- store artefacts in BSON format with the help of GridFS.
    """
    git_info = utils.scan_git(git_parent_dir)
    dependencies = utils.parse_requirements(requirements_file)
    experiment = Experiment(name=name, repositories=git_info, dependencies=dependencies)

    metrics_id = ObjectId()
    metrics_entries = []
    metrics_links = []
    for name, data in metrics.items():
        metric = ScalarMetric(_id = metrics_id,
                              run_id=_id,
                              name=name,
                              steps=data['steps'],
                              timestamps=data['timestamps'],
                              values=data['values'])
        metrics_entries.append(metric)
        metrics_links.append({'name': name, 'id': metrics_id})

    runs_entry = RunEntry(_id=_id,
                          config=config,
                          experiment=experiment,
                          info={'metrics': metrics_links}
                          )
    runs_entry.update(_RUNS_PLACEHOLDER)

    files_entry = (None, None)  # todo: store artifacts in db

    return runs_entry, metrics_entries, files_entry
