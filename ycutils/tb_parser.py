from typing import Optional, Dict, List, Iterable, Literal, Union

import csv
import json
import datetime
from bson import json_util
import tensorboard.backend.event_processing.event_accumulator as tbea


class TBParser:
    """Parse tensorboard Events File. Currently handles only ScalarEvents."""
    def __init__(self, path: str, size_guidance: int = 10000):
        """See tensorboard docs for size guidance explanation.
        Set size_guidance=0 to load all the data at once."""
        acc = tbea.EventAccumulator(path, size_guidance={tbea.SCALARS: size_guidance})
        self._acc = acc.Reload()

    def to_dict(
            self,
            suffix_keys: Optional[Iterable] = None,
            mode: Literal["strip", "unpack", None] = None
    ) -> Dict[str, Union[List[float], Dict[str, List[float]], List[tbea.ScalarEvent]]]:
        """Extract scalars from the reservoir.

        suffix_keys - select specific scalar by its suffix.
        mode - returned values can be:
            None: tbea.ScalarEvents(value, wall_time, step),
            strip: plain value,
            unpack: Dict[value, wall_time, step]
        """
        if isinstance(suffix_keys, str):
            suffix_keys = (suffix_keys, )

        def _predicate(key):
            if suffix_keys:
                return any(map(key.endswith, suffix_keys))
            return True

        def _process_scalars(key):
            scalars = self._acc.Scalars(key)
            if mode == "strip":
                scalars = list(map(lambda sc: sc.value, scalars))
            elif mode == "unpack":
                scalars = zip(*map(
                    lambda sc: (datetime.datetime.fromtimestamp(sc.wall_time),
                                sc.value,
                                sc.step),
                    scalars))
                scalars = dict(zip(("timestamps", "values", "steps"), scalars))
            return scalars

        return {key: _process_scalars(key) for key in self._acc.scalars.Keys() if _predicate(key)}

    def to_csv(
            self,
            file_path: str,
            suffix_keys: Optional[Iterable] = None,
    ) -> None:
        """Parse and save as CSV."""
        # TODO: make compatible with "unpack" mode.
        dict_log = self.to_dict(suffix_keys=suffix_keys, mode="strip")
        row_numbers = set(map(len, dict_log.values()))
        assert len(row_numbers) == 1, "Number of rows differ per column."

        with open(file_path, "w", encoding="utf-8") as log_file:
            writer = csv.DictWriter(log_file, fieldnames=dict_log.keys())
            writer.writeheader()

            keys = dict_log.keys()
            rows = map(lambda values: dict(zip(keys, values)), zip(*dict_log.values()))
            writer.writerows(rows)

    def to_npz(self, file_path) -> None:
        """To reduce memory usage."""
        raise NotImplementedError

    def to_json(
            self,
            file_path: str,
            suffix_keys: Optional[Iterable] = None,
            mode: Literal["strip", "unpack"] = "strip"
    ) -> None:
        """Parse and save as JSON."""
        dict_log = self.to_dict(suffix_keys=suffix_keys, mode=mode)
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(dict_log, file, default=json_util.default)

    @classmethod
    def detect_logs(cls, directory: str) -> List[str]:
        """Iterate over directory to find valid EventsFiles."""
        return list(filter(
            tbea.io_wrapper.IsSummaryEventsFile,
            tbea.io_wrapper.ListDirectoryAbsolute(directory)
        ))
