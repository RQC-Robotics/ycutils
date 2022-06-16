import csv
import json
from typing import Optional, Dict, List, Iterable
import tensorboard.backend.event_processing.event_accumulator as tbea


class TBParser:
    """Parse tensorboard logs to required format.
    For now handles only ScalarEvents.
    Hyperparams are second requested.
    """
    def __init__(self, path: str, size_guidance: int = 0):
        self._acc = self._prepare_accumulator(path, size_guidance)

    def to_dict(
            self,
            suffix_keys: Optional[Iterable] = None,
            strip_scalars: bool = False
    ) -> Dict[str, List[tbea.SCALARS]]:
        """Extract dictionary of ScalarEvents for required keys."""
        if isinstance(suffix_keys, str):
            suffix_keys = (suffix_keys, )

        def _predicate(key):
            if suffix_keys:
                return any(map(key.endswith, suffix_keys))
            else:
                return True

        def _strip_scalars(key):
            scalars = self._acc.Scalars(key)
            if strip_scalars:
                scalars = list(map(lambda sc: sc.value, scalars))
            return scalars

        return {key: _strip_scalars(key) for key in self._acc.scalars.Keys() if _predicate(key)}

    def to_csv(
            self,
            path: str,
            suffix_keys: Optional[Iterable] = None,
    ) -> None:
        dict_log = self.to_dict(suffix_keys=suffix_keys, strip_scalars=True)
        row_numbers = set(map(len, dict_log.values()))
        assert len(row_numbers) == 1, "Number of rows differ per column."

        with open(path, 'w') as log_file:
            writer = csv.DictWriter(log_file, fieldnames=dict_log.keys())
            writer.writeheader()

            keys = dict_log.keys()
            rows = map(lambda values: {k: v for k, v in zip(keys, values)}, zip(*dict_log.values()))
            writer.writerows(rows)

    def to_npz(self, path) -> None:
        """To reduce memory usage."""

    def to_json(
            self,
            path: str,
            suffix_keys: Optional[Iterable] = None
    ) -> None:
        """Since DVC support JSON."""
        dict_log = self.to_dict(suffix_keys=suffix_keys, strip_scalars=True)
        with open(path, 'w') as file:
            json.dump(dict_log, file)

    @staticmethod
    def _prepare_accumulator(path: str, size_guidance: int) -> tbea.EventAccumulator:
        acc = tbea.EventAccumulator(path, size_guidance={tbea.SCALARS: size_guidance})
        return acc.Reload()

    @classmethod
    def detect_logs(cls, directory: str) -> List[str]:
        return list(filter(
            tbea.io_wrapper.IsSummaryEventsFile,
            tbea.io_wrapper.ListDirectoryAbsolute(directory)
        ))
