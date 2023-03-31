import inspect
import re
from textwrap import dedent

from fennel.datasets import Dataset
from fennel.featuresets import Featureset


def _remove_empty_lines(source_code: str) -> str:
    source_code_lines = source_code.splitlines()
    source_code_lines = [line for line in source_code_lines if line.strip()]
    source_code = "\n".join(source_code_lines)
    source_code = dedent(source_code)
    source_code = "\n" + source_code + "\n"
    return source_code


def get_featureset_core_code(featureset: Featureset) -> str:
    # Keep all lines till class definition
    source_code = inspect.getsource(featureset.__fennel_original_cls__)
    for extractor in featureset.extractors:
        ext_code_lines, _ = inspect.getsourcelines(extractor.func)
        extractor_code = "".join(ext_code_lines)
        # Delete extractor code from source_code
        source_code = source_code.replace(extractor_code, "")
    return _remove_empty_lines(source_code)


def get_dataset_core_code(dataset: Dataset) -> str:
    source_code = inspect.getsource(dataset.__fennel_original_cls__)
    for pipeline in dataset._pipelines:
        pipe_code_lines, _ = inspect.getsourcelines(pipeline.func)
        pipeline_code = "".join(pipe_code_lines)
        # Delete pipeline code from source_code
        source_code = source_code.replace(pipeline_code, "")
    # Delete decorator @source() from source_code using regex
    source_code = re.sub(
        r"^\s*@source\(.*", "", source_code, flags=re.MULTILINE
    )
    return _remove_empty_lines(source_code)
