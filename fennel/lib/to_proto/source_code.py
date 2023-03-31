import inspect
import re
import sys
from textwrap import dedent, indent

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
    source_code = dedent(inspect.getsource(featureset.__fennel_original_cls__))
    for extractor in featureset.extractors:
        ext_code_lines, _ = inspect.getsourcelines(extractor.func)
        extractor_code = "".join(ext_code_lines)
        # Delete extractor code from source_code
        source_code = source_code.replace(extractor_code, "")
    # If python version 3.8 or below add @feature decorator
    if sys.version_info < (3, 9):
        source_code = f"@featureset\n{dedent(source_code)}"
    return _remove_empty_lines(source_code)


def get_dataset_core_code(dataset: Dataset) -> str:
    source_code = dedent(inspect.getsource(dataset.__fennel_original_cls__))
    for pipeline in dataset._pipelines:
        pipe_code_lines, line_number = inspect.getsourcelines(pipeline.func)
        pipeline_code = "".join(pipe_code_lines)
        pipeline_code = indent(dedent(pipeline_code), " " * 4)
        # Delete pipeline code from source_code
        source_code = source_code.replace(pipeline_code, "")
    # Delete decorator @source() from source_code using regex
    source_code = re.sub(
        r"^\s*@source\(.*", "", source_code, flags=re.MULTILINE
    )
    # If python version 3.8 or below add @dataset decorator
    if sys.version_info < (3, 9):
        source_code = f"@dataset\n{dedent(source_code)}"
    return _remove_empty_lines(source_code)
