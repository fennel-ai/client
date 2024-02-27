import ast
import inspect
import os
import re
import sys
from textwrap import dedent, indent
from typing import Callable, Dict

import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import Dataset
from fennel.dtypes.dtypes import (
    FENNEL_STRUCT_SRC_CODE,
    get_fennel_struct,
    FENNEL_STRUCT_DEPENDENCIES_SRC_CODE,
)
from fennel.featuresets import Featureset
from fennel.lib import FENNEL_GEN_CODE_MARKER
from fennel.lib.includes.include_mod import FENNEL_INCLUDED_MOD
from fennel.utils import fennel_get_source


def _remove_empty_lines(source_code: str) -> str:
    source_code_lines = source_code.splitlines()
    source_code_lines = [line for line in source_code_lines if line.strip()]
    source_code = "\n".join(source_code_lines)
    source_code = dedent(source_code)
    source_code = "\n" + source_code + "\n"
    return source_code


def get_featureset_core_code(
    featureset: Featureset,
) -> str:
    # Keep all lines till class definition
    source_code = fennel_get_source(featureset.__fennel_original_cls__)
    for extractor in featureset.extractors:
        if not extractor.func:
            continue
        extractor_code = fennel_get_source(extractor.func)
        extractor_code = indent(dedent(extractor_code), " " * 4)
        # Delete extractor code from source_code
        source_code = source_code.replace(extractor_code, "")

    # If python version 3.8 or below add @feature decorator
    if sys.version_info < (3, 9):
        source_code = f"@featureset\n{dedent(source_code)}"
    return _remove_empty_lines(source_code)


def get_featureset_gen_code(
    featureset: Featureset, fs_obj_map: Dict[str, Featureset]
) -> str:
    """
    Now that featuresets can hold features, which can be auto generated, we need to ensure
    that all the code required for the features and their extractors is present in the
    generated code.

    :param featureset:
    :return:
    """
    gen_code = ""
    for ds in featureset.get_dataset_dependencies():
        gen_code = get_dataset_core_code(ds) + "\n" + gen_code + "\n"

    for fs in featureset.get_featureset_dependencies():
        if fs not in fs_obj_map:
            raise ValueError(
                f"Featureset `{fs}` is required by `{featureset._name}` but is not present in the sync call. Please "
                f"ensure that all featuresets are present in the sync call."
            )
        if fs == featureset._name:
            continue
        fs_code = get_featureset_gen_code(fs_obj_map[fs], fs_obj_map)
        gen_code = dedent(fs_code) + "\n" + gen_code + "\n"

    # Adding code of struct if any struct is part of annotation of any Feature in the Featureset
    for feature in featureset.features:
        fennel_struct = get_fennel_struct(feature.dtype)
        if fennel_struct:
            if hasattr(fennel_struct, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE):
                code = dedent(
                    getattr(fennel_struct, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE)
                )
                if code not in gen_code:
                    gen_code = gen_code + "\n" + code + "\n"
            if hasattr(fennel_struct, FENNEL_STRUCT_SRC_CODE):
                code = dedent(getattr(fennel_struct, FENNEL_STRUCT_SRC_CODE))
                if code not in gen_code:
                    gen_code = gen_code + "\n" + code + "\n"

    source_code = gen_code + "\n" + get_featureset_core_code(featureset)
    return _remove_empty_lines(source_code)


def remove_source_decorator(text):
    # Define the regular expression pattern for the @source decorator block
    pattern = r"^\s*@source\((?:.|\n)*?\)\s*$"
    # Remove @source decorator using the regex pattern with re.MULTILINE and re.DOTALL flags
    result = re.sub(pattern, "", text, flags=re.MULTILINE | re.DOTALL)
    return result.strip()


def get_dataset_core_code(dataset: Dataset) -> str:
    source_code = fennel_get_source(dataset.__fennel_original_cls__)
    for pipeline in dataset._pipelines:
        pipeline_code = fennel_get_source(pipeline.func)
        pipeline_code = indent(dedent(pipeline_code), " " * 4)
        # Delete pipeline code from source_code
        source_code = source_code.replace(pipeline_code, "")
    # Delete decorator @source() from source_code using regex
    source_code = remove_source_decorator(source_code)

    # If python version 3.8 or below add @dataset decorator
    if sys.version_info < (3, 9):
        source_code = f"@dataset\n{dedent(source_code)}"

    # Add any struct definitions
    if hasattr(dataset, FENNEL_STRUCT_SRC_CODE):
        source_code = (
            getattr(dataset, FENNEL_STRUCT_SRC_CODE) + "\n" + source_code
        )

    return _remove_empty_lines(source_code)


def fix_lambda_text(source_text, lambda_func, line_num):
    # Try to fix the syntax error by removing any junk in the start
    if source_text.startswith("."):
        source_text = source_text[1:]

    err = ValueError(
        f"Unable to parse lambda function source code "
        f"`{source_text}`, please ensure there are no comments in the lambda "
        f"or use a regular function"
    )
    try:
        source_ast = ast.parse(source_text)
        return source_ast, source_text
    except SyntaxError:
        pass

    # Try to fix the syntax error by combining the lines
    # Find source code in line_num - 1 from the file
    with open(lambda_func.__code__.co_filename, "r") as f:
        lines = f.readlines()
        if line_num - 2 >= len(lines):
            raise err
        line_above = lines[line_num - 2]
    # Put source_text at end of line_above
    source_text = line_above.strip() + source_text.strip()
    try:
        source_ast = ast.parse(source_text)
        return source_ast, source_text
    except SyntaxError:
        raise err


def lambda_to_python_regular_func(lambda_func):
    """Return the source of a (short) lambda function.
    If it's impossible to obtain, returns None.
    """
    try:
        source_lines, line_num = inspect.getsourcelines(lambda_func)
    except (IOError, TypeError):
        return None

    source_lines = [line.strip() for line in source_lines]
    i = 0
    while i < len(source_lines) - 1:
        if source_lines[i].endswith("\\"):
            source_lines[i] = source_lines[i][:-1] + source_lines[i + 1]
            del source_lines[i + 1]
        else:
            i += 1

    source_text = os.linesep.join(source_lines).strip()
    # In case source_text is across multiple lines join them
    source_text = source_text.replace("\n", " ")
    source_text = source_text.replace("\r", " ")

    # find the AST node of a lambda definition
    # so we can locate it in the source code
    try:
        source_ast = ast.parse(source_text)
    except SyntaxError:
        try:
            source_ast, source_text = fix_lambda_text(
                source_text, lambda_func, line_num
            )
        except Exception as e:
            raise e

    lambda_node = next(
        (node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)),
        None,
    )
    if lambda_node is None:  # could be a single line `def fn(x): ...`
        return None

    lambda_text = dedent(
        source_text[lambda_node.col_offset : lambda_node.end_col_offset]
    )
    return lambda_text


def get_all_imports() -> str:
    # Add imports if not present
    imports = [
        "import pandas as pd",
        "import numpy as np",
        "import json",
        "import os",
        "import sys",
        "from datetime import datetime",
        "import time",
        "import random",
        "import math",
        "import re",
        "from typing import *",
        "from collections import defaultdict",
    ]

    fennel_imports = [
        "from fennel.sources.sources import *",
        "from fennel.datasets import *",
        "from fennel.featuresets import *",
        "from fennel.lib.expectations import *",
        "from fennel.internal_lib.schema import *",
        "from fennel.internal_lib.utils import *",
        "from fennel.lib.params import *",
        "from fennel.dtypes.dtypes import *",
        "from fennel.datasets.aggregate import *",
        "from fennel.lib.includes import includes",
        "from fennel.lib.metadata import meta",
        "from fennel.datasets.datasets import dataset_lookup",
    ]

    gen_code_marker = f"{FENNEL_GEN_CODE_MARKER}=True\n"
    return gen_code_marker + "\n".join(imports + fennel_imports) + "\n"


def to_includes_proto(func: Callable) -> pycode_proto.PyCode:
    dependencies = []
    gen_code = ""
    if hasattr(func, FENNEL_INCLUDED_MOD):
        for f in getattr(func, FENNEL_INCLUDED_MOD):
            dep = to_includes_proto(f)
            gen_code = dedent(dep.generated_code) + "\n" + gen_code
            dependencies.append(dep)

    entry_point = func.__name__
    if func.__name__ == "<lambda>":
        # generate a random name for the lambda function
        entry_point = "<lambda>"
        code = lambda_to_python_regular_func(func)
        if code is None:
            raise ValueError(
                "Lambda function parsing failed, please use regular python "
                "function instead."
            )
        code = dedent(code)
    else:
        code = fennel_get_source(func)

    gen_code = gen_code + "\n" + code + "\n"

    return pycode_proto.PyCode(
        source_code=code,
        generated_code=gen_code,
        core_code=code,
        entry_point=entry_point,
        includes=dependencies,
        imports=get_all_imports(),
    )
