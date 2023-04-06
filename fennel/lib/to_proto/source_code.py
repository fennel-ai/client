import ast
import inspect
import os
import re
import sys
from textwrap import dedent, indent

from typing import Callable

import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import Dataset
from fennel.featuresets import Featureset
from fennel.lib.includes.include_mod import FENNEL_INCLUDED_MOD
from fennel.utils import fennel_get_source


def _remove_empty_lines(source_code: str) -> str:
    source_code_lines = source_code.splitlines()
    source_code_lines = [line for line in source_code_lines if line.strip()]
    source_code = "\n".join(source_code_lines)
    source_code = dedent(source_code)
    source_code = "\n" + source_code + "\n"
    return source_code


def get_featureset_core_code(featureset: Featureset) -> str:
    # Keep all lines till class definition
    source_code = fennel_get_source(featureset.__fennel_original_cls__)
    for extractor in featureset.extractors:
        extractor_code = fennel_get_source(extractor.func)
        extractor_code = indent(dedent(extractor_code), " " * 4)
        # Delete extractor code from source_code
        source_code = source_code.replace(extractor_code, "")

    # If python version 3.8 or below add @feature decorator
    if sys.version_info < (3, 9):
        source_code = f"@featureset\n{dedent(source_code)}"
    z = _remove_empty_lines(source_code)
    return z


def get_dataset_core_code(dataset: Dataset) -> str:
    source_code = fennel_get_source(dataset.__fennel_original_cls__)
    for pipeline in dataset._pipelines:
        # pipe_code_lines, _ = inspect.getsourcelines(pipeline.func)
        pipeline_code = fennel_get_source(pipeline.func)
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


def lambda_to_python_regular_func(lambda_func):
    """Return the source of a (short) lambda function.
    If it's impossible to obtain, returns None.
    """
    try:
        source_lines, line_num = inspect.getsourcelines(lambda_func)
    except (IOError, TypeError):
        return None

    if len(source_lines) == 2:
        # Fix the case when first line ends with a backslash by concatenating
        # the second line
        if source_lines[0].strip().endswith("\\"):
            source_lines[0] = (
                source_lines[0].rstrip()[:-1] + source_lines[1].strip()
            )
        else:
            source_lines[0] = source_lines[0].strip() + source_lines[1].strip()
        source_lines = source_lines[:1]

    # skip `def`-ed functions and long lambdas
    if len(source_lines) != 1:
        raise ValueError(
            f"Lambda function `{source_lines}` is too long, please compress "
            "the function into a single line or use a regular function"
        )

    source_text = os.linesep.join(source_lines).strip()

    # find the AST node of a lambda definition
    # so we can locate it in the source code
    err = ValueError(
        f"Unable to parse lambda function source code "
        f"`{source_text}`, please compress the function into "
        "a single line or use a regular function"
    )
    try:
        source_ast = ast.parse(source_text)
    except SyntaxError:
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
        except SyntaxError:
            raise err

    lambda_node = next(
        (node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)),
        None,
    )
    if lambda_node is None:  # could be a single line `def fn(x): ...`
        return None

    # HACK: Since we can (and most likely will) get source lines
    # where lambdas are just a part of bigger expressions, they will have
    # some trailing junk after their definition.
    #
    # Unfortunately, AST nodes only keep their _starting_ offsets
    # from the original source, so we have to determine the end ourselves.
    # We do that by gradually shaving extra junk from after the definition.
    lambda_text = source_text[lambda_node.col_offset :]  # noqa
    lambda_body_text = source_text[lambda_node.body.col_offset :]  # noqa
    min_length = len("lambda:_")  # shortest possible lambda expression
    backup_code = ""
    while len(lambda_text) > min_length:
        # Ensure code compiles
        try:
            code = compile(lambda_body_text, "<unused filename>", "eval")
            if backup_code == "":
                backup_code = lambda_text

            # Byte code matching.
            if len(code.co_code) == len(lambda_func.__code__.co_code):
                if lambda_text[-1] == ",":
                    lambda_text = lambda_text[:-1]
                return lambda_text
        except SyntaxError:
            pass
        lambda_text = lambda_text[:-1]
        lambda_body_text = lambda_body_text[:-1]

    # TODO: This is a hack for python 3.11 and should be removed when
    # we have a better solution.
    # We try to return the longest compilable code, but if we can't, we return
    # None.
    if sys.version_info >= (3, 11):
        if len(backup_code) > 0:
            if backup_code[-1] == ",":
                backup_code = backup_code[:-1]
            return backup_code

    return None


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
        "from fennel.lib.metadata import meta",
        "from fennel.lib.includes import includes",
        "from fennel.datasets import *",
        "from fennel.featuresets import *",
        "from fennel.lib.schema import *",
        "from fennel.datasets.datasets import dataset_lookup",
    ]

    return "\n".join(imports + fennel_imports) + "\n"


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
        num_lines = len(inspect.getsourcelines(func)[0])
        if num_lines > 1:
            raise ValueError(
                "Lambda functions with more than 1 line are not supported."
            )
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
