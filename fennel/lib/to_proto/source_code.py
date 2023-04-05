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
        extractor_code = indent(dedent(extractor_code), " " * 4)
        # Delete extractor code from source_code
        source_code = source_code.replace(extractor_code, "")

    # If python version 3.8 or below add @feature decorator
    if sys.version_info < (3, 9):
        source_code = f"@featureset\n{dedent(source_code)}"
    z = _remove_empty_lines(source_code)
    return z


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


def lambda_to_python_regular_func(lambda_func):
    """Return the source of a (short) lambda function.
    If it's impossible to obtain, returns None.
    """
    try:
        source_lines, _ = inspect.getsourcelines(lambda_func)
    except (IOError, TypeError):
        return None

    # skip `def`-ed functions and long lambdas
    if len(source_lines) != 1:
        return None

    source_text = os.linesep.join(source_lines).strip()

    # find the AST node of a lambda definition
    # so we can locate it in the source code
    source_ast = ast.parse(source_text)
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
    lambda_text = source_text[lambda_node.col_offset :]
    lambda_body_text = source_text[lambda_node.body.col_offset :]
    min_length = len("lambda:_")  # shortest possible lambda expression
    while len(lambda_text) > min_length:
        # Ensure code compiles
        try:
            code = compile(lambda_body_text, "<unused filename>", "eval")

            # Byte code matching.
            if len(code.co_code) == len(lambda_func.__code__.co_code):
                return lambda_text
        except SyntaxError:
            pass
        lambda_text = lambda_text[:-1]
        lambda_body_text = lambda_body_text[:-1]

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
        code = dedent(lambda_to_python_regular_func(func))
    else:
        code = dedent(inspect.getsource(func))

    gen_code = gen_code + "\n" + code + "\n"

    return pycode_proto.PyCode(
        source_code=code,
        generated_code=gen_code,
        core_code=code,
        entry_point=entry_point,
        includes=dependencies,
        imports=get_all_imports(),
    )
