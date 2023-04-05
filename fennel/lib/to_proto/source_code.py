import inspect
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


def lambda_to_python_regular_func(lambda_str, func_name="lambda_func"):
    """Converts a lambda function to a regular python function. Only works
    for 1 line lambda functions.
    """
    # Parse the lambda string to get the argument name and expression
    lambda_parts = lambda_str.split(":", 1)
    # Pick arg name from the part between "lambda" and ":" using regex
    arg_name = re.search(r"lambda\s+(\w+)", lambda_str).group(1)
    expression = lambda_parts[1].strip()

    # Remove any unbalanced parentheses in expression
    # This is to avoid any syntax errors in the generated code
    while expression.count("(") != expression.count(")"):
        if expression.count("(") > expression.count(")"):
            expression = expression[1:]
        else:
            expression = expression[:-1]

    # Remove any trailing commas in the expression
    # This is to avoid any syntax errors in the generated code
    while expression.endswith(","):
        expression = expression[:-1]

    # Generate the source code for the regular function
    source_code = f"""
def {func_name}({arg_name}):
    return {expression}
"""
    source_code = "\n" + dedent(source_code.strip()) + "\n"

    # Check if the generated code is valid
    try:
        exec(source_code, {}, {})
    except Exception:
        raise ValueError(
            f"Unable to convert lambda function to regular function. "
            f"for {lambda_str}. Please use a regular function instead of a lambda."
        )
    return source_code


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
        entry_point = "lambda_func_" + str(hash(func))
        code = dedent(
            lambda_to_python_regular_func(inspect.getsource(func), entry_point)
        )
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
