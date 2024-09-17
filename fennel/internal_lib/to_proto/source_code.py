import ast
import hashlib
import inspect
import os
import re
import sys
from textwrap import dedent, indent
from typing import Callable, Dict, Optional, List

import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import Dataset
from fennel.datasets.datasets import get_all_operators
from fennel.dtypes.dtypes import (
    FENNEL_STRUCT_SRC_CODE,
    get_fennel_struct,
    FENNEL_STRUCT_DEPENDENCIES_SRC_CODE,
)
from fennel.featuresets import Featureset
from fennel.lib import FENNEL_GEN_CODE_MARKER
from fennel.lib.includes.include_mod import FENNEL_INCLUDED_MOD
from fennel.utils import fennel_get_source

# Reserved column for server to do grouping windowing.
SERVER_CLUSTER_COLUMN = "_@@_cluster"


def del_spaces_tabs_and_newlines(s):
    return re.sub(r"[\s\n\t]+", "", s)


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
        extractor_code_indented = indent(dedent(extractor_code), " " * 4)
        # Delete extractor code from source_code
        source_code = source_code.replace(extractor_code_indented, "")
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


def get_text_between_ast_pointers(
    text: str,
    lineno: int,
    col_offset: int,
    end_lineno: int,
    end_col_offset: int,
    is_decorator: bool = False,
):
    # Split the text into lines
    lines = text.splitlines()

    if is_decorator:
        # for adding @
        col_offset = col_offset - 1

    # If the text is within a single line
    if lineno == end_lineno:
        return lines[lineno - 1][col_offset:end_col_offset]

    # Collect parts from multiple lines
    output = [
        lines[lineno - 1][col_offset:]
    ]  # Start from the initial column offset of the first line
    output.extend(
        lines[lineno : end_lineno - 1]
    )  # Add full lines between the first and last line
    output.append(
        lines[end_lineno - 1][:end_col_offset]
    )  # End at the final column offset of the last line
    return "\n".join(output)


class DecoratorRemover:
    def __init__(self, text: str, decorator_list: List[str]):
        self.text = text
        self.decorator_list = decorator_list
        self.decorator_lines: List[str] = []

    def get_decorators(self, node):
        decorator_list = [
            decorator
            for decorator in node.decorator_list
            if isinstance(decorator, ast.Call)
            and hasattr(decorator.func, "id")
            and getattr(decorator.func, "id") in self.decorator_list
        ]
        for decorator in decorator_list:
            self.decorator_lines.append(
                get_text_between_ast_pointers(
                    self.text,
                    decorator.lineno,
                    decorator.col_offset,
                    decorator.end_lineno,
                    decorator.end_col_offset,
                    True,
                )
            )
        return node

    def get_filtered_text(self) -> str:
        try:
            generator = ast.walk(ast.parse(self.text))
        except IndentationError:
            self.text = dedent(self.text)
            generator = ast.walk(ast.parse(self.text))
        next(
            (
                self.get_decorators(node)
                for node in generator
                if isinstance(node, (ast.ClassDef, ast.FunctionDef))
            ),
            None,
        )
        for line in self.decorator_lines:
            self.text = self.text.replace(line, "")
        return self.text.strip()


def remove_decorators(text: str, decorators: List[str]):
    return DecoratorRemover(text, decorators).get_filtered_text()


def get_dataset_core_code(dataset: Dataset) -> str:
    source_code = fennel_get_source(dataset.__fennel_original_cls__)
    for pipeline in dataset._pipelines:
        pipeline_code = fennel_get_source(pipeline.func)
        pipeline_code_indented = indent(dedent(pipeline_code), " " * 4)
        # Delete pipeline code from source_code
        source_code = source_code.replace(pipeline_code_indented, "")
        source_code = source_code.replace(pipeline_code, "")
    # Delete decorator @source() and @sink from source_code using regex
    source_code = remove_decorators(source_code, ["source", "sink"])
    # If python version 3.8 or below add @dataset decorator
    if sys.version_info < (3, 9):
        source_code = f"@dataset\n{dedent(source_code)}"

    # Add any struct definitions
    if hasattr(dataset, FENNEL_STRUCT_SRC_CODE):
        source_code = (
            getattr(dataset, FENNEL_STRUCT_SRC_CODE) + "\n" + source_code
        )

    return _remove_empty_lines(source_code)


def fix_lambda_text(source_text: str, lambda_func: Callable, line_num: int):
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


def lambda_to_python_regular_func(lambda_func: Callable):
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
        except (SyntaxError, ValueError, IOError, TypeError):
            source_text = inspect.getsource(lambda_func)
            try:
                source_ast = ast.parse(source_text)
            except IndentationError:
                source_text = dedent(source_text)
                try:
                    source_ast = ast.parse(source_text)
                except SyntaxError:
                    source_ast, source_text = fix_lambda_text(
                        source_text, lambda_func, line_num
                    )

    lambda_node = next(
        (node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)),
        None,
    )
    if lambda_node is None:  # could be a single line `def fn(x): ...`
        return None

    lambda_text = dedent(
        get_text_between_ast_pointers(
            source_text,
            lambda_node.lineno,
            lambda_node.col_offset,
            (
                lambda_node.end_lineno
                if lambda_node.end_lineno
                else lambda_node.lineno
            ),
            (
                lambda_node.end_col_offset
                if lambda_node.end_col_offset
                else lambda_node.col_offset
            ),
        )
    )
    check_ambiguous_lambda(lambda_func, lambda_text, line_num)
    return lambda_text


def check_ambiguous_lambda(
    lambda_func: Callable, lambda_text: str, line_num: int
):
    """
    This function checks if combining next line into lambda text, we get a valid lambda function.
    Then we can return ambiguous_lambda and ask user to define lambda within parenthesis to remove ambiguity.
    """
    # Get the next line and append to lambda text
    next_line = ""
    try:
        with open(lambda_func.__code__.co_filename, "r") as f:
            lines = f.readlines()
            if line_num < len(lines):
                next_line = lines[line_num].strip()
    except Exception:
        # In jupyter notebook, we don't have co_filename available.
        # So we are using fennel_get_source to get the source code and try to find the next line.
        # This is not perfect, and might not work for all cases.
        src_code = fennel_get_source(lambda_func)
        num_lines = src_code.count("\n")
        if num_lines <= 1:
            return
        next_line = src_code.split("\n")[1]

    # No Next line found
    if next_line == "":
        return

    # Next line already present in lambda_text
    if next_line in lambda_text:
        return

    # If Next line is part of the next operator, sometimes they can make a perfect lambda,
    # so we should return in this case.
    operators = get_all_operators()
    for operator in operators:
        if operator in next_line:
            return

    # Add next line to lambda text
    source_text = lambda_text.strip() + next_line
    try:
        source_ast = ast.parse(source_text)
        lambda_node = next(
            (
                node
                for node in ast.walk(source_ast)
                if isinstance(node, ast.Lambda)
            ),
            None,
        )
        if lambda_node is not None:
            new_lambda_text = dedent(
                get_text_between_ast_pointers(
                    source_text,
                    lambda_node.lineno,
                    lambda_node.col_offset,
                    (
                        lambda_node.end_lineno
                        if lambda_node.end_lineno
                        else lambda_node.lineno
                    ),
                    (
                        lambda_node.end_col_offset
                        if lambda_node.end_col_offset
                        else lambda_node.col_offset
                    ),
                )
            )
            if new_lambda_text != lambda_text:
                raise ValueError(
                    f"Not able to choose lambda function between {lambda_text} and {new_lambda_text}. "
                    f"If lambda contains more than one line, please define the function within the parentheses."
                )
    except SyntaxError:
        return


def get_all_imports() -> str:
    # Add imports if not present
    imports = [
        "import pandas as pd",
        "import numpy as np",
        "import json",
        "import os",
        "import sys",
        "from datetime import datetime, date",
        "import time",
        "import random",
        "import math",
        "import re",
        "from enum import Enum",
        "from typing import *",
        "from collections import defaultdict",
    ]

    fennel_imports = [
        "from fennel.connectors.connectors import *",
        "from fennel.datasets import *",
        "from fennel.featuresets import *",
        "from fennel.featuresets import feature",
        "from fennel.featuresets import feature as F",
        "from fennel.lib.expectations import *",
        "from fennel.internal_lib.schema import *",
        "from fennel.internal_lib.utils import *",
        "from fennel.lib.params import *",
        "from fennel.dtypes.dtypes import *",
        "from fennel.datasets.aggregate import *",
        "from fennel.lib.includes import includes",
        "from fennel.lib.metadata import meta",
        "from fennel.lib import secrets, bucketize",
        "from fennel.datasets.datasets import dataset_lookup",
        "from fennel.expr import col, lit, when",
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


def wrap_function(
    dataset_name: str,
    dataset_code: str,
    lib_generated_code: str,
    op_pycode: pycode_proto.PyCode,
    is_filter=False,
    is_assign=False,
    is_summary=False,
    column_name: Optional[str] = None,
) -> pycode_proto.PyCode:
    gen_func_name = hashlib.sha256(
        del_spaces_tabs_and_newlines(op_pycode.core_code).encode()
    ).hexdigest()[:10]

    gen_function_name = f"wrapper_{gen_func_name}"
    if op_pycode.entry_point == "<lambda>":
        wrapper_function = f"""
@classmethod
def {gen_function_name}(cls, *args, **kwargs):
    _fennel_internal = {op_pycode.generated_code.strip()}
    return _fennel_internal(*args, **kwargs)
    """
    else:
        wrapper_function = f"""
@classmethod
def {gen_function_name}(cls, *args, **kwargs):
    {indent(op_pycode.generated_code, "    ")}
    return {op_pycode.entry_point}(*args, **kwargs)
    """
    wrapper_function = indent(dedent(wrapper_function), "    ")
    gen_code = (
        dedent(lib_generated_code)
        + "\n"
        + dataset_code
        + "\n"
        + wrapper_function
    )

    new_entry_point = f"{dataset_name}_{gen_function_name}"
    ret_code = f"""
def {new_entry_point}(*args, **kwargs):
    _fennel_internal = {dataset_name}.__fennel_original_cls__
    return getattr(_fennel_internal, "{gen_function_name}")(*args, **kwargs)
    """
    gen_code = gen_code + "\n" + dedent(ret_code)

    if is_filter:
        old_entry_point = new_entry_point
        new_entry_point = f"{old_entry_point}_filter"
        gen_code += f"""
def {new_entry_point}(df: pd.DataFrame) -> pd.DataFrame:
    return df[{old_entry_point}(df)]
    """
    if is_assign:
        old_entry_point = new_entry_point
        new_entry_point = f"{old_entry_point}_assign"
        gen_code += f"""
def {new_entry_point}(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign({column_name}={old_entry_point})
    """
    if is_summary:
        old_entry_point = new_entry_point
        new_entry_point = f"{old_entry_point}_summary"
        gen_code += f"""
def {new_entry_point}(df: pd.DataFrame) -> pd.DataFrame:
    df = df.groupby('{SERVER_CLUSTER_COLUMN}').apply({old_entry_point}).reset_index(name='result').sort_values('{SERVER_CLUSTER_COLUMN}')
    return df['result']
    """

    return pycode_proto.PyCode(
        entry_point=f"{new_entry_point}",
        generated_code=gen_code,
        core_code=op_pycode.core_code,
        source_code=op_pycode.source_code,
        includes=op_pycode.includes,
        imports=op_pycode.imports,
    )
