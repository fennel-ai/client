from __future__ import annotations

import ast
import dataclasses
import datetime
import hashlib
import inspect
import json
import sys
import textwrap
from typing import Any, cast, Callable, Dict, List, Tuple, Union

from pandas import DataFrame

import fennel._vendor.astunparse as astunparse
import fennel._vendor.requests as requests  # type: ignore
from fennel._vendor.pydantic.typing import get_args, get_origin  # type: ignore

Tags = Union[List[str], Tuple[str, ...], str]

FHASH_ATTR = "__fennel_fhash__"

FENNEL_VIRTUAL_FILE = "__fennel_virtual_file__"


def new_getfile(object, _old_getfile=inspect.getfile):
    if not inspect.isclass(object):
        return _old_getfile(object)

    # Lookup by parent module (as in current inspect)
    if hasattr(object, "__module__"):
        object_ = sys.modules.get(object.__module__)
        if hasattr(object_, "__file__"):
            return object_.__file__

    for name, member in inspect.getmembers(object):
        if (
            inspect.isfunction(member)
            and object.__qualname__ + "." + member.__name__
            == member.__qualname__
        ):
            return inspect.getfile(member)

    class_methods = [
        method
        for method in dir(object)
        if inspect.ismethod(getattr(object, method))
    ]
    for method in class_methods:
        func = getattr(object, method)
        file_name = inspect.getfile(func)
        if "ipykernel" in file_name:
            return inspect.getfile(func)

    # Get file name
    if hasattr(object, FENNEL_VIRTUAL_FILE):
        filename = getattr(object, FENNEL_VIRTUAL_FILE)
        if len(filename) > 0:
            return filename
    raise TypeError("Source for {!r} not found".format(object))


def fennel_get_source(obj: Any) -> str:
    try:
        code = inspect.getsource(obj)
    except Exception:
        save_get_file = inspect.getfile
        inspect.getfile = new_getfile
        ds_code_lines = inspect.getsource(obj)
        inspect.getfile = save_get_file
        code = "".join(ds_code_lines)
    return textwrap.dedent(code)


def check_response(response: requests.Response):  # type: ignore
    """Check the response from the server and raise an exception if the response is not OK"""
    if response.status_code != 200:
        if response.headers.get("content-type") == "application/json":
            response_json = response.json()
            if "err_msg" in response_json:
                msg = response_json["err_msg"]
            else:
                msg = response.text
            if "err_diff" in response_json:
                print(response_json["err_diff"])
            raise Exception(
                "Server returned: {}, {}: {}".format(
                    response.status_code, response.reason, msg
                )
            )
        else:
            raise Exception(
                "Server returned: {}, {}, {}".format(
                    response.status_code,
                    response.reason,
                    response.text,
                )
            )


def del_namespace(obj, depth):
    if not hasattr(obj, "__dict__"):
        return
    if depth > 10:
        return
    if "namespace" in obj.__dict__:
        obj.__dict__.pop("namespace")

    for k, v in obj.__dict__.items():
        if isinstance(v, dict):
            for k1, v1 in v.items():
                del_namespace(v1, depth + 1)
        elif isinstance(v, list):
            for v1 in v:
                del_namespace(v1, depth + 1)
        else:
            del_namespace(v, depth + 1)


class RemoveOffsetsTransformer(ast.NodeTransformer):
    def generic_visit(self, node):
        if hasattr(node, "lineno"):
            node.lineno = 1
        if hasattr(node, "col_offset"):
            node.col_offset = 2
        if hasattr(node, "end_lineno"):
            node.end_lineno = 3
        if hasattr(node, "end_col_offset"):
            node.end_col_offset = 4
        return super().generic_visit(node)


def _fhash_callable(obj: Callable) -> str:
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(obj)))
        new_tree = RemoveOffsetsTransformer().visit(tree)
        ast_text = astunparse.unparse(new_tree)
        return fhash(ast_text)
    except Exception:
        return fhash(textwrap.dedent(inspect.getsource(obj)))


def _json_default(item: Any):
    try:
        return dataclasses.asdict(item)
    except TypeError:
        pass
    if isinstance(item, datetime.datetime):
        return item.isoformat(timespec="microseconds")

    if isinstance(item, datetime.date):
        return item.isoformat()

    if hasattr(item, FHASH_ATTR):
        return getattr(item, FHASH_ATTR)(item)

    if isinstance(item, bytes):
        return item.hex()

    if callable(item) and not inspect.isclass(item):
        return _fhash_callable(item)

    if isinstance(item, datetime.timedelta):
        return str(item.total_seconds())

    raise TypeError(f"object of type {type(item).__name__} not hashable")


def _json_dumps(thing: Any):
    return json.dumps(
        thing,
        default=_json_default,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(",", ":"),
    )


def fhash(*items: Any):
    """Fennel Hash that is used to uniquely identify any python object."""
    item_list = list(cast(List[Any], items))
    return hashlib.md5(_json_dumps(item_list).encode("utf-8")).hexdigest()


def parse_annotation_comments(cls: Any) -> Dict[str, str]:
    try:
        source = fennel_get_source(cls)
        source_lines = source.splitlines()
        tree = ast.parse(source)
        if len(tree.body) != 1:
            return {}

        comments_for_annotations: Dict[str, str] = {}
        class_def = tree.body[0]
        if isinstance(class_def, ast.ClassDef):
            for stmt in class_def.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(
                    stmt.target, ast.Name
                ):
                    line = stmt.lineno - 2
                    comments: List[str] = []
                    while line >= 0 and source_lines[line].strip().startswith(
                        "#"
                    ):
                        comment = source_lines[line].strip().strip("#").strip()
                        comments.insert(0, comment)
                        line -= 1

                    if len(comments) > 0:
                        comments_for_annotations[stmt.target.id] = (
                            textwrap.dedent("\n".join(comments))
                        )

        return comments_for_annotations
    except Exception:
        return {}


def propogate_fennel_attributes(src: Any, dest: Any):
    """Propogates any  fennel attributes from src to dest."""
    if not hasattr(src, "__dict__"):
        return

    for k, v in src.__dict__.items():
        if k.startswith("__fennel") and k.endswith("__"):
            setattr(dest, k, v)


def to_columnar_json(df: DataFrame, as_str=False) -> str | Dict[str, Any]:
    """
    Converts a pandas dataframe into a json string that is
    made up of a dictionary of columns mapping to list of values, one value per row.
    This function is preferred over to_dict since it uses pandas.DataFrame.to_json
    which properly handles datetimes and nans according to the json standard.
    By default, a json-compatible dict is returned. Use as_str = true to
    stringify the json
    """

    # orient "split" returns: a list of columns, a list of index names, and
    # a 2d array of data
    split_json = json.loads(df.to_json(orient="split"))
    column_dict = {}
    num_rows = len(split_json["index"])
    for c, col in enumerate(split_json["columns"]):
        column_dict[col] = [split_json["data"][r][c] for r in range(num_rows)]
    return json.dumps(column_dict) if as_str else column_dict
