"""Structural equivalence of the extracted algorithms to reviewed main 82051a14."""

import ast
import hashlib
import importlib
import inspect
import json
from pathlib import Path


def test_extracted_algorithms_preserve_original_ast():
    expected = json.loads(
        (Path(__file__).parent / "fixtures/scanner_algorithm_fingerprints.json").read_text()
    )

    class RestoreContext(ast.NodeTransformer):
        def visit_Name(self, node):
            if node.id == "runtime":
                node.id = "self"
            return node

    for location, fingerprint in expected.items():
        module, name = location.split(".")
        tree = ast.parse(inspect.getsource(importlib.import_module("bybit_flow." + module)))
        service = next(c for c in tree.body if isinstance(c, ast.ClassDef) and c.name != "RuntimePorts")
        method = next(
            n
            for n in service.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
        )
        body = RestoreContext().visit(ast.Module(body=method.body[1:], type_ignores=[]))
        assert hashlib.sha256(ast.dump(body, include_attributes=False).encode()).hexdigest() == fingerprint, (
            location
        )
