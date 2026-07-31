"""Unit tests for ``to_plain_dict`` — the chuk-mcp result normaliser.

Covers every branch: dict passthrough, the three dump methods
(``model_dump``/``dict``/``to_dict``) in priority order, a method that raises
(fall-through), and objects that expose none of them.
"""

from chuk_tool_processor.mcp.transport._result_normalize import to_plain_dict


def test_dict_passes_through_unchanged():
    payload = {"tools": [{"name": "echo"}]}
    assert to_plain_dict(payload) is payload


def test_model_dump_pydantic_v2():
    class Model:
        def model_dump(self):
            return {"via": "model_dump"}

    assert to_plain_dict(Model()) == {"via": "model_dump"}


def test_dict_method_pydantic_v1():
    class Model:
        def dict(self):
            return {"via": "dict"}

    assert to_plain_dict(Model()) == {"via": "dict"}


def test_to_dict_rust_backed():
    class PyO3Like:
        def to_dict(self):
            return {"via": "to_dict"}

    assert to_plain_dict(PyO3Like()) == {"via": "to_dict"}


def test_model_dump_preferred_over_to_dict():
    # A mock may expose several dump methods; model_dump must win so existing
    # pydantic-mock tests keep their behaviour.
    class Both:
        def model_dump(self):
            return {"via": "model_dump"}

        def to_dict(self):
            return {"via": "to_dict"}

    assert to_plain_dict(Both()) == {"via": "model_dump"}


def test_raising_method_falls_through_to_next_form():
    class Flaky:
        def model_dump(self):
            raise ValueError("boom")

        def to_dict(self):
            return {"via": "to_dict"}

    assert to_plain_dict(Flaky()) == {"via": "to_dict"}


def test_object_without_dump_methods_returned_as_is():
    sentinel = object()
    assert to_plain_dict(sentinel) is sentinel


def test_non_callable_dump_attribute_is_ignored():
    class Weird:
        model_dump = "not callable"

        def to_dict(self):
            return {"via": "to_dict"}

    assert to_plain_dict(Weird()) == {"via": "to_dict"}


def test_all_methods_raise_returns_original_object():
    class AllBad:
        def model_dump(self):
            raise RuntimeError

        def dict(self):
            raise RuntimeError

        def to_dict(self):
            raise RuntimeError

    obj = AllBad()
    assert to_plain_dict(obj) is obj
