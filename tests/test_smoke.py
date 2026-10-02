"""冒烟测试：验证包可以导入。"""


def test_imports() -> None:
    from app import api, engine, errors, handlers, models, store  # noqa: F401
