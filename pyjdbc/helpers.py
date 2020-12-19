__all__ = ['doc_str']

import inspect


def doc_str(obj):
    """
    Get method or function doc string without recursion
    """
    try:
        doc = obj.__doc__
    except AttributeError:
        return None
    if not isinstance(doc, str):
        return None
    return inspect.cleandoc(doc)