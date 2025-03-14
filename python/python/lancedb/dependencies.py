# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


#
# The following code is originally from https://github.com/pola-rs/polars/blob/ea4389c31b0e87ddf20a85e4c3797b285966edb6/py-polars/polars/dependencies.py
# and is licensed under the MIT license:
#
# License: MIT, Copyright (c) 2020 Ritchie Vink
# https://github.com/pola-rs/polars/blob/main/LICENSE
#
# It has been modified by the LanceDB developers
# to fit the needs of the LanceDB project.


from __future__ import annotations

import re
import sys
from functools import lru_cache
from importlib import import_module
from importlib.util import find_spec
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, Hashable, cast

_NUMPY_AVAILABLE = True
_PANDAS_AVAILABLE = True
_POLARS_AVAILABLE = True
_TORCH_AVAILABLE = True
_HUGGING_FACE_AVAILABLE = True
_TENSORFLOW_AVAILABLE = True
_RAY_AVAILABLE = True
_LANCE_AVAILABLE = True


class _LazyModule(ModuleType):
    """
    Module that can act both as a lazy-loader and as a proxy.

    Notes
    -----
    We do NOT register this module with `sys.modules` so as not to cause
    confusion in the global environment. This way we have a valid proxy
    module for our own use, but it lives _exclusively_ within lance.

    """

    __lazy__ = True

    _mod_pfx: ClassVar[dict[str, str]] = {
        "numpy": "np.",
        "pandas": "pd.",
        "polars": "pl.",
        "torch": "torch.",
        "tensorflow": "tf.",
        "ray": "ray.",
        "lance": "lance.",
    }

    def __init__(
        self,
        module_name: str,
        *,
        module_available: bool,
    ) -> None:
        """
        Initialise lazy-loading proxy module.

        Parameters
        ----------
        module_name : str
            the name of the module to lazy-load (if available).

        module_available : bool
            indicate if the referenced module is actually available (we will proxy it
            in both cases, but raise a helpful error when invoked if it doesn't exist).

        """
        self._module_available = module_available
        self._module_name = module_name
        self._globals = globals()
        super().__init__(module_name)

    def _import(self) -> ModuleType:
        # import the referenced module, replacing the proxy in this module's globals
        module = import_module(self.__name__)
        self._globals[self._module_name] = module
        self.__dict__.update(module.__dict__)
        return module

    def __getattr__(self, attr: Any) -> Any:
        # have "hasattr('__wrapped__')" return False without triggering import
        # (it's for decorators, not modules, but keeps "make doctest" happy)
        if attr == "__wrapped__":
            raise AttributeError(
                f"{self._module_name!r} object has no attribute {attr!r}"
            )

        # accessing the proxy module's attributes triggers import of the real thing
        if self._module_available:
            # import the module and return the requested attribute
            module = self._import()
            return getattr(module, attr)

        # user has not installed the proxied/lazy module
        elif attr == "__name__":
            return self._module_name
        elif re.match(r"^__\w+__$", attr) and attr != "__version__":
            # allow some minimal introspection on private module
            # attrs to avoid unnecessary error-handling elsewhere
            return None
        else:
            # all other attribute access raises a helpful exception
            pfx = self._mod_pfx.get(self._module_name, "")
            raise ModuleNotFoundError(
                f"{pfx}{attr} requires {self._module_name!r} module to be installed"
            ) from None


def _lazy_import(module_name: str) -> tuple[ModuleType, bool]:
    """
    Lazy import the given module; avoids up-front import costs.

    Parameters
    ----------
    module_name : str
        name of the module to import, eg: "polars".

    Notes
    -----
    If the requested module is not available (eg: has not been installed), a proxy
    module is created in its place, which raises an exception on any attribute
    access. This allows for import and use as normal, without requiring explicit
    guard conditions - if the module is never used, no exception occurs; if it
    is, then a helpful exception is raised.

    Returns
    -------
    tuple of (Module, bool)
        A lazy-loading module and a boolean indicating if the requested/underlying
        module exists (if not, the returned module is a proxy).

    """
    # check if module is LOADED
    if module_name in sys.modules:
        return sys.modules[module_name], True

    # check if module is AVAILABLE
    try:
        module_spec = find_spec(module_name)
        module_available = not (module_spec is None or module_spec.loader is None)
    except ModuleNotFoundError:
        module_available = False

    # create lazy/proxy module that imports the real one on first use
    # (or raises an explanatory ModuleNotFoundError if not available)
    return (
        _LazyModule(
            module_name=module_name,
            module_available=module_available,
        ),
        module_available,
    )


if TYPE_CHECKING:
    import datasets
    import numpy
    import pandas
    import polars
    import ray
    import tensorflow
    import torch
    import lance
else:
    # heavy/optional third party libs
    numpy, _NUMPY_AVAILABLE = _lazy_import("numpy")
    pandas, _PANDAS_AVAILABLE = _lazy_import("pandas")
    polars, _POLARS_AVAILABLE = _lazy_import("polars")
    torch, _TORCH_AVAILABLE = _lazy_import("torch")
    datasets, _HUGGING_FACE_AVAILABLE = _lazy_import("datasets")
    tensorflow, _TENSORFLOW_AVAILABLE = _lazy_import("tensorflow")
    ray, _RAY_AVAILABLE = _lazy_import("ray")
    lance, _LANCE_AVAILABLE = _lazy_import("lance")


@lru_cache(maxsize=None)
def _might_be(cls: type, type_: str) -> bool:
    # infer whether the given class "might" be associated with the given
    # module (in which case it's reasonable to do a real isinstance check)
    try:
        return any(f"{type_}." in str(o) for o in cls.mro())
    except TypeError:
        return False


def _check_for_numpy(obj: Any, *, check_type: bool = True) -> bool:
    return _NUMPY_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "numpy"
    )


def _check_for_pandas(obj: Any, *, check_type: bool = True) -> bool:
    return _PANDAS_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "pandas"
    )


def _check_for_polars(obj: Any, *, check_type: bool = True) -> bool:
    return _POLARS_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "polars"
    )


def _check_for_torch(obj: Any, *, check_type: bool = True) -> bool:
    return _TORCH_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "torch"
    )


def _check_for_hugging_face(obj: Any, *, check_type: bool = True) -> bool:
    return _HUGGING_FACE_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "datasets"
    )


def _check_for_tensorflow(obj: Any, *, check_type: bool = True) -> bool:
    return _TENSORFLOW_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "tensorflow"
    )


def _check_for_ray(obj: Any, *, check_type: bool = True) -> bool:
    return _RAY_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "ray"
    )


def _check_for_lance(obj: Any, *, check_type: bool = True) -> bool:
    return _LANCE_AVAILABLE and _might_be(
        cast(Hashable, type(obj) if check_type else obj), "lance"
    )


__all__ = [
    # lazy-load third party libs
    "datasets",
    "numpy",
    "pandas",
    "polars",
    "ray",
    "tensorflow",
    "torch",
    "lance",
    # lazy utilities
    "_check_for_hugging_face",
    "_check_for_numpy",
    "_check_for_pandas",
    "_check_for_polars",
    "_check_for_tensorflow",
    "_check_for_torch",
    "_check_for_ray",
    "_check_for_lance",
    "_LazyModule",
    # exported flags/guards
    "_NUMPY_AVAILABLE",
    "_PANDAS_AVAILABLE",
    "_POLARS_AVAILABLE",
    "_TORCH_AVAILABLE",
    "_HUGGING_FACE_AVAILABLE",
    "_TENSORFLOW_AVAILABLE",
    "_RAY_AVAILABLE",
    "_LANCE_AVAILABLE",
]
