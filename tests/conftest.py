import os
import time

import pytest

from lazyflow.graph import Graph
from dask.distributed import Client
from lazyflow import slot


@pytest.fixture(scope="session", autouse=True)
def _slot():
    ts = time.time()
    cl = Client()
    print("DONE IN", time.time() - ts)
    # setattr(slot, "dask_client", cl)
    res = cl.submit(sum, [1, 2, 3])
    print(res.result())
    return cl


@pytest.fixture(scope="session")
def inputdata_dir():
    conftest_dir = os.path.dirname(__file__)
    return os.path.join(conftest_dir, "data", "inputdata")


@pytest.fixture
def graph():
    return Graph()
