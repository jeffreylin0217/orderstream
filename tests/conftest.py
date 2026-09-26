import pytest


@pytest.fixture(scope="session")
def spark():
    from streaming.session import create_spark

    session = create_spark("orderstream-tests")
    yield session
    session.stop()
