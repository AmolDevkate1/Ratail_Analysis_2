import pytest
from  lib.Utils import get_spark_session

@pytest.fixture
def spark():
  spark_session = get_spark_session("LOCAL")
  #return spark_session ------- not teardown
  yield spark_session   #-------- Teardown
  spark_session.stop()