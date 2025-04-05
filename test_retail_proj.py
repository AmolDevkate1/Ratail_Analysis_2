import pytest 
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders
from lib.ConfigReader import get_app_config

# FIXTURE written in seperate file named "conftest.py" instead of here.
# @pytest.fixture
# def spark():
#     return get_spark_session("LOCAL")

def test_read_customers_df(spark):
#def test_read_customers_df():
   #spark = get_spark_session("LOCAL") 
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

def test_read_orders_df(spark):
#def test_read_orders_df():
    #spark = get_spark_session ("LOCAL")
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
#def test_filter_closed_orders():    
   #spark = get_spark_session("LOCAL")
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count ==7556

def test_read_app_conf():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"   

