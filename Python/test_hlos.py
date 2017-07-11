# End to end validation tests for Python Hospital Length of Stay.

import os
import dill
from pandas import DataFrame

from revoscalepy import RxOdbcData, RxSqlServerData, RxTextData, rx_import, rx_read_object


def test_step1_check_output():
    # Load the connection string and compute context definitions.
    connection_string = "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"

    col_info = {"eid": {'type': 'integer'},
                "vdate": {'type': 'character'},
                "rcount": {'type': 'character'},
                "gender": {'type': 'factor'},
                "dialysisrenalendstage": {'type': 'factor'},
                "asthma": {'type': 'factor'},
                "irondef": {'type': 'factor'},
                "pneum": {'type': 'factor'},
                "substancedependence": {'type': 'factor'},
                "psychologicaldisordermajor": {'type': 'factor'},
                "depress": {'type': 'factor'},
                "psychother": {'type': 'factor'},
                "fibrosisandother": {'type': 'factor'},
                "malnutrition": {'type': 'factor'},
                "hemo": {'type': 'factor'},
                "hematocrit": {'type': 'numeric'},
                "neutrophils": {'type': 'numeric'},
                "sodium": {'type': 'numeric'},
                "glucose": {'type': 'numeric'},
                "bloodureanitro": {'type': 'numeric'},
                "creatinine": {'type': 'numeric'},
                "bmi": {'type': 'numeric'},
                "pulse": {'type': 'numeric'},
                "respiration": {'type': 'numeric'},
                "secondarydiagnosisnonicd9": {'type': 'factor'},
                "discharged": {'type': 'character'},
                "facid": {'type': 'factor'},
                "lengthofstay": {'type': 'integer'}}

    # Point to the input data set while specifying the classes.
    file_path = "..\\Data"
    LoS_text = RxTextData(file=os.path.join(file_path, "LengthOfStay.csv"), column_info=col_info)
    table_text = rx_import(LoS_text)

    LengthOfStay_sql = RxSqlServerData(table="LengthOfStay", connection_string=connection_string, column_info=col_info)
    table_sql = rx_import(LengthOfStay_sql)
    assert table_text.equals(table_sql)


def test_step2_check_output():
    # Load the connection string and compute context definitions.
    connection_string = "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"
    LoS_sql = RxSqlServerData(table="LoS", connection_string=connection_string)
    LoS = rx_import(input_data=LoS_sql)
    # dill.dump(LoS, open("LoS.pkl", 'wb'))
    LoS_saved = dill.load(open("LoS.pkl", 'rb'))

    assert LoS.equals(LoS_saved)


def test_step3_check_output():
    # Load the connection string
    connection_string = "Driver=SQL Server;Server=localhost;Database=Hospital;UID=rdemo;PWD=D@tascience"

    # Check that all RTS models have been created
    RTS_odbc = RxOdbcData(connection_string, table="RTS")
    forest_serialized = rx_read_object(RTS_odbc, key="forest", deserialize=False, decompress=None)
    boosted_serialized = rx_read_object(RTS_odbc, key="boosted", deserialize=False, decompress=None)
    fast_serialized = rx_read_object(RTS_odbc, key="fast", deserialize=False, decompress=None)
    NN_serialized = rx_read_object(RTS_odbc, key="NN", deserialize=False, decompress=None)

    assert forest_serialized.__str__()[:6] == "b'blob"
    assert boosted_serialized.__str__()[:6] == "b'blob"
    assert fast_serialized.__str__()[:6] == "b'blob"
    assert NN_serialized.__str__()[:6] == "b'blob"

    # Check that predictions have been made for for all models
    forest_prediction_sql = RxSqlServerData(
        sql_query="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'Forest_Prediction'",
        connection_string=connection_string)
    forest_prediction = rx_import(input_data=forest_prediction_sql)
    boosted_prediction_sql = RxSqlServerData(
        sql_query="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'Boosted_Prediction'",
        connection_string=connection_string)
    boosted_prediction = rx_import(input_data=boosted_prediction_sql)
    fast_prediction_sql = RxSqlServerData(
        sql_query="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'Fast_Prediction'",
        connection_string=connection_string)
    fast_prediction = rx_import(input_data=fast_prediction_sql)
    NN_prediction_sql = RxSqlServerData(
        sql_query="SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'NN_Prediction'",
        connection_string=connection_string)
    NN_prediction = rx_import(input_data=NN_prediction_sql)

    assert isinstance(forest_prediction, DataFrame)
    assert isinstance(boosted_prediction, DataFrame)
    assert isinstance(fast_prediction, DataFrame)
    assert isinstance(NN_prediction, DataFrame)