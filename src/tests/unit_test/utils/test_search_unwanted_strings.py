import os

from pyspark.sql import SparkSession

from utils.search_unwanted_strings import search_pattern


class TestSearchUnwantedStrings:
    def test_search_pattern_msisdn(self, spark_session: SparkSession):
        filepath = "./test.txt"

        text = (
            '"628123234233"\n'
            "'628123234233'\n"
            "628123234233\n"
            '"682123234233"\n'
            '"62812323423"\n'
            '"6281232342333333"\n'
        )
        pat = "[\",']628\\d{9,12}[\",']"
        fo = open(filepath, "w")
        fo.write(text)
        fo.close()

        actual = search_pattern(pat, "test.txt")
        expected = [(1, '"628123234233"'), (2, "'628123234233'")]

        os.remove(filepath)
        assert expected == actual

    def test_search_pattern_outlet_id(self, spark_session: SparkSession):
        filepath = "./test.txt"

        text = (
            '"1009829384"\n'
            "'1009829384'\n"
            '"0009829384"\n'
            '"7009829384"\n'
            '"10098293842"\n'
            '"100982938"\n'
        )
        pat = "[\",'][1-6][0-9]{9}[\",']"
        fo = open(filepath, "w")
        fo.write(text)
        fo.close()

        actual = search_pattern(pat, "test.txt")
        expected = [(1, '"1009829384"'), (2, "'1009829384'")]

        os.remove(filepath)
        assert expected == actual

    def test_search_pattern_nik(self, spark_session: SparkSession):
        filepath = "./test.txt"

        text = (
            '"3451221111980003"\n'
            "'3451225111980003'\n"
            '"345122111198000"\n'
            '"34512211119800032"\n'
            "'3451225114980003'\n"
            "'3451223911980003'\n"
        )
        pat = "[\",'][0-9]{6}([1256][0-9]|[37][0-1]|[40][1-9])(0[1-9]|1[0-2])[0-9]{6}[\",']"
        fo = open(filepath, "w")
        fo.write(text)
        fo.close()

        actual = search_pattern(pat, "test.txt")
        expected = [(1, '"3451221111980003"'), (2, "'3451225111980003'")]

        os.remove(filepath)
        assert expected == actual
