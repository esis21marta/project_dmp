import attr

from src.dmp.pipelines.model_factory import preprocessing_nodes


@attr.s
class DummyDType:
    tn = attr.ib()

    def typeName(self):
        return self.tn


@attr.s
class DummyCol:
    name: str = attr.ib()
    dataType = attr.ib(converter=DummyDType)


def test_col_selection_include():
    feat_pf = "fea_"
    conf = {
        "required": ["test", "bad_prefix_1", "3_bad_suffix"],
        "excluded": ["test", "test2"],
        "exclude_dtype_prefix": ["bar"],
        "exclude_suffix": ["bad_suffix"],
        "exclude_prefix": ["bad_prefix"],
    }

    cols = [
        DummyCol("test", "bar"),
        DummyCol("test2", "foo"),
        DummyCol("test3", "bar"),
        DummyCol("bad_prefix_1", "foo"),
        DummyCol("bad_prefix_2", "foo"),
        DummyCol("2_bad_suffix", "foo"),
        DummyCol("3_bad_suffix", "foo"),
        DummyCol("fea_A", "foo"),
        DummyCol("fea_B", "bar"),
    ]

    # noinspection PyTypeChecker
    res = preprocessing_nodes._col_selection_loop(
        cols, feat_pf, conf, "is_taker", "msisdn", "is_treatment"
    )
    expected = {
        # Via inclusion list
        "test",
        "bad_prefix_1",
        "3_bad_suffix",
        # Via fea_ prefix
        "fea_A",
    }
    unexpected = {
        "test2",  # Via exclusion list
        "test3",  # Via dtype
        "bad_prefix_2",  # Via prefix
        "2_bad_suffix",  # Via suffix
        "fea_B",  # Via dtype
    }
    auto_included = {"target", "is_taker", "is_treatment", "msisdn"}
    # This checks for the correct result from the tool, and checks for a
    # defined ordering.
    assert set(res) == expected.union(auto_included)
    assert res == sorted(res)
    # This checks that we've "explained" everything in # either expected
    # or unexpected.
    assert unexpected.union(expected) == {c.name for c in cols}
