import pytest

from kafka.broker.log import Segment


@pytest.fixture
def segment(base_segment: Segment, request: pytest.FixtureRequest) -> Segment:
    base_offset: int = request.param
    return base_segment.model_copy(update=dict(base_offset=base_offset))


@pytest.mark.parametrize(
    "segment, expected",
    [
        (0, "00000000000000000000.log"),
        (12345, "00000000000000012345.log"),
        (99999, "00000000000000099999.log"),
        (1000000, "00000000000001000000.log"),
    ],
    indirect=["segment"],
)
def test_log(segment: Segment, expected: str):
    assert segment.log == expected


@pytest.mark.parametrize(
    "segment, expected",
    [
        (0, "00000000000000000000.index"),
        (12345, "00000000000000012345.index"),
        (99999, "00000000000000099999.index"),
        (1000000, "00000000000001000000.index"),
    ],
    indirect=["segment"],
)
def test_index(segment: Segment, expected: str):
    assert segment.index == expected
