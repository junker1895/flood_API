from app.core.validation import valid_latlon


def test_bbox_helper_latlon():
    assert valid_latlon(10, 10)
    assert not valid_latlon(100, 10)
