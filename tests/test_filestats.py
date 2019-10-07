from digital_archive import filestats


def test_main():
    result = filestats.main()
    assert result == 1
