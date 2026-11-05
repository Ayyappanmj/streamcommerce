import pandas as pd

from src.processing.deduplication import split_duplicates


def test_first_occurrence_wins_and_duplicates_carry_reason():
    df = pd.DataFrame({"id": ["a", "b", "a", "a", None, None], "v": [1, 2, 3, 4, 5, 6]})
    unique, dups = split_duplicates(df, "id")
    assert list(unique["v"]) == [1, 2, 5, 6]  # null keys are not deduplicated
    assert list(dups["v"]) == [3, 4]
    assert set(dups["reject_reason"]) == {"duplicate_key"}


def test_no_duplicates_returns_everything():
    df = pd.DataFrame({"id": ["a", "b"]})
    unique, dups = split_duplicates(df, "id")
    assert len(unique) == 2 and dups.empty
