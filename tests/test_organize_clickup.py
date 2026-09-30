from reverse_etl.organize_clickup import priority_for, tags_for


def test_bug_label_becomes_a_high_priority_bug_tag():
    tags = tags_for(["type:bug", "engine:v1", "area:engine"])
    assert tags == {"bug"}
    assert priority_for(tags) == 2


def test_plain_bug_and_needs_repro_are_high():
    assert priority_for(tags_for(["bug"])) == 2
    assert "needs-repro" in tags_for(["status:needs-repro"])
    assert priority_for(tags_for(["status:needs-repro"])) == 2


def test_feature_and_triage_stay_normal():
    tags = tags_for(["type:feature", "status:triage", "adapter:duckdb"])
    assert tags == {"feature", "triage"}
    assert priority_for(tags) == 3


def test_docs_without_a_product_type_is_low():
    assert tags_for(["type:docs"]) == {"docs"}
    assert priority_for({"docs"}) == 4


def test_unlabeled_issue_has_no_tags_and_normal_priority():
    assert tags_for([]) == set()
    assert priority_for(set()) == 3
