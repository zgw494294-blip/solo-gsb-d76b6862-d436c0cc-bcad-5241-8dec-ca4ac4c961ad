"""Tests for the multi-replica offline write reconciliation API."""

import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def _isolated_history_db(tmp_path, monkeypatch):
    """Point the persistent store at a per-test SQLite file."""

    monkeypatch.setenv("HISTORY_DB_PATH", str(tmp_path / "history.db"))
    yield


def ev(replica, seq, key, put=None, delete=None, clock=None):
    e = {"replica_id": replica, "seq": seq, "key": key, "clock": clock or {}}
    if put is not None:
        e["put"] = put
    elif delete is not None:
        e["delete"] = delete
    else:
        e["delete"] = "delete"
    return e


def post(replica_ids, events):
    return client.post(
        "/api/resolve", json={"replica_ids": replica_ids, "events": events}
    )


def post_catchup(replica_ids, events, known_clock):
    return client.post(
        "/api/catchup",
        json={
            "replica_ids": replica_ids,
            "events": events,
            "known_clock": known_clock,
        },
    )


def post_resolve_conflict(replica_ids, events, key, writer, versions, **ops):
    body = {
        "replica_ids": replica_ids,
        "events": events,
        "key": key,
        "replica_id": writer,
        "versions": versions,
    }
    body.update(ops)
    return client.post("/api/resolve-conflict", json=body)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_concurrent_puts_conflict_even_when_values_equal():
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "conflict"
    assert [e["replica_id"] for e in item["events"]] == ["r1", "r2"]


def test_results_sorted_by_key_and_put_shadows_older_event():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "z", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "a", delete="delete", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "a", put="kept", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    results = r.json()["results"]
    assert [x["key"] for x in results] == ["a", "z"]
    assert results[0]["state"] == "put"
    assert results[0]["value"] == "kept"
    assert results[0]["event"]["replica_id"] == "r1"
    assert results[0]["event"]["seq"] == 2


def test_unique_maximal_delete_is_reported_deleted():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", delete="delete", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "deleted"
    assert item["event"]["op"] == "delete"
    assert item["event"]["value"] is None


def test_events_may_arrive_out_of_order():
    events = [
        ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 1}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
    ]
    r = post(["r1", "r2"], events)
    assert r.status_code == 200, r.text
    k = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert k["state"] == "put"
    assert k["value"] == "c"


def test_empty_put_value_is_allowed():
    r = post(
        ["a", "b"],
        [ev("a", 1, "k", put="", clock={"a": 1, "b": 0})],
    )
    assert r.status_code == 200, r.text
    (item,) = r.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == ""


def test_conflict_events_sorted_by_replica_then_seq():
    # Concurrent branches after common predecessor r2/1:
    # r1/2 and r2/2 are incomparable (each advances a different component).
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
            ev("r2", 2, "k", put="d", clock={"r1": 1, "r2": 2}),
            ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    item = [x for x in r.json()["results"] if x["key"] == "k"][0]
    assert item["state"] == "conflict"
    assert [(e["replica_id"], e["seq"]) for e in item["events"]] == [
        ("r1", 2),
        ("r2", 2),
    ]


def test_three_replicas_and_eventless_replica():
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0}),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["event"]["clock"] == {"r1": 1, "r2": 0, "r3": 0}


# ---------------------------------------------------------------------------
# Validation failures -> 422
# ---------------------------------------------------------------------------


def assert_422(r, code=None):
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    if code is not None:
        assert any(e.get("code") == code for e in detail)
    return detail


def test_duplicate_replica_ids_rejected():
    r = post(["r1", "r1"], [ev("r1", 1, "k", put="v", clock={"r1": 0})])
    assert_422(r, "duplicate_replica_id")


def test_unknown_event_replica_rejected():
    r = post(
        ["r1", "r2"],
        [ev("r9", 1, "k", put="v", clock={"r1": 0, "r2": 0})],
    )
    assert_422(r, "unknown_replica_id")


def test_clock_missing_and_extra_components():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r3": 0})],
    )
    assert_422(r, "clock_replica_coverage")


def test_clock_negative_component():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": -1})],
    )
    assert r.status_code == 422


def test_self_component_must_equal_seq():
    r = post(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    detail = assert_422(r, "clock_self_component")
    assert detail[0]["event_index"] == 0


def test_duplicate_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 1, "k", put="b", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert_422(r, "duplicate_seq")


def test_gap_in_seq_rejected():
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r1", 3, "k", put="b", clock={"r1": 3, "r2": 0}),
        ],
    )
    assert_422(r, "non_contiguous_seq")


def test_seq_must_start_at_one():
    r = post(
        ["r1", "r2"],
        [ev("r1", 2, "k", put="a", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "non_contiguous_seq")


def test_clock_pointing_to_missing_event():
    # r1/1 claims to know r3/1, but no event was produced by r3.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0, "r3": 1}),
        ],
    )
    detail = assert_422(r, "clock_points_to_missing_event")
    assert detail[0]["event_index"] == 0


def test_other_replica_clock_equal_rejected():
    # Alias covered by test_equal_clock_events_are_causally_invalid:
    # re-verify the offending event index is reported.
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 0


def test_equal_clock_events_are_causally_invalid():
    # Cross-replica: two events with identical clocks cannot reference each
    # other (the referenced clock must be strictly smaller).
    r = post(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert_422(r, "clock_causality")


def test_predecessor_clock_with_component_greater_rejected():
    # r1/1 knows r2/2 (clock r2=2), but r1/2 only knows r2/1 (r2=1):
    # the predecessor's clock is not <= the current one. Events arrive
    # out of order; the violation must still be pinpointed at r1/2.
    r = post(
        ["r1", "r2"],
        [
            ev("r2", 1, "x", put="x", clock={"r1": 0, "r2": 1}),
            ev("r2", 2, "x", put="x", clock={"r1": 0, "r2": 2}),
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 2}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_other_component_referenced_clock_not_le_rejected():
    # Three replicas: r1/1 references r2/1 (r2 component = 1), but r2/1
    # already knows r3/1 while r1/1 does not -> referenced clock not <=.
    r = post(
        ["r1", "r2", "r3"],
        [
            ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1, "r3": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1, "r3": 0}),
        ],
    )
    detail = assert_422(r, "clock_causality")
    assert detail[0]["event_index"] == 2


def test_put_and_delete_together_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "put": "v",
                "delete": "delete",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_delete_field_wrong_value_rejected():
    r = post(
        ["r1", "r2"],
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "k",
                "delete": "remove",
                "clock": {"r1": 1, "r2": 0},
            }
        ],
    )
    assert r.status_code == 422


def test_boundary_replica_and_event_counts():
    # 8 replicas, single event
    ids = [f"r{i}" for i in range(8)]
    r = post(ids, [ev("r0", 1, "k", put="v", clock={rid: 0 for rid in ids} | {"r0": 1})])
    assert r.status_code == 200, r.text

    # 1 replica -> schema error
    r = post(["only"], [ev("only", 1, "k", put="v", clock={"only": 1})])
    assert r.status_code == 422

    # 121 events (one replica with 121 seqs) -> schema error
    many = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0}) for n in range(1, 122)
    ]
    r = post(["r1", "r2"], many)
    assert r.status_code == 422


def test_empty_events_rejected():
    r = client.post(
        "/api/resolve", json={"replica_ids": ["r1", "r2"], "events": []}
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# /api/catchup
# ---------------------------------------------------------------------------


def test_catchup_schedules_by_dependency_rounds():
    # r1/1 -> r2/1 (knows r1/1) -> r1/2 (knows r2/1); target only has r1/1.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ],
        {"r1": 1, "r2": 0},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    rounds = data["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r2", 1)],
        [("r1", 2)],
    ]
    # Operations, values and clocks are preserved verbatim.
    first = rounds[0][0]
    assert first["key"] == "k"
    assert first["op"] == "put"
    assert first["value"] == "b"
    assert first["clock"] == {"r1": 1, "r2": 1}
    assert data["progress"] == {"r1": 2, "r2": 1}


def test_catchup_round_includes_all_currently_ready_sorted():
    # Independent first events on both replicas are ready simultaneously;
    # second events wait for cross-replica knowledge.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "k", put="a2", clock={"r1": 2, "r2": 1}),
            ev("r2", 2, "k", put="b2", clock={"r1": 1, "r2": 2}),
        ],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1), ("r2", 1)],
        [("r1", 2), ("r2", 2)],
    ]


def test_catchup_concurrent_equal_value_events_are_both_transferred():
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="x", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0}),
        ],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    assert len(rounds) == 1
    assert [(e["replica_id"], e["seq"]) for e in rounds[0]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    assert all(e["value"] == "x" for e in rounds[0])


def test_catchup_all_received_has_empty_rounds():
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ],
        {"r1": 1, "r2": 1},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["rounds"] == []
    assert data["progress"] == {"r1": 1, "r2": 1}


def test_catchup_delete_events_preserved():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", delete="delete", clock={"r1": 1, "r2": 0})],
        {"r1": 0, "r2": 0},
    )
    assert r.status_code == 200, r.text
    (only,) = r.json()["rounds"][0]
    assert only["op"] == "delete"
    assert only["value"] is None
    assert only["key"] == "k"


def test_catchup_progress_reports_eventless_replica_as_zero():
    r = post_catchup(
        ["r1", "r2", "r3"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0, "r3": 0})],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert r.status_code == 200, r.text
    assert r.json()["progress"] == {"r1": 1, "r2": 0, "r3": 0}


def test_catchup_missing_known_clock_component():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 0},
    )
    detail = assert_422(r, "known_clock_coverage")
    assert "event_index" not in detail[0]


def test_catchup_extra_known_clock_component():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert_422(r, "known_clock_coverage")


def test_catchup_known_clock_beyond_batch_max_seq():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": 1, "r2": 3},
    )
    assert_422(r, "known_clock_ahead_of_batch")


def test_catchup_received_event_clock_beyond_known_clock():
    # r2/1 is missing (known_clock r2 = 0), but the already-received r1/1
    # claims to know r2/1 -> the target's knowledge is inconsistent.
    r = post_catchup(
        ["r1", "r2"],
        [
            ev("r2", 1, "k", put="b", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 1}),
        ],
        {"r1": 1, "r2": 0},
    )
    detail = assert_422(r, "clock_exceeds_known_clock")
    assert detail[0]["event_index"] == 1


def test_catchup_negative_known_clock_component_is_422():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
        {"r1": -1, "r2": 0},
    )
    assert r.status_code == 422


def test_catchup_still_validates_batch_with_original_rules():
    r = post_catchup(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
        {"r1": 0, "r2": 0},
    )
    assert_422(r, "clock_self_component")


def test_catchup_strictly_alternating_chain_takes_one_event_per_round():
    events = [
        ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k", put="c", clock={"r1": 2, "r2": 1}),
        ev("r2", 2, "k", put="d", clock={"r1": 2, "r2": 2}),
        ev("r1", 3, "k", put="e", clock={"r1": 3, "r2": 2}),
    ]
    r = post_catchup(["r1", "r2"], events, {"r1": 0, "r2": 0})
    assert r.status_code == 200, r.text
    rounds = r.json()["rounds"]
    # Dependencies must be present at the target or scheduled in an *earlier*
    # round, so the strictly alternating chain cannot batch events together.
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1)],
        [("r2", 1)],
        [("r1", 2)],
        [("r2", 2)],
        [("r1", 3)],
    ]
    assert r.json()["progress"] == {"r1": 3, "r2": 2}


# ---------------------------------------------------------------------------
# /api/resolve-conflict
# ---------------------------------------------------------------------------


def two_branch_conflict(put_a="alice", put_b="bob"):
    return [
        ev("r2", 1, "name", put=put_b, clock={"r1": 0, "r2": 1}),
        ev("r1", 1, "name", put=put_a, clock={"r1": 1, "r2": 0}),
    ]


def test_resolve_conflict_appends_dominating_put_and_returns_verdict():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    new_event = data["event"]
    assert new_event == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"] == {
        "key": "name",
        "state": "put",
        "value": "chosen",
        "event": {
            "replica_id": "r1",
            "seq": 2,
            "op": "put",
            "value": "chosen",
            "clock": {"r1": 2, "r2": 1},
        },
    }


def test_resolve_conflict_equal_value_conflict_is_also_resolved():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict("x", "x"),
        key="name",
        writer="r2",
        versions=[{"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="x",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["event"]["seq"] == 2
    assert data["event"]["clock"] == {"r1": 1, "r2": 2}
    assert data["result"]["state"] == "put"
    assert data["result"]["event"]["seq"] == 2


def test_resolve_conflict_delete_op():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        delete="delete",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "delete": "delete",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"]["state"] == "deleted"
    assert data["result"]["event"]["op"] == "delete"
    assert data["result"]["event"]["value"] is None


def test_resolve_conflict_empty_put_allowed():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["put"] == ""
    assert r.json()["result"]["value"] == ""


def test_resolve_conflict_writer_with_no_prior_events_starts_at_seq_one():
    # r3 is eventless; it writes the resolution as r3/1 with an all-history
    # clock covering both branches.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ],
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"] == {
        "replica_id": "r3",
        "seq": 1,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 1, "r2": 1, "r3": 1},
    }


def test_resolve_conflict_clock_covers_writer_prior_history_beyond_branches():
    # r1/1 (key z) knows r3/1; r1/2 and r2/1 conflict on name but neither
    # branch has seen r3/1.  The resolution clock must still carry r3=1 from
    # the writer r1's own prior history.
    events = [
        ev("r3", 1, "z", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
        ev("r1", 1, "z", put="z2", clock={"r1": 1, "r2": 0, "r3": 1}),
        ev("r1", 2, "name", put="a", clock={"r1": 2, "r2": 0, "r3": 1}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 3, "r2": 1, "r3": 1}


def test_resolve_conflict_event_replays_through_resolve():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    new_event = r.json()["event"]

    rr = post(["r1", "r2"], two_branch_conflict() + [new_event])
    assert rr.status_code == 200, rr.text
    (item,) = rr.json()["results"]
    assert item["state"] == "put"
    assert item["value"] == "chosen"
    assert item["event"]["seq"] == 2


def test_resolve_conflict_event_replays_through_catchup_by_dependency():
    # Eventless writer r3 resolves a 3-replica conflict; replaying onto a
    # target that knows nothing must transfer all three branches first, with
    # the resolution in a later round.
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r3",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    resolution = r.json()["event"]

    cr = post_catchup(
        ["r1", "r2", "r3"],
        branches + [resolution],
        {"r1": 0, "r2": 0, "r3": 0},
    )
    assert cr.status_code == 200, cr.text
    rounds = cr.json()["rounds"]
    assert [[(e["replica_id"], e["seq"]) for e in rnd] for rnd in rounds] == [
        [("r1", 1), ("r2", 1)],
        [("r3", 1)],
    ]
    assert rounds[1][0]["value"] == "chosen"


def test_resolve_conflict_does_not_change_other_keys_verdicts():
    events = two_branch_conflict() + [
        ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1})
    ]
    # Note: r1/2 here dominates r2/1; it sits on a different key, unaffected.
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r2",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        delete="delete",
    )
    assert r.status_code == 200, r.text
    resolution = r.json()["event"]

    rr = post(["r1", "r2"], events + [resolution])
    assert rr.status_code == 200, rr.text
    results = {x["key"]: x for x in rr.json()["results"]}
    assert results["name"]["state"] == "deleted"
    assert results["other"]["state"] == "put"
    assert results["other"]["value"] == "keep"


def test_resolve_conflict_versions_may_arrive_out_of_order():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r2", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    assert r.json()["event"]["clock"] == {"r1": 2, "r2": 1}


def test_resolve_conflict_unknown_writer_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r9",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "unknown_replica_id")


def test_resolve_conflict_version_with_unknown_replica_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r9", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "unknown_replica_id")


def test_resolve_conflict_duplicate_versions_are_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "duplicate_conflict_version")


def test_resolve_conflict_single_version_is_422():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 422, r.text


def test_resolve_conflict_missing_version_is_409():
    # Three pairwise-incomparable maximals; listing only two omits one.
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )

    # A one-entry list is a shape error (at least two required) -> 422.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        branches,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 422, r.text

    # Two listed but one is not maximal (r3/1 lives on another key): 409.
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r3", 1, "other", put="x", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r3", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_extra_version_is_409():
    r = post_resolve_conflict(
        ["r1", "r2", "r3"],
        [
            ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
            ev("r3", 1, "other", put="x", clock={"r1": 0, "r2": 0, "r3": 1}),
        ],
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
            {"replica_id": "r3", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_version_pointing_at_nonexistent_seq_is_409():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 7}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "conflict_versions_mismatch" for e in r.json()["detail"]
    )


def test_resolve_conflict_key_already_resolved_is_409():
    # r1/2 causally dominates r2/1: name is a plain put, not a conflict.
    events = [
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
        ev("r1", 1, "other", put="x", clock={"r1": 1, "r2": 0}),
        ev("r1", 2, "name", put="a", clock={"r1": 2, "r2": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 2}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(e["code"] == "key_not_conflicted" for e in r.json()["detail"])


def test_resolve_conflict_unknown_key_is_409():
    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="missing",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert r.status_code == 409, r.text
    assert any(e["code"] == "key_not_conflicted" for e in r.json()["detail"])


def test_resolve_conflict_full_batch_is_422():
    # 120 valid events on r1 (seq 1..60) and r2 (seq 1..60), all key 'name';
    # the batch already contains a conflict and cannot take one more event.
    events = []
    for n in range(1, 61):
        c1 = {"r1": n, "r2": n - 1}
        c2 = {"r1": n - 1, "r2": n}
        events.append(ev("r1", n, "name", put="a", clock=c1))
        events.append(ev("r2", n, "name", put="b", clock=c2))
    assert len(events) == 120
    r = post_resolve_conflict(
        ["r1", "r2"],
        events,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 60}, {"replica_id": "r2", "seq": 60}],
        put="chosen",
    )
    assert r.status_code == 422, r.text
    assert any(e["code"] == "batch_full" for e in r.json()["detail"])


def test_resolve_conflict_still_validates_original_batch():
    bad = [
        ev("r1", 1, "name", put="a", clock={"r1": 2, "r2": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1}),
    ]
    r = post_resolve_conflict(
        ["r1", "r2"],
        bad,
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="chosen",
    )
    assert_422(r, "clock_self_component")


def test_resolve_conflict_requires_exactly_one_op():
    r = client.post(
        "/api/resolve-conflict",
        json={
            "replica_ids": ["r1", "r2"],
            "events": two_branch_conflict(),
            "key": "name",
            "replica_id": "r1",
            "versions": [
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
        },
    )
    assert r.status_code == 422

    r = post_resolve_conflict(
        ["r1", "r2"],
        two_branch_conflict(),
        key="name",
        writer="r1",
        versions=[{"replica_id": "r1", "seq": 1}, {"replica_id": "r2", "seq": 1}],
        put="x",
        delete="delete",
    )
    assert r.status_code == 422


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Persistent histories
# ---------------------------------------------------------------------------


def hist_create(replica_ids, events):
    return client.post(
        "/api/histories",
        json={"replica_ids": replica_ids, "events": events},
    )


def hist_get(hid):
    return client.get(f"/api/histories/{hid}")


def hist_merge(hid, events):
    return client.post(
        f"/api/histories/{hid}/events", json={"events": events}
    )


def hist_commit(hid, key, writer, versions, **ops):
    body = {
        "key": key,
        "replica_id": writer,
        "versions": versions,
    }
    body.update(ops)
    return client.post(
        f"/api/histories/{hid}/resolve-conflict", json=body
    )


def hist_withdraw(hid, replica_id, seq):
    return client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": replica_id, "seq": seq},
    )


def create_conflict_history():
    r = hist_create(["r1", "r2"], two_branch_conflict())
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_history_create_returns_id_and_reads_back_sorted_batch_and_verdict():
    r = hist_create(
        ["r1", "r2"],
        [
            ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1}),
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0}),
        ],
    )
    assert r.status_code == 201, r.text
    hid = r.json()["id"]
    assert isinstance(hid, str) and len(hid) >= 16

    got = hist_get(hid)
    assert got.status_code == 200, got.text
    data = got.json()
    assert data["id"] == hid
    assert data["replica_ids"] == ["r1", "r2"]
    # Events sorted by replica_id then seq regardless of input order.
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r2", 1),
    ]
    # The verdict is computed over that same committed state.
    (result,) = data["results"]
    assert result["state"] == "conflict"
    assert [e["seq"] for e in result["events"]] == [1, 1]


def test_history_create_invalid_batch_is_422_and_nothing_stored():
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 2, "r2": 0})],
    )
    assert_422(r, "clock_self_component")

    # The bad batch never became a history (its id was never issued anyway,
    # and the DB still has no rows: reads of random ids keep returning 404).
    assert hist_get("deadbeef" * 4).status_code == 404


def test_history_unknown_id_is_404_on_every_endpoint():
    hid = "0" * 32
    assert hist_get(hid).status_code == 404
    assert (
        hist_merge(
            hid, [ev("r1", 1, "k", put="x", clock={"r1": 1, "r2": 0})]
        ).status_code
        == 404
    )
    assert (
        hist_commit(
            hid,
            key="k",
            writer="r1",
            versions=[
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
            put="x",
        ).status_code
        == 404
    )


def test_history_merge_appends_events_and_returns_full_state():
    hid = create_conflict_history()

    r = hist_merge(
        hid,
        [ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1})],
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    results = {x["key"]: x for x in data["results"]}
    assert results["name"]["state"] == "conflict"
    assert results["other"]["state"] == "put"

    # A subsequent read returns exactly what was committed.
    got = hist_get(hid).json()
    assert got["events"] == data["events"]
    assert got["results"] == data["results"]


def test_history_merge_identical_event_is_ignored():
    hid = create_conflict_history()
    same = two_branch_conflict()

    # Re-send both stored events verbatim: no new events, no error.
    r = hist_merge(hid, same)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 2

    # Mix of identical + genuinely new in the same call.
    r = hist_merge(
        hid,
        [
            same[0],
            ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 3


def test_history_merge_same_identity_different_content_is_409_and_atomic():
    hid = create_conflict_history()

    before = hist_get(hid).json()
    r = hist_merge(
        hid,
        [
            ev("r2", 1, "name", put="different", clock={"r1": 0, "r2": 1}),
            ev("r1", 2, "other", put="x", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "event_content_mismatch" for e in r.json()["detail"]
    )

    # Neither event (not even the valid second one) was written.
    after = hist_get(hid).json()
    assert after["events"] == before["events"]


def assert_merge_409(r, expected_index):
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    (err,) = detail
    assert err["code"] == "event_content_mismatch"
    assert err["event_index"] == expected_index
    return err


def test_history_merge_duplicate_extra_zero_clock_component_is_409_and_atomic():
    # Regression: a resend must be compared on its FULL clock.  An extra
    # component (even with value 0) is different content -> 409, and the
    # legal new event travelling in the same batch must not be written.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            ev("r1", 1, "name", put="alice", clock={"r1": 1, "r2": 0, "r3": 0}),
            ev("r1", 2, "other", put="x", clock={"r1": 2, "r2": 1}),
        ],
    )
    assert_merge_409(r, 0)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_duplicate_missing_clock_component_is_409_not_500():
    # Regression: a missing clock component must not raise KeyError while
    # projecting the clock; it is simply different content -> 409.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [ev("r2", 1, "name", put="bob", clock={"r2": 1})],
    )
    assert_merge_409(r, 0)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_duplicate_changed_clock_component_value_is_409():
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            # Same key/op/value as stored r2/1 but a different clock component.
            ev("r2", 1, "name", put="bob", clock={"r1": 1, "r2": 1}),
        ],
    )
    assert_merge_409(r, 0)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_duplicate_clock_key_order_does_not_matter():
    # Equal clocks serialized with the keys in a different order are still
    # the same event -> ignored, nothing changes.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            {
                "replica_id": "r1",
                "seq": 1,
                "key": "name",
                "put": "alice",
                "clock": {"r2": 0, "r1": 1},
            }
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["events"] == before["events"]


def test_history_merge_batch_internal_duplicate_extra_clock_component_is_409():
    # The same full-content comparison applies to two copies of a brand-new
    # (replica_id, seq) inside one batch: an extra clock component on the
    # second copy is a content mismatch -> 409, nothing is written.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1}),
            ev("r2", 1, "k", put="b", clock={"r1": 1, "r2": 1, "r3": 0}),
        ],
    )
    assert_merge_409(r, 1)
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_new_event_illegal_clock_coverage_is_still_422():
    # Coverage rules only apply to genuinely new events (the full history is
    # re-validated): extra or missing clock components on a not-yet-stored
    # identity stay 422, not 409, and nothing is written.
    hid = create_conflict_history()
    before = hist_get(hid).json()

    r = hist_merge(
        hid,
        [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})],
    )
    assert_422(r, "clock_replica_coverage")
    assert hist_get(hid).json()["events"] == before["events"]

    r = hist_merge(
        hid,
        [ev("r1", 2, "k", put="x", clock={"r1": 2})],
    )
    assert_422(r, "clock_replica_coverage")
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_failing_validation_is_422_and_atomic():
    # Valid seed on r1; incoming batch breaks contiguity on r2.
    r = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    )
    hid = r.json()["id"]

    before = hist_get(hid).json()
    r = hist_merge(
        hid,
        [ev("r2", 2, "k", put="b", clock={"r1": 1, "r2": 2})],
    )
    assert_422(r, "non_contiguous_seq")
    assert hist_get(hid).json()["events"] == before["events"]


def test_history_merge_enforces_120_event_limit():
    # 60 paired events -> r1 has 1..60 and r2 has 1..60 (120 total).
    seed = []
    for n in range(1, 61):
        seed.append(
            ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1})
        )
        seed.append(
            ev("r2", n, "k", put="b", clock={"r1": n, "r2": n})
        )
    assert len(seed) == 120
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    r = hist_merge(
        hid, [ev("r1", 61, "k", put="c", clock={"r1": 61, "r2": 60})]
    )
    assert_422(r, "batch_full")
    assert len(hist_get(hid).json()["events"]) == 120


def test_history_merge_unknown_replica_is_422():
    hid = create_conflict_history()
    r = hist_merge(
        hid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")


def test_history_commit_resolves_conflict_and_persists_event():
    hid = create_conflict_history()

    r = hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid
    assert data["event"] == {
        "replica_id": "r1",
        "seq": 2,
        "key": "name",
        "put": "chosen",
        "clock": {"r1": 2, "r2": 1},
    }
    assert data["result"]["state"] == "put"
    assert data["result"]["value"] == "chosen"

    # The committed history contains the resolution event and its verdict.
    got = hist_get(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    assert got["results"][0]["state"] == "put"


def test_history_commit_then_stale_versions_get_409_and_nothing_written():
    hid = create_conflict_history()
    versions = [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    ]

    assert hist_commit(
        hid, key="name", writer="r1", versions=versions, put="chosen"
    ).status_code == 200
    state_after_first = hist_get(hid).json()

    # Replaying the same stale versions against the resolved key -> 409.
    r = hist_commit(
        hid, key="name", writer="r2", versions=versions, delete="delete"
    )
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "key_not_conflicted" for e in r.json()["detail"]
    )

    # No r2/2 was allocated; the history is unchanged.
    assert hist_get(hid).json() == state_after_first


def test_history_commit_version_mismatch_is_409():
    branches = [
        ev("r1", 1, "name", put="a", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "name", put="b", clock={"r1": 0, "r2": 1, "r3": 0}),
        ev("r3", 1, "name", put="c", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = hist_create(["r1", "r2", "r3"], branches).json()["id"]

    r = hist_commit(
        hid,
        key="name",
        writer="r1",
        versions=[
            {"replica_id": "r1", "seq": 1},
            {"replica_id": "r2", "seq": 1},
        ],
        put="chosen",
    )
    assert r.status_code == 409
    assert any(
        e["code"] == "conflict_versions_mismatch"
        for e in r.json()["detail"]
    )


def test_history_commit_writer_seq_continues_tail_under_concurrency():
    # Ten independent conflicting keys; resolve each from r1 concurrently.
    events = []
    for n in range(1, 11):
        events.append(
            ev("r1", n, f"k{n}", put=f"a{n}", clock={"r1": n, "r2": n - 1})
        )
        events.append(
            ev("r2", n, f"k{n}", put=f"b{n}", clock={"r1": n - 1, "r2": n})
        )
    hid = hist_create(["r1", "r2"], events).json()["id"]
    barrier = threading.Barrier(10)

    def resolve(n):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/resolve-conflict",
            json={
                "key": f"k{n}",
                "replica_id": "r1",
                "versions": [
                    {"replica_id": "r1", "seq": n},
                    {"replica_id": "r2", "seq": n},
                ],
                "put": f"chosen{n}",
            },
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(resolve, range(1, 11)))

    assert all(r.status_code == 200 for r in results), [
        r.text for r in results if r.status_code != 200
    ]
    # Exactly one resolution per seq 11..20 -- no duplicated seq, none lost.
    got = hist_get(hid).json()
    r1_seqs = sorted(
        e["seq"] for e in got["events"] if e["replica_id"] == "r1"
    )
    assert r1_seqs == list(range(1, 21))
    assert len(got["events"]) == 30
    # All ten keys end resolved (single maximal put from r1).
    assert all(x["state"] == "put" for x in got["results"])


def test_history_concurrent_merges_lose_no_events_and_validate():
    # Seven writers each append their own independent replica tail, so the
    # only correctness under concurrency is that every append survives and
    # every writer's seq tail is contiguous/unique.
    replicas = ["r1", "r2", "r3", "r4", "r5", "r6", "r7"]
    seed = [
        ev("r1", 1, "k", put="start", clock={r: 0 for r in replicas} | {"r1": 1})
    ]
    hid = hist_create(replicas, seed).json()["id"]
    barrier = threading.Barrier(6)

    def append_tail(rid):
        c = TestClient(app)
        barrier.wait()
        # First event on this replica knows r1/1; later ones chain locally.
        c.post(
            f"/api/histories/{hid}/events",
            json={
                "events": [
                    ev(
                        rid,
                        1,
                        f"k-{rid}-1",
                        put="x",
                        clock={r: 0 for r in replicas} | {"r1": 1, rid: 1},
                    )
                ]
            },
        )
        for n in range(2, 7):
            r = c.post(
                f"/api/histories/{hid}/events",
                json={
                    "events": [
                        ev(
                            rid,
                            n,
                            f"k-{rid}-{n}",
                            put="x",
                            clock={r: 0 for r in replicas}
                            | {"r1": 1, rid: n},
                        )
                    ]
                },
            )
            assert r.status_code == 200, r.text

    with ThreadPoolExecutor(max_workers=7) as pool:
        list(pool.map(append_tail, replicas[1:]))

    got = hist_get(hid).json()
    assert len(got["events"]) == 1 + 6 * 6
    for rid in replicas[1:]:
        seqs = sorted(
            e["seq"] for e in got["events"] if e["replica_id"] == rid
        )
        assert seqs == [1, 2, 3, 4, 5, 6]


def test_history_concurrent_commits_to_same_conflict_one_wins():
    hid = create_conflict_history()
    versions = [
        {"replica_id": "r1", "seq": 1},
        {"replica_id": "r2", "seq": 1},
    ]
    barrier = threading.Barrier(2)

    def commit(writer):
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/resolve-conflict",
            json={
                "key": "name",
                "replica_id": writer,
                "versions": versions,
                "put": f"from-{writer}",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(commit, ["r1", "r2"]))

    statuses = sorted(r.status_code for r in results)
    assert statuses == [200, 409], [r.text for r in results]
    got = hist_get(hid).json()
    # Exactly one resolution event was ever allocated.
    assert len(got["events"]) == 3
    resolution = [
        e for e in got["events"]
        if (e["replica_id"], e["seq"]) not in [("r1", 1), ("r2", 1)]
    ]
    assert len(resolution) == 1
    assert got["results"][0]["state"] == "put"


def test_history_survives_process_restart_with_same_sqlite_file(tmp_path):
    db = tmp_path / "history.db"
    env = dict(os.environ, HISTORY_DB_PATH=str(db), PYTHONPATH="/workspace")

    def run(code):
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
        )

    # Process A: create a history, merge an event, commit a resolution.
    create = run(
        """
import json
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice',
         'clock': {'r1': 1, 'r2': 0}},
        {'replica_id': 'r2', 'seq': 1, 'key': 'name', 'put': 'bob',
         'clock': {'r1': 0, 'r2': 1}},
    ],
})
hid = r.json()['id']
c.post(f'/api/histories/{hid}/events', json={'events': [
    {'replica_id': 'r2', 'seq': 2, 'key': 'other', 'put': 'x',
     'clock': {'r1': 0, 'r2': 2}}
]})
c.post(f'/api/histories/{hid}/resolve-conflict', json={
    'key': 'name', 'replica_id': 'r1',
    'versions': [
        {'replica_id': 'r1', 'seq': 1},
        {'replica_id': 'r2', 'seq': 1},
    ], 'put': 'chosen'})
print(hid)
"""
    )
    assert create.returncode == 0, create.stderr
    hid = create.stdout.strip()

    # Process B (fresh import, same file): everything is still readable.
    read = run(
        f"""
import json
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.get('/api/histories/{hid}')
assert r.status_code == 200, r.text
data = r.json()
assert data['replica_ids'] == ['r1', 'r2']
assert [(e['replica_id'], e['seq']) for e in data['events']] == [
    ('r1', 1), ('r1', 2), ('r2', 1), ('r2', 2)
]
name = [x for x in data['results'] if x['key'] == 'name'][0]
assert name['state'] == 'put' and name['value'] == 'chosen'
print('ok')
"""
    )
    assert read.returncode == 0, read.stderr
    assert read.stdout.strip() == "ok"



# ---------------------------------------------------------------------------
# Read-only withdrawal preview
# ---------------------------------------------------------------------------


def test_withdraw_preview_cascades_across_keys_and_keeps_concurrent_events():
    # r1/1 (key a) -> r2/1 synced with it (key b) -> r1/2 (key a);
    # r3/1 is a causally unrelated concurrent write on key c.
    seed = [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0, "r3": 0}),
        ev("r2", 1, "b", put="y", clock={"r1": 1, "r2": 1, "r3": 0}),
        ev("r1", 2, "a", delete="delete", clock={"r1": 2, "r2": 1, "r3": 0}),
        ev("r3", 1, "c", put="z", clock={"r1": 0, "r2": 0, "r3": 1}),
    ]
    hid = hist_create(["r1", "r2", "r3"], seed).json()["id"]

    r = hist_withdraw(hid, "r1", 1)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == hid
    # Withdrawn identities sorted by (replica_id, seq): target plus its
    # transitive dependents across both keys.
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    # Remaining events keep original wire form and canonical order; no seq or
    # clock is renumbered; the unrelated concurrent r3/1 survives untouched.
    assert data["events"] == [
        ev("r3", 1, "c", put="z", clock={"r1": 0, "r2": 0, "r3": 1})
    ]
    # Changes sorted by key, full verdict structures compared.
    assert [ch["key"] for ch in data["changes"]] == ["a", "b"]
    by_key = {ch["key"]: ch for ch in data["changes"]}
    assert by_key["a"]["before"]["state"] == "deleted"
    assert by_key["a"]["after"] is None
    assert by_key["b"]["before"]["state"] == "put"
    assert by_key["b"]["before"]["value"] == "y"
    assert by_key["b"]["after"] is None
    # Key c never appears: its verdict did not change.
    assert "c" not in by_key


def test_withdraw_preview_conflict_resolves_to_remaining_concurrent_branch():
    seed = [
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "a", put="y", clock={"r1": 0, "r2": 1}),
        # Unrelated tail on r1: withdrawing r2/1 must not touch it.
        ev("r1", 2, "b", put="z", clock={"r1": 2, "r2": 0}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    data = hist_withdraw(hid, "r2", 1).json()
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r2", 1)
    ]
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
    ]
    assert [ch["key"] for ch in data["changes"]] == ["a"]
    change = data["changes"][0]
    assert change["before"]["state"] == "conflict"
    assert len(change["before"]["events"]) == 2
    assert change["after"] == {
        "key": "a",
        "state": "put",
        "value": "x",
        "event": {
            "replica_id": "r1",
            "seq": 1,
            "op": "put",
            "value": "x",
            "clock": {"r1": 1, "r2": 0},
        },
    }


def test_withdraw_preview_removing_only_event_still_returns_200_empty_list():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "only", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    r = hist_withdraw(hid, "r1", 1)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["withdrawn"] == [{"replica_id": "r1", "seq": 1}]
    assert data["events"] == []
    assert len(data["changes"]) == 1
    assert data["changes"][0]["key"] == "only"
    assert data["changes"][0]["before"]["state"] == "put"
    # An emptied key's after verdict is null, which is not a "deleted" state.
    assert data["changes"][0]["after"] is None


def test_withdraw_preview_follows_clock_components_on_other_replicas():
    # r1/1 (k1); r2/1 depends on r1/1 (k2); r1/2 depends on r2/1 and its own
    # previous event (k3): withdrawing r1/1 must pull all three out.
    seed = [
        ev("r1", 1, "k1", put="a", clock={"r1": 1, "r2": 0}),
        ev("r2", 1, "k2", put="b", clock={"r1": 1, "r2": 1}),
        ev("r1", 2, "k3", delete="delete", clock={"r1": 2, "r2": 1}),
    ]
    hid = hist_create(["r1", "r2"], seed).json()["id"]

    data = hist_withdraw(hid, "r1", 1).json()
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
    ]
    assert data["events"] == []
    assert [ch["key"] for ch in data["changes"]] == ["k1", "k2", "k3"]
    assert all(ch["after"] is None for ch in data["changes"])


def test_withdraw_preview_does_not_mutate_the_stored_history():
    before_events = [
        ev("r2", 1, "a", put="y", clock={"r1": 1, "r2": 1}),
        ev("r1", 1, "a", put="x", clock={"r1": 1, "r2": 0}),
    ]
    hid = hist_create(["r1", "r2"], before_events).json()["id"]
    before = hist_get(hid).json()

    preview = hist_withdraw(hid, "r1", 1)
    assert preview.status_code == 200
    # Repeated previews are identical...
    assert hist_withdraw(hid, "r1", 1).json() == preview.json()
    # ...and the committed history is byte-for-byte unchanged.
    after = hist_get(hid).json()
    assert after == before
    assert len(after["events"]) == 2


def test_withdraw_preview_unknown_history_and_missing_event_are_404():
    r = client.get(
        f"/api/histories/{'0' * 32}/withdraw-preview",
        params={"replica_id": "r1", "seq": 1},
    )
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "history_not_found"

    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    # Existing history, seq never used on that replica.
    r = hist_withdraw(hid, "r1", 2)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "event_not_found"

    # replica_id outside the history's replica set is an unknown event, not
    # a 422: the parameters themselves are well-formed strings.
    r = hist_withdraw(hid, "r9", 1)
    assert r.status_code == 404
    assert r.json()["detail"][0]["code"] == "event_not_found"


def test_withdraw_preview_invalid_params_are_422():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="v", clock={"r1": 1, "r2": 0})],
    ).json()["id"]

    # seq below the minimum of 1.
    r = hist_withdraw(hid, "r1", 0)
    assert r.status_code == 422, r.text

    # Empty replica_id.
    r = client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": "", "seq": 1},
    )
    assert r.status_code == 422, r.text

    # Missing seq.
    r = client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": "r1"},
    )
    assert r.status_code == 422, r.text

    # Non-integer seq.
    r = client.get(
        f"/api/histories/{hid}/withdraw-preview",
        params={"replica_id": "r1", "seq": "x"},
    )
    assert r.status_code == 422, r.text


def test_withdraw_preview_works_after_merge_and_conflict_commit():
    # The preview reads the same committed history the mutating endpoints
    # write; a resolution event causally dominates both conflict branches.
    hid = create_conflict_history()
    assert (
        hist_commit(
            hid,
            key="name",
            writer="r1",
            versions=[
                {"replica_id": "r1", "seq": 1},
                {"replica_id": "r2", "seq": 1},
            ],
            put="chosen",
        ).status_code
        == 200
    )

    # Withdrawing r1/1 pulls the committed r1/2 resolution (whose previous
    # event is r1/1), but r2/1 is a *concurrent* branch (its clock has
    # r1 = 0) and therefore stays; the key reverts to r2's put.
    data = hist_withdraw(hid, "r1", 1).json()
    assert [(d["replica_id"], d["seq"]) for d in data["withdrawn"]] == [
        ("r1", 1),
        ("r1", 2),
    ]
    assert data["events"] == [
        ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1})
    ]
    change = data["changes"][0]
    assert change["before"]["state"] == "put"
    assert change["before"]["value"] == "chosen"
    assert change["after"] == {
        "key": "name",
        "state": "put",
        "value": "bob",
        "event": {
            "replica_id": "r2",
            "seq": 1,
            "op": "put",
            "value": "bob",
            "clock": {"r1": 0, "r2": 1},
        },
    }


# ---------------------------------------------------------------------------
# Import sessions: staged multi-batch offline imports, committed at once
# ---------------------------------------------------------------------------


def session_create(hid):
    return client.post(f"/api/histories/{hid}/import-sessions")


def session_get(hid, sid):
    return client.get(f"/api/histories/{hid}/import-sessions/{sid}")


def session_append(hid, sid, events):
    return client.post(
        f"/api/histories/{hid}/import-sessions/{sid}/events",
        json={"events": events},
    )


def session_commit(hid, sid):
    return client.post(f"/api/histories/{hid}/import-sessions/{sid}/commit")


def create_session(hid):
    r = session_create(hid)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_import_session_create_returns_id_and_reads_back_empty_open():
    hid = create_conflict_history()

    sid = create_session(hid)
    assert isinstance(sid, str) and len(sid) >= 16

    r = session_get(hid, sid)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data == {
        "id": sid,
        "history_id": hid,
        "status": "open",
        "events": [],
    }


def test_import_session_unknown_history_or_session_is_404():
    hid = create_conflict_history()
    sid = create_session(hid)
    missing_hid = "0" * 32
    missing_sid = "1" * 32

    # Unknown history on every session endpoint.
    assert session_create(missing_hid).status_code == 404
    assert session_get(missing_hid, sid).status_code == 404
    assert (
        session_append(
            missing_hid,
            sid,
            [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 404
    )
    assert session_commit(missing_hid, sid).status_code == 404
    r = session_create(missing_hid)
    assert r.json()["detail"][0]["code"] == "history_not_found"

    # Unknown session (including a session id of another history).
    other_hid = create_conflict_history()
    assert session_get(hid, missing_sid).status_code == 404
    assert session_get(other_hid, sid).status_code == 404
    r = session_get(hid, missing_sid)
    assert r.json()["detail"][0]["code"] == "session_not_found"
    assert (
        session_append(
            hid,
            missing_sid,
            [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 404
    )
    assert session_commit(hid, missing_sid).status_code == 404


def test_import_session_stages_out_of_order_with_missing_dependencies():
    # Staging tolerates events whose predecessors or cross-replica
    # dependencies have not arrived yet; they only fail at commit time.
    hid = create_conflict_history()
    sid = create_session(hid)

    # r1/3 before r1/2; r2/2 points at a not-yet-staged r1/3.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 3, "name", put="c", clock={"r1": 3, "r2": 1}),
            ev("r2", 2, "other", put="x", clock={"r1": 3, "r2": 2}),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "open"

    r = session_append(
        hid, sid, [ev("r1", 2, "name", put="b", clock={"r1": 2, "r2": 1})]
    )
    assert r.status_code == 200, r.text

    # The session read returns the staged events in original wire format,
    # sorted by replica_id then seq regardless of arrival order.
    data = session_get(hid, sid).json()
    assert data["status"] == "open"
    assert data["events"] == [
        ev("r1", 2, "name", put="b", clock={"r1": 2, "r2": 1}),
        ev("r1", 3, "name", put="c", clock={"r1": 3, "r2": 1}),
        ev("r2", 2, "other", put="x", clock={"r1": 3, "r2": 2}),
    ]

    # Committing the completed session merges everything at once.
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r1", 3),
        ("r2", 1),
        ("r2", 2),
    ]
    assert session_get(hid, sid).json()["status"] == "closed"


def test_import_session_append_per_event_validation_is_422_and_atomic():
    hid = create_conflict_history()
    sid = create_session(hid)
    ok = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})

    # Unknown replica.
    r = session_append(
        hid, sid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")

    # Clock coverage: extra or missing component.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})]
    )
    assert_422(r, "clock_replica_coverage")
    r = session_append(hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2})])
    assert_422(r, "clock_replica_coverage")

    # Self component must equal seq.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1})]
    )
    assert_422(r, "clock_self_component")

    # Schema-level field errors (put+delete together) are 422 as well.
    r = session_append(
        hid,
        sid,
        [
            {
                "replica_id": "r1",
                "seq": 2,
                "key": "k",
                "put": "x",
                "delete": "delete",
                "clock": {"r1": 2, "r2": 1},
            }
        ],
    )
    assert r.status_code == 422

    # A valid event travelling with an illegal one is not staged either.
    r = session_append(
        hid,
        sid,
        [ok, ev("r1", 3, "k", put="y", clock={"r1": 4, "r2": 1})],
    )
    assert_422(r, "clock_self_component")
    assert session_get(hid, sid).json()["events"] == []


def test_import_session_append_identical_ignored_different_content_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    staged = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})
    assert session_append(hid, sid, [staged]).status_code == 200

    # Identical re-staging (clock key order irrelevant) is ignored.
    r = session_append(
        hid,
        sid,
        [
            {
                "replica_id": "r1",
                "seq": 2,
                "key": "k",
                "put": "x",
                "clock": {"r2": 1, "r1": 2},
            }
        ],
    )
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 1

    # Same identity, different content -> 409, and the valid event in the
    # same append is not staged either.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="different", clock={"r1": 2, "r2": 1}),
            ev("r2", 2, "k", put="y", clock={"r1": 2, "r2": 2}),
        ],
    )
    assert r.status_code == 409, r.text
    (err,) = r.json()["detail"]
    assert err["code"] == "event_content_mismatch"
    assert err["event_index"] == 0
    assert len(session_get(hid, sid).json()["events"]) == 1

    # Two copies of a new identity inside one append: identical -> one
    # staged; different content -> 409.
    new = ev("r1", 3, "k", put="z", clock={"r1": 3, "r2": 1})
    r = session_append(hid, sid, [new, dict(new)])
    assert r.status_code == 200
    assert len(r.json()["events"]) == 2
    r = session_append(
        hid,
        sid,
        [
            ev("r2", 2, "k", put="y", clock={"r1": 3, "r2": 2}),
            ev("r2", 2, "k", put="z", clock={"r1": 3, "r2": 2}),
        ],
    )
    assert r.status_code == 409
    assert r.json()["detail"][0]["event_index"] == 1


def assert_session_409(r, expected_index):
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    (err,) = detail
    assert err["code"] == "event_content_mismatch"
    assert err["event_index"] == expected_index
    return err


def test_import_session_append_retransmission_bad_clock_is_409_not_422():
    # Regression: a same-identity retransmission whose clock misses a
    # component, adds an extra (even zero-valued) component, or changes the
    # own component must be judged by full-content comparison -> 409, not by
    # the per-event coverage/self-component checks -> 422.
    hid = create_conflict_history()
    sid = create_session(hid)
    original = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})
    assert session_append(hid, sid, [original]).status_code == 200
    before = session_get(hid, sid).json()
    hist_before = hist_get(hid).json()

    # Extra component (value 0) -> 409.
    r = session_append(
        hid,
        sid,
        [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})],
    )
    assert_session_409(r, 0)

    # Missing component -> 409 (and no KeyError projecting the clock).
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2})]
    )
    assert_session_409(r, 0)

    # Own component changed (r1 = 3 != seq 2) -> 409, not clock_self_component.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1})]
    )
    assert_session_409(r, 0)

    # Another component value changed -> 409.
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 2})]
    )
    assert_session_409(r, 0)

    # A legal new event travelling with the mismatched retransmission is not
    # staged either; the history is untouched as well.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1}),
            ev("r2", 2, "other", put="y", clock={"r1": 2, "r2": 2}),
        ],
    )
    assert_session_409(r, 0)
    assert session_get(hid, sid).json() == before
    assert hist_get(hid).json() == hist_before

    # A byte-identical retransmission (only clock key order differs) is still
    # ignored.
    r = session_append(
        hid,
        sid,
        [
            {
                "replica_id": "r1",
                "seq": 2,
                "key": "k",
                "put": "x",
                "clock": {"r2": 1, "r1": 2},
            }
        ],
    )
    assert r.status_code == 200, r.text
    assert session_get(hid, sid).json() == before


def test_import_session_append_batch_internal_retransmission_bad_clock_is_409():
    # Two copies of a brand-new identity inside one append: an extra zero
    # component (or missing component) on the second copy is a content
    # mismatch against the earlier copy -> 409, not a 422 coverage error.
    hid = create_conflict_history()
    sid = create_session(hid)

    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1}),
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0}),
        ],
    )
    assert_session_409(r, 1)
    assert session_get(hid, sid).json()["events"] == []

    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1}),
            ev("r1", 2, "k", put="x", clock={"r1": 2}),
        ],
    )
    assert_session_409(r, 1)
    assert session_get(hid, sid).json()["events"] == []


def test_import_session_append_new_identity_still_gets_per_event_422():
    # The per-event checks still apply to genuinely new identities; missing
    # dependencies remain tolerated.
    hid = create_conflict_history()
    sid = create_session(hid)
    before = session_get(hid, sid).json()

    # New identity with an illegal clock -> 422 (even though the same clock
    # shape on a retransmission would be 409).
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0})]
    )
    assert_422(r, "clock_replica_coverage")
    r = session_append(hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2})])
    assert_422(r, "clock_replica_coverage")
    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1})]
    )
    assert_422(r, "clock_self_component")
    r = session_append(
        hid, sid, [ev("r9", 1, "k", put="x", clock={"r1": 0, "r2": 0})]
    )
    assert_422(r, "unknown_replica_id")
    assert session_get(hid, sid).json() == before

    # 422 (illegal new identity) wins over a 409 (mismatched retransmission)
    # in the same append, and nothing is staged.
    r = session_append(
        hid,
        sid,
        [
            ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1, "r3": 0}),
            ev("r1", 2, "k", put="x", clock={"r1": 3, "r2": 1}),
        ],
    )
    assert_422(r, "clock_replica_coverage")
    assert session_get(hid, sid).json() == before


def test_import_session_append_121_identical_events_stage_one():
    # A batch may carry more than 120 raw entries: 121 identical copies of
    # one legal new event deduplicate to a single staged event.
    hid = create_conflict_history()
    sid = create_session(hid)
    one = ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})

    r = session_append(hid, sid, [dict(one) for _ in range(121)])
    assert r.status_code == 200, r.text
    assert r.json()["events"] == [one]

    # Mix: 121 copies of the staged event plus 120 copies of one more new
    # identity -> two distinct events staged, still within the cap.
    other = ev("r2", 2, "other", put="y", clock={"r1": 2, "r2": 2})
    r = session_append(
        hid, sid, [dict(one) for _ in range(121)] + [dict(other) for _ in range(120)]
    )
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 2),
        ("r2", 2),
    ]


def test_import_session_full_session_retransmissions_still_succeed():
    # A session holding exactly 120 distinct events accepts another raw
    # batch that only re-sends (all of) them, even though the batch itself
    # has 120 entries and one more copy makes it 121 raw entries.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r2", 1, "seed", put="s", clock={"r1": 0, "r2": 1})],
    ).json()["id"]
    sid = create_session(hid)
    staged = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 121)
    ]
    assert session_append(hid, sid, staged).status_code == 200
    assert len(session_get(hid, sid).json()["events"]) == 120

    # Re-send all 120, plus one extra copy -> 121 raw entries, all repeats.
    r = session_append(hid, sid, staged + [dict(staged[0])])
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 120

    # A genuinely new identity still exceeds the cap -> 422, even when it
    # rides along with 121 retransmissions; the append is atomic.
    r = session_append(
        hid,
        sid,
        [dict(staged[0]) for _ in range(121)]
        + [ev("r1", 121, "k", put="v", clock={"r1": 121, "r2": 0})],
    )
    assert_422(r, "batch_full")
    assert len(session_get(hid, sid).json()["events"]) == 120


def test_import_session_cap_counts_distinct_identities_only():
    # 119 distinct events staged; an append with one genuinely new identity
    # duplicated twice reaches exactly 120 distinct -> OK.  One more new
    # identity (also duplicated) would make 121 distinct -> 422, and the
    # whole append (including its duplicates) is rejected.
    hid = hist_create(
        ["r1", "r2"],
        [ev("r2", 1, "seed", put="s", clock={"r1": 0, "r2": 1})],
    ).json()["id"]
    sid = create_session(hid)
    first = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 120)
    ]
    assert session_append(hid, sid, first).status_code == 200

    last = ev("r1", 120, "k", put="v", clock={"r1": 120, "r2": 0})
    r = session_append(hid, sid, [dict(last), dict(last)])
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 120

    overflow = ev("r1", 121, "k", put="v", clock={"r1": 121, "r2": 0})
    r = session_append(hid, sid, [dict(overflow), dict(overflow)])
    assert_422(r, "batch_full")
    assert len(session_get(hid, sid).json()["events"]) == 120

    # 121 distinct identities in a single raw append on an empty session is
    # likewise rejected by the distinct-identity cap, not accepted.
    sid2 = create_session(hid)
    many = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 122)
    ]
    r = session_append(hid, sid2, many)
    assert_422(r, "batch_full")
    assert session_get(hid, sid2).json()["events"] == []


def test_import_session_append_enforces_120_distinct_event_limit():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r2", 1, "seed", put="s", clock={"r1": 0, "r2": 1})],
    ).json()["id"]
    sid = create_session(hid)

    first = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(1, 61)
    ]
    second = [
        ev("r1", n, "k", put="v", clock={"r1": n, "r2": 0})
        for n in range(61, 121)
    ]
    assert session_append(hid, sid, first).status_code == 200
    r = session_append(hid, sid, second)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 120

    # Re-staging an already-staged event is still fine (no new identity)...
    r = session_append(hid, sid, [first[0]])
    assert r.status_code == 200
    # ...but one more distinct event exceeds the session limit.
    r = session_append(
        hid, sid, [ev("r1", 121, "k", put="v", clock={"r1": 121, "r2": 0})]
    )
    assert_422(r, "batch_full")
    assert len(session_get(hid, sid).json()["events"]) == 120


def test_import_session_commit_returns_history_read_structure_and_closes():
    hid = create_conflict_history()
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [
                ev("r1", 2, "other", put="keep", clock={"r1": 2, "r2": 1}),
                ev("r2", 2, "name", put="bob2", clock={"r1": 2, "r2": 2}),
            ],
        ).status_code
        == 200
    )

    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    data = r.json()
    # Same shape as GET /api/histories/{id}.
    assert data["id"] == hid
    assert data["replica_ids"] == ["r1", "r2"]
    assert [(e["replica_id"], e["seq"]) for e in data["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r2", 1),
        ("r2", 2),
    ]
    results = {x["key"]: x for x in data["results"]}
    assert results["other"]["state"] == "put"
    assert results["name"]["state"] == "put"
    assert results["name"]["value"] == "bob2"

    # The merge and the close took effect together.
    assert hist_get(hid).json() == data
    assert session_get(hid, sid).json()["status"] == "closed"


def test_import_session_commit_missing_dependency_422_then_fix_and_retry():
    hid = hist_create(
        ["r1", "r2"],
        [ev("r1", 1, "k", put="a", clock={"r1": 1, "r2": 0})],
    ).json()["id"]
    sid = create_session(hid)

    # r1/3 staged without r1/2: staging accepts, commit rejects (422).
    assert (
        session_append(
            hid, sid, [ev("r1", 3, "k", put="c", clock={"r1": 3, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert_422(r, "non_contiguous_seq")

    # The failed commit changed nothing: the session is still open with its
    # staged events, and the history is untouched.
    session = session_get(hid, sid).json()
    assert session["status"] == "open"
    assert [(e["replica_id"], e["seq"]) for e in session["events"]] == [
        ("r1", 3)
    ]
    assert len(hist_get(hid).json()["events"]) == 1

    # Cross-replica dependencies are checked at commit time too: a second
    # session stages r2/1 pointing at r1/9, which exists nowhere.
    sid2 = create_session(hid)
    assert (
        session_append(
            hid, sid2, [ev("r2", 1, "k", put="b", clock={"r1": 9, "r2": 1})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid2)
    assert_422(r, "clock_points_to_missing_event")
    assert session_get(hid, sid2).json()["status"] == "open"
    assert len(hist_get(hid).json()["events"]) == 1

    # Append the missing r1/2 to the first session and retry the same
    # commit: now it succeeds and closes the session.
    assert (
        session_append(
            hid, sid, [ev("r1", 2, "k", put="b", clock={"r1": 2, "r2": 0})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert [(e["replica_id"], e["seq"]) for e in r.json()["events"]] == [
        ("r1", 1),
        ("r1", 2),
        ("r1", 3),
    ]
    assert session_get(hid, sid).json()["status"] == "closed"
    # The second session is unaffected and still open.
    assert session_get(hid, sid2).json()["status"] == "open"


def test_import_session_commit_content_mismatch_with_history_is_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    # Stage an event whose identity already exists in the committed history
    # but with different content: staging does not compare against the
    # history, the commit does.
    assert (
        session_append(
            hid,
            sid,
            [ev("r1", 1, "name", put="different", clock={"r1": 1, "r2": 0})],
        ).status_code
        == 200
    )
    before = hist_get(hid).json()
    r = session_commit(hid, sid)
    assert r.status_code == 409, r.text
    assert any(
        e["code"] == "event_content_mismatch" for e in r.json()["detail"]
    )
    # Session stays open, history untouched.
    assert session_get(hid, sid).json()["status"] == "open"
    assert hist_get(hid).json() == before


def test_import_session_commit_identical_to_committed_is_ignored():
    hid = create_conflict_history()
    sid = create_session(hid)
    # Re-stage an event that is already committed, plus a genuinely new one.
    assert (
        session_append(
            hid,
            sid,
            two_branch_conflict()
            + [ev("r1", 2, "x", put="y", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert r.status_code == 200, r.text
    assert len(r.json()["events"]) == 3


def test_import_session_commit_enforces_120_total_limit():
    seed = []
    for n in range(1, 61):
        seed.append(ev("r1", n, "k", put="a", clock={"r1": n, "r2": n - 1}))
        seed.append(ev("r2", n, "k", put="b", clock={"r1": n, "r2": n}))
    hid = hist_create(["r1", "r2"], seed).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid, sid, [ev("r1", 61, "k", put="c", clock={"r1": 61, "r2": 60})]
        ).status_code
        == 200
    )
    r = session_commit(hid, sid)
    assert_422(r, "batch_full")
    assert session_get(hid, sid).json()["status"] == "open"
    assert len(hist_get(hid).json()["events"]) == 120


def test_import_session_closed_rejects_append_and_commit_with_409():
    hid = create_conflict_history()
    sid = create_session(hid)
    assert session_commit(hid, sid).status_code == 200  # empty commit closes

    r = session_commit(hid, sid)
    assert r.status_code == 409, r.text
    assert r.json()["detail"][0]["code"] == "session_closed"

    r = session_append(
        hid, sid, [ev("r1", 2, "k", put="x", clock={"r1": 2, "r2": 1})]
    )
    assert r.status_code == 409, r.text
    assert r.json()["detail"][0]["code"] == "session_closed"

    # The closed session remains readable, with its (empty) staged set.
    data = session_get(hid, sid).json()
    assert data["status"] == "closed"
    assert data["events"] == []


def test_import_session_staging_does_not_affect_history_reads_or_verdicts():
    hid = create_conflict_history()
    before = hist_get(hid).json()

    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [ev("r1", 2, "name", put="staged", clock={"r1": 2, "r2": 1})],
        ).status_code
        == 200
    )

    # The committed history read and the withdrawal preview are unchanged
    # while events are only staged.
    assert hist_get(hid).json() == before
    assert hist_withdraw(hid, "r1", 1).json()["events"] == [
        ev("r2", 1, "name", put="bob", clock={"r1": 0, "r2": 1})
    ]

    # After the commit, the staged events are part of the history.
    assert session_commit(hid, sid).status_code == 200
    assert len(hist_get(hid).json()["events"]) == 3


def test_import_session_commit_concurrent_with_legacy_merge_loses_nothing():
    replicas = ["r1", "r2", "r3"]
    seed = [
        ev("r1", 1, "k", put="start", clock={"r1": 1, "r2": 0, "r3": 0})
    ]
    hid = hist_create(replicas, seed).json()["id"]
    sid = create_session(hid)
    assert (
        session_append(
            hid,
            sid,
            [ev("r2", 1, "a", put="x", clock={"r1": 1, "r2": 1, "r3": 0})],
        ).status_code
        == 200
    )
    barrier = threading.Barrier(2)

    def commit_session():
        c = TestClient(app)
        barrier.wait()
        return c.post(f"/api/histories/{hid}/import-sessions/{sid}/commit")

    def legacy_merge():
        c = TestClient(app)
        barrier.wait()
        return c.post(
            f"/api/histories/{hid}/events",
            json={
                "events": [
                    ev(
                        "r3",
                        1,
                        "b",
                        put="y",
                        clock={"r1": 1, "r2": 0, "r3": 1},
                    )
                ]
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda f: f(), [commit_session, legacy_merge]))

    assert all(r.status_code == 200 for r in results), [
        r.text for r in results
    ]
    got = hist_get(hid).json()
    assert [(e["replica_id"], e["seq"]) for e in got["events"]] == [
        ("r1", 1),
        ("r2", 1),
        ("r3", 1),
    ]
    assert session_get(hid, sid).json()["status"] == "closed"


def test_import_session_survives_process_restart_with_same_sqlite_file(
    tmp_path,
):
    db = tmp_path / "history.db"
    env = dict(os.environ, HISTORY_DB_PATH=str(db), PYTHONPATH="/workspace")

    def run(code):
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
        )

    # Process A: create a history and a session, stage one event.
    create = run(
        """
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.post('/api/histories', json={
    'replica_ids': ['r1', 'r2'],
    'events': [
        {'replica_id': 'r1', 'seq': 1, 'key': 'name', 'put': 'alice',
         'clock': {'r1': 1, 'r2': 0}},
    ],
})
hid = r.json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions')
sid = r.json()['id']
r = c.post(f'/api/histories/{hid}/import-sessions/{sid}/events', json={
    'events': [
        {'replica_id': 'r1', 'seq': 3, 'key': 'name', 'put': 'c',
         'clock': {'r1': 3, 'r2': 0}},
    ],
})
assert r.status_code == 200, r.text
print(hid, sid)
"""
    )
    assert create.returncode == 0, create.stderr
    hid, sid = create.stdout.split()

    # Process B (fresh import, same file): the open session is still there,
    # accepts more events, and commits.
    resume = run(
        f"""
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)
r = c.get('/api/histories/{hid}/import-sessions/{sid}')
assert r.status_code == 200, r.text
data = r.json()
assert data['status'] == 'open'
assert [(e['replica_id'], e['seq']) for e in data['events']] == [('r1', 3)]

r = c.post('/api/histories/{hid}/import-sessions/{sid}/events', json={{
    'events': [
        {{'replica_id': 'r1', 'seq': 2, 'key': 'other', 'put': 'b',
          'clock': {{'r1': 2, 'r2': 0}}}},
    ],
}})
assert r.status_code == 200, r.text
r = c.post('/api/histories/{hid}/import-sessions/{sid}/commit')
assert r.status_code == 200, r.text
assert [(e['replica_id'], e['seq']) for e in r.json()['events']] == [
    ('r1', 1), ('r1', 2), ('r1', 3)
]
r = c.get('/api/histories/{hid}/import-sessions/{sid}')
assert r.json()['status'] == 'closed'
print('ok')
"""
    )
    assert resume.returncode == 0, resume.stderr
    assert resume.stdout.strip() == "ok"
