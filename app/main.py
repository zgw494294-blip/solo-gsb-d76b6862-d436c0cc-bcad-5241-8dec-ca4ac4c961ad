"""HTTP API for reconciling multi-replica offline writes.

A batch of events (each carrying a per-replica sequence number and a vector
clock covering every replica) is validated against the causal history it
declares, and then reduced to the effective version of every key under the
vector-clock partial order.

Besides the resolution endpoint, a catch-up endpoint turns the same batch
into a directly replayable incremental transfer plan for a target replica:
given the target's ``known_clock`` (the highest seq contiguously received
from each source), missing events are scheduled into rounds of causally
satisfied dependencies.

A conflict-resolution endpoint turns a confirmed resolution of a still
conflicting key into a new, replayable event: it appends a put/delete event
to the writing replica, with a clock covering every conflicting version and
the writer's own prior history, and returns the new event together with the
key's post-append verdict.

The three endpoints above are stateless: request payloads are never persisted.
A second family of endpoints (/api/histories) gives multiple callers one
shared, persistent history created from a valid batch: callers then merge in
more offline events by ID (identical events -- full content including every
clock component -- are ignored; same replica_id/seq with different key,
operation, value or clock, including a missing or extra-zero clock
component, is 409) and commit conflict resolutions
by ID with only the original resolution parameters (key, writer, versions,
op) -- the stored versions are re-checked at commit time and stale lists get
409.  Histories live in a local SQLite database (HISTORY_DB_PATH, WAL) and
therefore survive container restarts.

A read-only withdrawal-preview endpoint
(/api/histories/{id}/withdraw-preview) rehearses, on one committed history,
which events and key verdicts withdrawing a single offline event would
affect: the target and everything that directly or transitively depends on
it (across keys) are reported as withdrawn, causally unrelated concurrent
events remain, and every changed key is shown with its before/after verdict
(null after when the key empties).  It never modifies the stored history,
renumbers seqs or rewrites clocks.

A third family of endpoints (/api/histories/{id}/import-sessions) stages
offline events that arrive in several batches into an import session before
they touch the persistent history.  A session is created empty by history
ID and accepts event appends where the per-event checks (fields, replica
membership, full clock coverage, self component == seq) apply only to
genuinely new (replica_id, seq) identities; missing predecessors or
cross-replica dependencies are tolerated while staging and only fail at
commit time.  A retransmission of an identity already staged or seen
earlier in the same batch is compared on full content instead: identical
events (full content including every clock component, key order
irrelevant) are ignored, while the same replica_id/seq with different key,
operation, value or clock -- including a missing component, an extra
component even when its value is 0, or a changed own component -- is 409,
never 422.  The 120-event session cap counts already-staged identities
plus the new identities after in-batch deduplication, so 121 identical
resends stage exactly one event and a full session re-sending its events
still succeeds.  Committing merges the staged events into the
history's latest committed state with the original full-equality, causal
and 120-event rules (content mismatch 409, missing dependencies or other
violations 422) and closes the session; the merge and the close take effect
in the same transaction, a failed commit leaves the open session and its
staged events untouched for a retry, and a closed session rejects further
appends and commits with 409.  Staging never affects the history reads or
verdicts, and sessions persist in the same SQLite database, so they survive
restarts.

A read-only missing-ranges query on a session
(/api/histories/{id}/import-sessions/{sid}/missing-ranges) reports what the
session would still have to fetch from the source: it takes the committed
history and the staged events from one committed snapshot, first reports
every staged identity that the committed history holds with different full
content as a single sorted 409, then computes, per replica, the demand upper
bound (the replica's highest own seq among committed+staged events or any
event clock's component for it, whichever is larger) and coalesces the
missing seqs from 1 to that bound into maximal closed ranges.  It never
commits the session or changes the history, staged events or any state, and
closed sessions are queryable as well.
"""

from __future__ import annotations

import json
import uuid
from typing import Annotated, Optional

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictStr
from pydantic import model_validator

from . import store

app = FastAPI(
    title="Multi-Replica Offline Write Reconciler",
    version="1.0.0",
    description=(
        "Validate a batch of offline write/delete events from multiple "
        "replicas, resolve the effective (maximal) version of each key, and "
        "build replayable incremental catch-up plans for a target replica."
    ),
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class Event(BaseModel):
    """A single put/delete event observed on one replica."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Non-empty ID of the originating replica."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Per-replica sequence number, starting at 1.",
        ),
    ]
    key: Annotated[str, Field(min_length=1, description="Non-empty key.")]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the event is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None, description="Must equal 'delete' when the event is a deletion."
    )
    clock: dict[
        Annotated[str, Field(min_length=1)],
        Annotated[int, Field(strict=True, ge=0)],
    ] = Field(
        ...,
        description=(
            "Vector clock covering every replica ID; non-negative integer "
            "component per replica."
        ),
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "Event":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replica_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Field(
            min_length=2,
            max_length=8,
            description="2-8 non-empty, unique replica IDs.",
        ),
    ]
    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events, any order."),
    ]


class CatchupRequest(ResolveRequest):
    known_clock: Annotated[
        dict[
            Annotated[str, Field(min_length=1)],
            Annotated[int, Field(strict=True, ge=0)],
        ],
        Field(
            ...,
            description=(
                "Per-source highest seq contiguously received by the target; "
                "one non-negative integer component per replica_id."
            ),
        ),
    ]


class ConflictVersion(BaseModel):
    """One of the currently maximal (conflicting) versions of a key."""

    model_config = ConfigDict(extra="forbid")

    replica_id: Annotated[
        str,
        Field(min_length=1, description="Replica that produced the version."),
    ]
    seq: Annotated[
        int,
        Field(
            strict=True,
            ge=1,
            description="Seq of the maximal event on that replica.",
        ),
    ]


class ConflictDecision(BaseModel):
    """The original conflict-resolution parameters, without any batch."""

    model_config = ConfigDict(extra="forbid")

    key: Annotated[str, Field(min_length=1, description="Conflicting key to resolve.")]
    replica_id: Annotated[
        str,
        Field(
            min_length=1,
            description="Replica that writes the resolution event; must be "
            "one of the history's replica_ids.",
        ),
    ]
    versions: Annotated[
        list[ConflictVersion],
        Field(
            min_length=2,
            description=(
                "The conflicting versions as seen when resolving: each with "
                "replica_id and seq; no duplicates, at least two."
            ),
        ),
    ]
    put: Optional[StrictStr] = Field(
        default=None, description="Set when the resolution is a put (empty string allowed)."
    )
    delete: Optional[StrictStr] = Field(
        default=None,
        description="Must equal 'delete' when the resolution is a deletion.",
    )

    @model_validator(mode="after")
    def _exactly_one_op(self) -> "ConflictDecision":
        has_put = self.put is not None
        has_delete = self.delete is not None
        if has_put == has_delete:
            raise ValueError(
                "exactly one of 'put' (string value) or 'delete' (\"delete\") "
                "must be provided"
            )
        if has_delete and self.delete != "delete":
            raise ValueError("the 'delete' field must equal the string \"delete\"")
        return self


class ResolveConflictRequest(ResolveRequest, ConflictDecision):
    """Original batch plus the confirmed resolution of one conflicting key."""


class MergeEventsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: Annotated[
        list[Event],
        Field(min_length=1, max_length=120, description="1-120 events to merge, any order."),
    ]


class StageEventsRequest(BaseModel):
    """A session append: 1+ raw events, no hard cap on the request length.

    Retransmissions of an already-staged (or earlier-in-batch) identity are
    deduplicated by full content, so a batch may legitimately carry more than
    120 raw entries (e.g. 121 identical resends) while staging far fewer
    distinct events.  The 120-event cap is enforced on the number of distinct
    identities after deduplication.
    """

    model_config = ConfigDict(extra="forbid")

    events: Annotated[
        list[Event],
        Field(
            min_length=1,
            description=(
                "One or more events to stage, any order; same-identity "
                "retransmissions are compared by full content and deduplicated."
            ),
        ),
    ]


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class BatchError(Exception):
    """A whole-batch semantic validation failure (HTTP 422)."""

    def __init__(self, errors: list[dict]):
        super().__init__("invalid event batch")
        self.errors = errors


class ConflictStateError(Exception):
    """The declared resolution no longer matches the batch state (HTTP 409)."""

    def __init__(self, errors: list[dict]):
        super().__init__("conflict resolution out of date")
        self.errors = errors


class HistoryNotFound(Exception):
    """No persistent history exists for the referenced ID (HTTP 404)."""

    def __init__(self, history_id: str):
        super().__init__(f"history {history_id!r} not found")
        self.history_id = history_id


class EventNotFound(Exception):
    """A history exists but it has no event with that replica_id/seq (404)."""

    def __init__(self, replica_id: str, seq: int):
        super().__init__(f"event ({replica_id!r}, {seq}) not found")
        self.replica_id = replica_id
        self.seq = seq


class SessionNotFound(Exception):
    """No import session with that ID exists on the history (HTTP 404)."""

    def __init__(self, session_id: str):
        super().__init__(f"import session {session_id!r} not found")
        self.session_id = session_id


class SessionClosedError(Exception):
    """The import session is already closed; no more appends/commits (409)."""

    def __init__(self, session_id: str):
        super().__init__(f"import session {session_id!r} is closed")
        self.session_id = session_id


def _err(code: str, message: str, event_index: int | None = None) -> dict:
    e: dict = {"code": code, "message": message}
    if event_index is not None:
        e["event_index"] = event_index
    return e


# ---------------------------------------------------------------------------
# Validation + resolution
# ---------------------------------------------------------------------------


def _clock_le(
    a: dict[str, int], b: dict[str, int], replicas: list[str]
) -> tuple[bool, bool]:
    """Return (<= under componentwise order, < under partial order).

    ``a <= b`` when every component of ``a`` is <= the corresponding
    component of ``b``; ``a < b`` when additionally some component is
    strictly smaller.
    """

    any_less = False
    for r in replicas:
        x, y = a[r], b[r]
        if x > y:
            return False, False
        if x < y:
            any_less = True
    return True, any_less


def validate_batch(
    replica_ids: list[str], events: list[Event]
) -> dict[tuple[str, int], Event]:
    """Validate the batch and index events by (replica_id, seq).

    Raises BatchError (-> 422) on any violation, pointing at event indices.
    """

    errors: list[dict] = []

    # Rule 1: replica IDs unique (cardinality/non-empty handled by schema).
    seen_replicas: set[str] = set()
    duplicate_replicas: set[str] = set()
    for rid in replica_ids:
        if rid in seen_replicas:
            duplicate_replicas.add(rid)
        seen_replicas.add(rid)
    for rid in sorted(duplicate_replicas):
        errors.append(
            _err("duplicate_replica_id", f"replica_id {rid!r} appears more than once")
        )

    # Per-replica structural checks, one pass.
    seen: dict[tuple[str, int], Event] = {}
    seen_index: dict[tuple[str, int], int] = {}
    seqs_by_replica: dict[str, list[int]] = {rid: [] for rid in replica_ids}

    for i, ev in enumerate(events):
        bad = False

        if ev.replica_id not in seen_replicas:
            errors.append(
                _err(
                    "unknown_replica_id",
                    f"event replica_id {ev.replica_id!r} is not listed in replica_ids",
                    i,
                )
            )
            bad = True
        else:
            seqs_by_replica[ev.replica_id].append(ev.seq)

        # Rule 3: clock covers exactly all replicas, non-negative integers.
        clock_keys = set(ev.clock)
        if clock_keys != seen_replicas:
            missing = sorted(seen_replicas - clock_keys)
            extra = sorted(clock_keys - seen_replicas)
            parts = []
            if missing:
                parts.append(f"missing components for {missing}")
            if extra:
                parts.append(f"unexpected components {extra}")
            errors.append(
                _err(
                    "clock_replica_coverage",
                    "clock must cover exactly all replica_ids: " + "; ".join(parts),
                    i,
                )
            )
            bad = True

        # Rule 4a: per-replica seq unique.
        key = (ev.replica_id, ev.seq)
        if key in seen:
            errors.append(
                _err(
                    "duplicate_seq",
                    f"replica {ev.replica_id!r} already has an event with seq {ev.seq} "
                    f"(event index {seen_index[key]})",
                    i,
                )
            )
            bad = True
        else:
            seen[key] = ev
            seen_index[key] = i

    # Rule 4b: per-replica seq must be a contiguous 1..N range.
    valid_seqs: dict[str, set[int]] = {}
    for rid, seqs in seqs_by_replica.items():
        if not seqs:
            continue
        uniq = set(seqs)
        valid_seqs[rid] = uniq
        expected = set(range(1, max(uniq) + 1))
        if len(uniq) != len(seqs) or uniq != expected:
            gaps = sorted(expected - uniq)
            errors.append(
                _err(
                    "non_contiguous_seq",
                    f"seqs for replica {rid!r} must be unique and start at 1 with no "
                    f"gaps; got sorted seqs {sorted(seqs)} (missing {gaps})",
                )
            )

    if errors:
        raise BatchError(errors)

    # From here the structure is sound; replica/seq/clock lookups are safe.

    # Rule 5: the clock's own component must equal this event's seq.
    for i, ev in enumerate(events):
        if ev.clock[ev.replica_id] != ev.seq:
            raise BatchError(
                [
                    _err(
                        "clock_self_component",
                        f"clock[{ev.replica_id!r}] = {ev.clock[ev.replica_id]} "
                        f"must equal seq = {ev.seq}",
                        i,
                    )
                ]
            )

    # Rules 6 + 7: referenced history must exist and its clocks must be
    # strictly smaller (componentwise <= with at least one strict <).
    for i, ev in enumerate(events):
        referenced: list[Event] = []

        # The immediately previous event on this replica.
        if ev.seq > 1:
            referenced.append(seen[(ev.replica_id, ev.seq - 1)])

        # For every other replica, the latest event its component points at;
        # component 0 means "nothing known".
        for rid in replica_ids:
            if rid == ev.replica_id:
                continue
            n = ev.clock[rid]
            if n > 0:
                if n not in valid_seqs.get(rid, set()):
                    raise BatchError(
                        [
                            _err(
                                "clock_points_to_missing_event",
                                f"clock[{rid!r}] = {n}, but replica {rid!r} has no "
                                f"event with seq {n}",
                                i,
                            )
                        ]
                    )
                referenced.append(seen[(rid, n)])

        for ref in referenced:
            le, any_less = _clock_le(ref.clock, ev.clock, replica_ids)
            if not le or not any_less:
                ref_idx = seen_index[(ref.replica_id, ref.seq)]
                kind = "not <= current clock" if not le else "equal to current clock"
                raise BatchError(
                    [
                        _err(
                            "clock_causality",
                            f"referenced event (index {ref_idx}, replica "
                            f"{ref.replica_id!r}, seq {ref.seq}) clock {ref.clock} is "
                            f"{kind} {ev.clock}; it must be componentwise <= with at "
                            f"least one strictly smaller component",
                            i,
                        )
                    ]
                )

    return seen


def _maximal_events(group: list[Event], replica_ids: list[str]) -> list[Event]:
    """Maximal events of one key under the vector-clock partial order.

    Maximal = no *other* event in the group is strictly greater.
    """

    maximals: list[Event] = []
    for ev in group:
        dominated = False
        for other in group:
            if other is ev:
                continue
            le, any_less = _clock_le(ev.clock, other.clock, replica_ids)
            if le and any_less:
                dominated = True
                break
        if not dominated:
            maximals.append(ev)

    maximals.sort(key=lambda e: (e.replica_id, e.seq))
    return maximals


def _event_shape(ev: Event, replica_ids: list[str]) -> dict:
    """Verdict-shaped event projection used in resolve responses."""

    return {
        "replica_id": ev.replica_id,
        "seq": ev.seq,
        "op": "put" if ev.put is not None else "delete",
        "value": ev.put,
        "clock": {rid: ev.clock[rid] for rid in replica_ids},
    }


def _event_wire(ev: Event) -> dict:
    """Event in request wire format (exactly one of put/delete present).

    The returned object can be appended to a batch's ``events`` and replayed
    through /api/resolve and /api/catchup unchanged.
    """

    out = {
        "replica_id": ev.replica_id,
        "seq": ev.seq,
        "key": ev.key,
    }
    if ev.put is not None:
        out["put"] = ev.put
    else:
        out["delete"] = "delete"
    out["clock"] = dict(ev.clock)
    return out


def _canonical_wire(ev: Event, replica_ids: list[str]) -> dict:
    """Wire form with clock keys in the history's replica_ids order."""

    out = _event_wire(ev)
    out["clock"] = {rid: ev.clock[rid] for rid in replica_ids}
    return out


def _wire_model(raw: dict, replica_ids: list[str]) -> Event:
    """Parse a stored wire event back into an Event (clock order irrelevant)."""

    payload = dict(raw)
    payload["clock"] = {rid: raw["clock"][rid] for rid in replica_ids}
    return Event.model_validate(payload)


def _load_row(row: object) -> tuple[str, list[str], list[Event]]:
    replica_ids: list[str] = json.loads(row["replica_ids"])
    events = [_wire_model(raw, replica_ids) for raw in json.loads(row["events"])]
    return row["id"], replica_ids, events


def _canonical_events(
    events: list[Event], replica_ids: list[str]
) -> list[dict]:
    """Canonical persisted/returned order: replica_id then seq."""

    return [
        _canonical_wire(ev, replica_ids)
        for ev in sorted(events, key=lambda e: (e.replica_id, e.seq))
    ]


def _history_state(
    history_id: str, replica_ids: list[str], events: list[Event]
) -> dict:
    """Full original-format batch plus the verdict over the same events."""

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    return {
        "id": history_id,
        "replica_ids": list(replica_ids),
        "events": [_canonical_wire(ev, replica_ids) for ev in ordered],
        "results": resolve(ordered, replica_ids),
    }


def _batch_full_error() -> BatchError:
    return BatchError(
        [
            _err(
                "batch_full",
                "the history already contains 120 events; the 120-event "
                "limit leaves no room to merge or append more events",
            )
        ]
    )


def _same_event_content(a: Event, b: Event) -> bool:
    """Whether two events sharing an identity carry identical content.

    Compares the full wire content: key, operation kind, value and the
    *complete* clock -- not just the components projected onto the history's
    replica_ids.  Clock component order is irrelevant (dict equality), but a
    missing component, an extra component (even when its value is 0), or any
    differing component value makes the two events different content.
    """

    return (
        a.key == b.key
        and (a.put is None) == (b.put is None)
        and a.put == b.put
        and a.clock == b.clock
    )


def _merge_events(
    replica_ids: list[str],
    current: list[Event],
    incoming: list[Event],
) -> list[Event]:
    """Merge incoming events by (replica_id, seq) into the stored history.

    Identical events (same identity and same full content, including every
    clock component) are ignored; the same (replica_id, seq) with different
    key, operation, value or clock is a 409 -- an extra zero-valued clock
    component or a missing component already count as different content.
    The merged list is then checked against the original batch validation
    (so a genuinely new event with illegal clock coverage still gets 422) and
    the 120-event cap.  Either the whole merge applies or nothing does.
    """

    stored = {(ev.replica_id, ev.seq): ev for ev in current}

    merged = list(current)
    for i, ev in enumerate(incoming):
        prior = stored.get((ev.replica_id, ev.seq))
        if prior is not None:
            if not _same_event_content(prior, ev):
                raise ConflictStateError(
                    [
                        {
                            "code": "event_content_mismatch",
                            "message": (
                                f"event with replica_id {ev.replica_id!r}, "
                                f"seq {ev.seq} already exists in this history "
                                "with different content; stored "
                                f"{_event_wire(prior)!r}, incoming "
                                f"{_event_wire(ev)!r}"
                            ),
                            "event_index": i,
                        }
                    ]
                )
            # Same identity, identical content -> ignore.
            continue
        stored[(ev.replica_id, ev.seq)] = ev
        merged.append(ev)

    if len(merged) > 120:
        raise _batch_full_error()

    # Re-run the original validation on the whole merged history.
    validate_batch(replica_ids, merged)
    return merged


def _stage_events(
    replica_ids: list[str],
    staged: list[Event],
    incoming: list[Event],
) -> list[Event]:
    """Stage incoming events into an open import session.

    The per-event checks (unknown replica, clock must cover exactly all
    replica_ids, own clock component == seq; all 422) apply only to
    *genuinely new* (replica_id, seq) identities -- the first occurrence of
    an identity that is not already staged.  Missing predecessors or
    cross-replica dependencies of such events are tolerated here and only
    fail when the session is committed.

    A *retransmission* -- an identity already staged, or seen earlier in this
    same batch -- does not go through the per-event checks at all; a missing
    clock component, an extra component (even one with value 0) or a changed
    own component on a retransmitted identity is therefore never a 422.
    Retransmissions are compared by full content (key, operation kind, value
    and the *complete* clock; clock key order is irrelevant): identical
    content is ignored, different content is a 409 (event_content_mismatch)
    pointing at the offending batch index.

    The session stages at most 120 distinct identities: the cap counts
    already-staged identities plus the genuinely new identities after
    in-batch deduplication, so e.g. 121 identical resends of one legal event
    stage exactly one and a full (120-event) session re-sending its events
    still succeeds.  Either the whole append applies or nothing does.
    """

    replica_set = set(replica_ids)

    # Resolve every incoming identity against what is already staged and the
    # earlier entries of this same batch; only the first occurrence of an
    # identity that is not already staged is treated as genuinely new.
    staged_by_id = {(ev.replica_id, ev.seq): ev for ev in staged}
    seen_new: set[tuple[str, int]] = set()
    new_identity: list[bool] = []
    for ev in incoming:
        ident = (ev.replica_id, ev.seq)
        is_new = ident not in staged_by_id and ident not in seen_new
        new_identity.append(is_new)
        if is_new:
            seen_new.add(ident)

    # Per-event checks, collected in index order, for genuinely new events
    # only -- retransmissions are judged by full-content comparison below.
    errors: list[dict] = []
    for i, ev in enumerate(incoming):
        if not new_identity[i]:
            continue
        if ev.replica_id not in replica_set:
            errors.append(
                _err(
                    "unknown_replica_id",
                    f"event replica_id {ev.replica_id!r} is not listed in "
                    "replica_ids",
                    i,
                )
            )
        clock_keys = set(ev.clock)
        if clock_keys != replica_set:
            missing = sorted(replica_set - clock_keys)
            extra = sorted(clock_keys - replica_set)
            parts = []
            if missing:
                parts.append(f"missing components for {missing}")
            if extra:
                parts.append(f"unexpected components {extra}")
            errors.append(
                _err(
                    "clock_replica_coverage",
                    "clock must cover exactly all replica_ids: "
                    + "; ".join(parts),
                    i,
                )
            )
    if errors:
        raise BatchError(errors)

    # Own component check for genuinely new events only; those events have
    # exact coverage from here on, so the self-component lookup is safe.
    for i, ev in enumerate(incoming):
        if not new_identity[i]:
            continue
        if ev.clock[ev.replica_id] != ev.seq:
            raise BatchError(
                [
                    _err(
                        "clock_self_component",
                        f"clock[{ev.replica_id!r}] = {ev.clock[ev.replica_id]} "
                        f"must equal seq = {ev.seq}",
                        i,
                    )
                ]
            )

    merged_by_id = dict(staged_by_id)
    merged = list(staged)
    for i, ev in enumerate(incoming):
        ident = (ev.replica_id, ev.seq)
        prior = merged_by_id.get(ident)
        if prior is not None:
            if not _same_event_content(prior, ev):
                raise ConflictStateError(
                    [
                        {
                            "code": "event_content_mismatch",
                            "message": (
                                f"event with replica_id {ev.replica_id!r}, "
                                f"seq {ev.seq} is already present in this "
                                "session with different content; prior "
                                f"{_event_wire(prior)!r}, incoming "
                                f"{_event_wire(ev)!r}"
                            ),
                            "event_index": i,
                        }
                    ]
                )
            # Same identity, identical content -> ignore.
            continue
        merged_by_id[ident] = ev
        merged.append(ev)

    if len(merged) > 120:
        raise BatchError(
            [
                _err(
                    "batch_full",
                    "the import session already stages 120 distinct events; "
                    "the 120-event limit leaves no room to stage more",
                )
            ]
        )
    return merged


def _load_session_row(
    row: object, replica_ids: list[str]
) -> tuple[str, str, list[Event]]:
    events = [_wire_model(raw, replica_ids) for raw in json.loads(row["events"])]
    return row["id"], row["status"], events


def _session_state(
    session_id: str,
    history_id: str,
    status: str,
    events: list[Event],
    replica_ids: list[str],
) -> dict:
    """Session status plus the staged events in original wire format."""

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    return {
        "id": session_id,
        "history_id": history_id,
        "status": status,
        "events": [_canonical_wire(ev, replica_ids) for ev in ordered],
    }


def _missing_ranges(
    replica_ids: list[str],
    committed: list[Event],
    staged: list[Event],
) -> list[dict]:
    """Seq ranges still missing from the source for a session commit.

    The two lists are the history's current committed events and the open
    session's staged events -- never merged, only inspected, so neither is
    mutated.

    A staged identity that already exists in the committed history is first
    compared on full original content (key, op, value and the *complete*
    clock, via the same full-equality rule used by merges); every
    same-identity/different-content conflict is collected, sorted by
    (replica_id, seq), and reported together as a 409.

    For each replica the demand upper bound is the maximum of:

    * the highest own seq among the committed+staged events of that replica,
    * every event clock's component for that replica (a staged event may
      legitimately depend on a cross-replica predecessor that has not arrived
      yet).

    Everything from seq 1 up to that bound whose identity is present neither
    in the committed history nor in the session is missing; missing seqs are
    coalesced into the largest possible closed [start_seq, end_seq] ranges.
    A bound of 0 (the replica is demanded neither by its own events nor by
    any clock component) produces no range.  Missing seqs are never expanded
    individually -- a gap reaching 10**12 is still one range.  Ranges are
    sorted by (replica_id, start_seq).  An empty list means only that every
    demanded identity is present; it does not promise the commit passes
    (causal/order validation still runs at commit time).
    """

    committed_by_id = {(ev.replica_id, ev.seq): ev for ev in committed}

    # Full-content conflicts between staged and committed identities first:
    # report *all* of them, sorted by (replica_id, seq), and nothing else.
    conflicts: list[tuple[str, int]] = []
    staged_by_id: dict[tuple[str, int], Event] = {}
    for ev in staged:
        ident = (ev.replica_id, ev.seq)
        prior = committed_by_id.get(ident)
        if prior is not None and not _same_event_content(prior, ev):
            conflicts.append(ident)
        staged_by_id.setdefault(ident, ev)
    if conflicts:
        raise ConflictStateError(
            [
                {
                    "code": "event_content_mismatch",
                    "message": (
                        f"staged event with replica_id {rid!r}, seq {seq} "
                        "already exists in the committed history with "
                        f"different content; stored "
                        f"{_event_wire(committed_by_id[(rid, seq)])!r}, "
                        f"staged {_event_wire(staged_by_id[(rid, seq)])!r}"
                    ),
                    "replica_id": rid,
                    "seq": seq,
                }
                for rid, seq in sorted(set(conflicts))
            ]
        )

    # Per-replica demand upper bound: own max seq and every clock component.
    bound = {rid: 0 for rid in replica_ids}
    present: dict[str, set[int]] = {rid: set() for rid in replica_ids}
    for ev in committed:
        present[ev.replica_id].add(ev.seq)
    for ev in staged:
        present[ev.replica_id].add(ev.seq)
    for ev in committed + staged:
        if ev.seq > bound[ev.replica_id]:
            bound[ev.replica_id] = ev.seq
        # Stored/staged events always cover exactly the history's replicas.
        for rid in replica_ids:
            if ev.clock[rid] > bound[rid]:
                bound[rid] = ev.clock[rid]

    ranges: list[dict] = []
    for rid in replica_ids:
        upper = bound[rid]
        if upper == 0:
            continue
        have = present[rid]
        # Walk only the present identities and derive maximal missing runs
        # from the gaps between them; never iterate every missing seq.
        start = 1
        for n in sorted(have):
            if n > upper:
                break
            if n > start:
                ranges.append(
                    {"replica_id": rid, "start_seq": start, "end_seq": n - 1}
                )
            start = n + 1
        if start <= upper:
            ranges.append(
                {"replica_id": rid, "start_seq": start, "end_seq": upper}
            )

    ranges.sort(key=lambda r: (r["replica_id"], r["start_seq"]))
    return ranges


def _key_verdict(key: str, group: list[Event], replica_ids: list[str]) -> dict:
    """Reduce the events touching one key to its effective-version verdict."""

    maximals = _maximal_events(group, replica_ids)

    if len(maximals) == 1:
        m = maximals[0]
        if m.put is not None:
            return {
                "key": key,
                "state": "put",
                "value": m.put,
                "event": _event_shape(m, replica_ids),
            }
        return {"key": key, "state": "deleted", "event": _event_shape(m, replica_ids)}

    return {
        "key": key,
        "state": "conflict",
        "events": [_event_shape(m, replica_ids) for m in maximals],
    }


def resolve(events: list[Event], replica_ids: list[str]) -> list[dict]:
    """Compute maximal events per key under the vector-clock partial order."""

    by_key: dict[str, list[Event]] = {}
    for ev in events:
        by_key.setdefault(ev.key, []).append(ev)

    return [
        _key_verdict(key, by_key[key], replica_ids)
        for key in sorted(by_key)
    ]


# ---------------------------------------------------------------------------
# Read-only withdrawal preview
# ---------------------------------------------------------------------------


def _direct_dependencies(
    ev: Event, replica_ids: list[str]
) -> set[tuple[str, int]]:
    """Events an event causally builds on: the previous event on its own
    replica and, for every other replica, the event its clock component
    points at (non-zero components only).  Stored histories have passed
    batch validation, so every referenced identity exists."""

    deps: set[tuple[str, int]] = set()
    if ev.seq > 1:
        deps.add((ev.replica_id, ev.seq - 1))
    for rid in replica_ids:
        if rid == ev.replica_id:
            continue
        n = ev.clock[rid]
        if n > 0:
            deps.add((rid, n))
    return deps


def preview_withdrawal(
    replica_ids: list[str],
    events: list[Event],
    target_replica: str,
    target_seq: int,
) -> dict:
    """Pure, non-mutating projection of what withdrawing one event would take.

    The withdrawn set is the target event plus every event that directly or
    transitively depends on it (its reverse reachability under the same
    dependency edges used by batch validation: each replica's previous event
    and the events other replicas' non-zero clock components point at).  All
    other events, including causally unrelated concurrent writes, are kept.

    Returns the withdrawn identities (sorted by replica_id then seq), the
    kept events in the original canonical order (same order, seqs and clocks
    -- nothing is renumbered), and the before/after verdict of every key
    whose full verdict structure would change (sorted by key); a key with no
    remaining event reports an ``after`` verdict of ``None``.
    """

    seen = {(ev.replica_id, ev.seq): ev for ev in events}
    if (target_replica, target_seq) not in seen:
        raise EventNotFound(target_replica, target_seq)

    # Reverse dependency graph: identity -> events that directly depend on it.
    dependents: dict[tuple[str, int], set[tuple[str, int]]] = {}
    for ev in events:
        for dep in _direct_dependencies(ev, replica_ids):
            dependents.setdefault(dep, set()).add((ev.replica_id, ev.seq))

    # Reverse reachability from the target = target plus its transitive
    # dependents, i.e. everything that would have to be withdrawn with it.
    withdrawn: set[tuple[str, int]] = set()
    frontier = [(target_replica, target_seq)]
    while frontier:
        ident = frontier.pop()
        if ident in withdrawn:
            continue
        withdrawn.add(ident)
        frontier.extend(dependents.get(ident, ()))

    ordered = sorted(events, key=lambda e: (e.replica_id, e.seq))
    kept = [ev for ev in ordered if (ev.replica_id, ev.seq) not in withdrawn]

    before_by_key = {v["key"]: v for v in resolve(ordered, replica_ids)}
    after_by_key = {v["key"]: v for v in resolve(kept, replica_ids)}
    touched_keys = {
        ev.key for ev in ordered if (ev.replica_id, ev.seq) in withdrawn
    }

    changes: list[dict] = []
    for key in sorted(touched_keys):
        before = before_by_key[key]
        after = after_by_key.get(key)
        # Compare the complete verdict structures (put/deleted/conflict
        # shapes differ as a whole), not just a state/value subset.
        if before != after:
            changes.append({"key": key, "before": before, "after": after})

    withdrawn_ids = sorted(withdrawn)
    return {
        "withdrawn": [
            {"replica_id": rid, "seq": seq} for rid, seq in withdrawn_ids
        ],
        "events": [_canonical_wire(ev, replica_ids) for ev in kept],
        "changes": changes,
    }


# ---------------------------------------------------------------------------
# Conflict resolution event
# ---------------------------------------------------------------------------


def build_resolution_event(
    replica_ids: list[str],
    events: list[Event],
    seen: dict[tuple[str, int], Event],
    key: str,
    writer: str,
    versions: list[ConflictVersion],
    put: Optional[str],
    delete: Optional[str],
) -> tuple[Event, dict]:
    """Build the replayable event that records a confirmed conflict resolution.

    Structural checks (unknown writer, duplicate versions, full batch) raise
    BatchError (422).  When the declared conflict no longer matches the batch
    -- the key is not conflicting, or the version list is stale -- raises
    ConflictStateError (409) and generates no event.

    Returns the new event and the key's verdict over the appended batch.
    """

    replica_set = set(replica_ids)

    # Structural 422s first.
    if writer not in replica_set:
        raise BatchError(
            [
                _err(
                    "unknown_replica_id",
                    f"writing replica_id {writer!r} is not listed in replica_ids",
                )
            ]
        )

    dupes: set[tuple[str, int]] = set()
    seen_versions: set[tuple[str, int]] = set()
    for v in versions:
        ident = (v.replica_id, v.seq)
        if ident in seen_versions:
            dupes.add(ident)
        seen_versions.add(ident)
    if dupes:
        raise BatchError(
            [
                _err(
                    "duplicate_conflict_version",
                    "conflict version list must contain no duplicates; "
                    f"repeated entries: {sorted(dupes)}",
                )
            ]
        )

    if len(events) >= 120:
        raise BatchError(
            [
                _err(
                    "batch_full",
                    "the original batch already contains 120 events; no room "
                    "to append the resolution event (batch limit is 120)",
                )
            ]
        )

    for v in versions:
        if v.replica_id not in replica_set:
            raise BatchError(
                [
                    _err(
                        "unknown_replica_id",
                        f"version replica_id {v.replica_id!r} is not listed in "
                        "replica_ids",
                    )
                ]
            )

    # 409s: the declared conflict must still be live and exactly observed.
    group = [ev for ev in events if ev.key == key]
    if not group:
        raise ConflictStateError(
            [
                {
                    "code": "key_not_conflicted",
                    "message": f"key {key!r} has no events in the batch",
                }
            ]
        )

    maximals = _maximal_events(group, replica_ids)
    if len(maximals) < 2:
        raise ConflictStateError(
            [
                {
                    "code": "key_not_conflicted",
                    "message": f"key {key!r} is no longer in conflict",
                }
            ]
        )

    current = {(m.replica_id, m.seq) for m in maximals}
    if seen_versions != current:
        raise ConflictStateError(
            [
                {
                    "code": "conflict_versions_mismatch",
                    "message": (
                        "the supplied version list is stale or incomplete: it "
                        f"must equal exactly all current maximal events of "
                        f"{key!r}; supplied {sorted(seen_versions)}, current "
                        f"maximals {sorted(current)}"
                    ),
                }
            ]
        )

    # The new seq continues the writer's tail (1 when it has no events yet).
    writer_max = max(
        (ev.seq for ev in events if ev.replica_id == writer), default=0
    )
    new_seq = writer_max + 1

    # Clock = componentwise maximum of every conflicting version's clock and
    # the writer's prior history, then advance the writer's own component to
    # the new seq.
    clock = {rid: 0 for rid in replica_ids}
    for m in maximals:
        for rid in replica_ids:
            if m.clock[rid] > clock[rid]:
                clock[rid] = m.clock[rid]
    if writer_max > 0:
        prior = seen[(writer, writer_max)]
        for rid in replica_ids:
            if prior.clock[rid] > clock[rid]:
                clock[rid] = prior.clock[rid]
    clock[writer] = new_seq

    new_event = Event(
        replica_id=writer,
        seq=new_seq,
        key=key,
        put=put,
        delete=delete,
        clock=clock,
    )

    appended = events + [new_event]

    # The construction guarantees this, but re-run the original validation on
    # the extended batch so the returned event is replayable by construction.
    validate_batch(replica_ids, appended)

    verdict = _key_verdict(
        key, [ev for ev in appended if ev.key == key], replica_ids
    )
    return new_event, verdict


# ---------------------------------------------------------------------------
# Incremental catch-up plan
# ---------------------------------------------------------------------------


def _max_seq_by_replica(
    replica_ids: list[str], events: list[Event]
) -> dict[str, int]:
    max_seq: dict[str, int] = {rid: 0 for rid in replica_ids}
    for ev in events:
        if ev.seq > max_seq[ev.replica_id]:
            max_seq[ev.replica_id] = ev.seq
    return max_seq


def validate_known_clock(
    replica_ids: list[str],
    known_clock: dict[str, int],
    max_seq: dict[str, int],
) -> None:
    """Validate known_clock against replicas and the batch.

    It must cover exactly all replicas with non-negative integer components,
    and no component may exceed that source's highest seq in this batch.
    Raises BatchError (-> 422) otherwise.
    """

    replica_set = set(replica_ids)
    clock_keys = set(known_clock)

    errors: list[dict] = []
    missing = sorted(replica_set - clock_keys)
    if missing:
        errors.append(
            _err(
                "known_clock_coverage",
                f"known_clock must cover exactly all replica_ids; missing "
                f"components for {missing}",
            )
        )
    extra = sorted(clock_keys - replica_set)
    if extra:
        errors.append(
            _err(
                "known_clock_coverage",
                f"known_clock must cover exactly all replica_ids; unexpected "
                f"components {extra}",
            )
        )
    if errors:
        raise BatchError(errors)

    over: list[str] = []
    for rid in replica_ids:
        if known_clock[rid] > max_seq[rid]:
            over.append(
                f"{rid!r}: known_clock {known_clock[rid]} > highest batch seq "
                f"{max_seq[rid]}"
            )
    if over:
        raise BatchError(
            [
                _err(
                    "known_clock_ahead_of_batch",
                    "known_clock component exceeds that source's highest seq "
                    "in the batch; "
                    + "; ".join(over),
                )
            ]
        )


def validate_received_events(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
) -> None:
    """Reject batches where an already-received event claims newer history.

    An event whose own seq is at or below its source's known_clock entry has
    already been received by the target; every component of its clock must
    then be at or below the corresponding known_clock entry.
    """

    for i, ev in enumerate(events):
        if ev.seq > known_clock[ev.replica_id]:
            continue
        beyond = {
            rid: ev.clock[rid]
            for rid in replica_ids
            if ev.clock[rid] > known_clock[rid]
        }
        if beyond:
            raise BatchError(
                [
                    _err(
                        "clock_exceeds_known_clock",
                        f"event already received (replica {ev.replica_id!r}, "
                        f"seq {ev.seq} <= known_clock {known_clock[ev.replica_id]}) "
                        f"but its clock {ev.clock} exceeds known_clock; "
                        f"components beyond target knowledge: {beyond}",
                        i,
                    )
                ]
            )


def plan_catchup(
    replica_ids: list[str],
    events: list[Event],
    known_clock: dict[str, int],
    max_seq: dict[str, int],
) -> dict:
    """Schedule missing events into replay rounds.

    A missing event is eligible once its previous event on the same replica
    and, for every other replica, the event its clock component points at
    are all either already at the target (covered by known_clock) or were
    scheduled in an earlier round.
    """

    remaining = {
        (ev.replica_id, ev.seq): ev
        for ev in events
        if ev.seq > known_clock[ev.replica_id]
    }

    rounds: list[list[dict]] = []
    received = dict(known_clock)

    while remaining:
        ready: list[Event] = []
        for (rid, n), ev in remaining.items():
            # Previous event on the same replica must be present.
            if n > 1 and n - 1 > received[rid]:
                continue

            # Events on other replicas pointed at by this clock component.
            blocked = any(
                other != rid
                and ev.clock[other] > received[other]
                for other in replica_ids
            )
            if blocked:
                continue

            ready.append(ev)

        if not ready:
            # Defensive: unreachable for a batch that passed causal
            # validation, reported rather than looping forever.
            raise BatchError(
                [
                    _err(
                        "catchup_unschedulable",
                        "missing events form a dependency cycle and cannot be "
                        "scheduled into rounds",
                    )
                ]
            )

        ready.sort(key=lambda e: (e.replica_id, e.seq))
        round_events: list[dict] = []
        for ev in ready:
            round_events.append(
                {
                    "replica_id": ev.replica_id,
                    "seq": ev.seq,
                    "key": ev.key,
                    "op": "put" if ev.put is not None else "delete",
                    "value": ev.put,
                    "clock": {rid: ev.clock[rid] for rid in replica_ids},
                }
            )
            # The local component is the highest seq observed from this source.
            received[ev.replica_id] = ev.seq
            del remaining[(ev.replica_id, ev.seq)]
        rounds.append(round_events)

    return {"rounds": rounds, "progress": {rid: max_seq[rid] for rid in replica_ids}}


# ---------------------------------------------------------------------------
# Routes / error handling
# ---------------------------------------------------------------------------


@app.exception_handler(BatchError)
async def batch_error_handler(request: Request, exc: BatchError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors},
    )


@app.exception_handler(ConflictStateError)
async def conflict_state_error_handler(
    request: Request, exc: ConflictStateError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": exc.errors},
    )


@app.exception_handler(HistoryNotFound)
async def history_not_found_handler(
    request: Request, exc: HistoryNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "history_not_found",
                    "message": f"no history exists with id {exc.history_id!r}",
                }
            ]
        },
    )


@app.exception_handler(EventNotFound)
async def event_not_found_handler(
    request: Request, exc: EventNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "event_not_found",
                    "message": (
                        f"this history has no event with replica_id "
                        f"{exc.replica_id!r} and seq {exc.seq}"
                    ),
                }
            ]
        },
    )


@app.exception_handler(SessionNotFound)
async def session_not_found_handler(
    request: Request, exc: SessionNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "detail": [
                {
                    "code": "session_not_found",
                    "message": (
                        f"this history has no import session with id "
                        f"{exc.session_id!r}"
                    ),
                }
            ]
        },
    )


@app.exception_handler(SessionClosedError)
async def session_closed_handler(
    request: Request, exc: SessionClosedError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": [
                {
                    "code": "session_closed",
                    "message": (
                        f"import session {exc.session_id!r} is already "
                        "closed; it accepts no further appends or commits"
                    ),
                }
            ]
        },
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Keep FastAPI's 422 shape, but drop the non-JSON-serializable 'ctx'
    # payload (it holds the original ValueError object).
    safe_errors: list[dict] = []
    for e in exc.errors():
        safe = {k: v for k, v in e.items() if k != "ctx"}
        safe_errors.append(safe)
    return JSONResponse(status_code=422, content={"detail": safe_errors})


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok"}


@app.post("/api/resolve", tags=["resolve"], response_model=None)
async def resolve_batch(body: ResolveRequest) -> dict:
    """Validate the batch and return the effective version of each key."""

    validate_batch(body.replica_ids, body.events)
    return {"results": resolve(body.events, body.replica_ids)}


@app.post("/api/resolve-conflict", tags=["resolve"], response_model=None)
async def resolve_conflict(body: ResolveConflictRequest) -> dict:
    """Record a confirmed conflict resolution as a replayable event.

    The original batch is validated with the same rules as /api/resolve; the
    supplied conflict versions must then be exactly the key's current maximal
    events (>= 2, no duplicates).  The new event continues the writing
    replica's seq tail with a clock covering the conflicting versions and the
    writer's prior history.
    """

    seen = validate_batch(body.replica_ids, body.events)
    new_event, verdict = build_resolution_event(
        replica_ids=body.replica_ids,
        events=body.events,
        seen=seen,
        key=body.key,
        writer=body.replica_id,
        versions=body.versions,
        put=body.put,
        delete=body.delete,
    )
    return {"event": _event_wire(new_event), "result": verdict}


@app.post("/api/catchup", tags=["catchup"], response_model=None)
async def catchup_batch(body: CatchupRequest) -> dict:
    """Build a replayable incremental transfer plan for a target replica."""

    validate_batch(body.replica_ids, body.events)
    max_seq = _max_seq_by_replica(body.replica_ids, body.events)
    validate_known_clock(body.replica_ids, body.known_clock, max_seq)
    validate_received_events(body.replica_ids, body.events, body.known_clock)
    return plan_catchup(
        body.replica_ids, body.events, body.known_clock, max_seq
    )


# ---------------------------------------------------------------------------
# Persistent histories (/api/histories)
#
# Every mutation runs inside one BEGIN IMMEDIATE transaction (see store.py),
# which serialises all read-modify-write sequences: merges and conflict
# commits can neither lose events nor allocate the same seq twice, and a
# commit is always evaluated against -- and written back to -- the current
# committed state.  Any validation/conflict failure rolls the transaction
# back, so each write is all-or-nothing.
# ---------------------------------------------------------------------------


@app.post("/api/histories", tags=["histories"], response_model=None, status_code=201)
def create_history(body: ResolveRequest) -> dict:
    """Validate a batch and persist it as a new history, returning its ID."""

    validate_batch(body.replica_ids, body.events)
    history_id = uuid.uuid4().hex
    with store.write_transaction() as conn:
        store.insert_history(
            conn,
            history_id,
            body.replica_ids,
            _canonical_events(body.events, body.replica_ids),
        )
    return {"id": history_id}


@app.get("/api/histories/{history_id}", tags=["histories"], response_model=None)
def read_history(history_id: str) -> dict:
    """Return the stored batch (original wire format) and its verdicts.

    Events are sorted by replica_id then seq; both the batch and ``results``
    come from one committed snapshot.  Unknown IDs get 404.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    hid, replica_ids, events = _load_row(row)
    return _history_state(hid, replica_ids, events)


@app.get(
    "/api/histories/{history_id}/withdraw-preview",
    tags=["histories"],
    response_model=None,
)
def preview_history_event_withdrawal(
    history_id: str,
    replica_id: Annotated[
        str,
        Query(
            min_length=1,
            description="Replica ID of the event whose withdrawal is previewed.",
        ),
    ],
    seq: Annotated[
        int,
        Query(
            ge=1,
            description="Per-replica seq of the event whose withdrawal is previewed.",
        ),
    ],
) -> dict:
    """Preview withdrawing one offline event without touching the history.

    Read-only: the stored history is fetched as one committed snapshot and
    never written back.  The previewed withdrawal removes the target event
    plus every event that directly or transitively depends on it (following
    each replica's previous event and other replicas' non-zero clock
    components), across every key; unrelated concurrent events are kept with
    their original seqs and clocks.  The response lists the withdrawn
    identities, the remaining events in original wire order, and the
    before/after verdict of every changed key (``after`` is null when the key
    has no remaining event).  Unknown history/event -> 404; invalid params
    (missing/non-integer/non-positive) -> 422.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    hid, replica_ids, events = _load_row(row)
    preview = preview_withdrawal(replica_ids, events, replica_id, seq)
    return {"id": hid, **preview}


@app.post(
    "/api/histories/{history_id}/events",
    tags=["histories"],
    response_model=None,
)
def merge_history_events(history_id: str, body: MergeEventsRequest) -> dict:
    """Merge offline events into a history and return the merged state.

    Merge key is (replica_id, seq): identical events are ignored and the same
    identity with different content is 409.  The merged history must still
    satisfy the original validation and the 120-event limit, otherwise the
    whole merge is rejected with 422 and nothing is written.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        merged = _merge_events(replica_ids, events, body.events)
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return _history_state(hid, replica_ids, merged)


@app.post(
    "/api/histories/{history_id}/resolve-conflict",
    tags=["histories"],
    response_model=None,
)
def commit_history_conflict(history_id: str, body: ConflictDecision) -> dict:
    """Commit a confirmed conflict resolution into a stored history.

    Only the original resolution parameters are supplied (key, writing
    replica_id, versions, put/delete); the batch itself is read from the
    history under a serialised write lock.  ``versions`` must equal the
    key's current maximal events -- a stale list gets 409 and no event is
    generated.  The new event continues the writer's seq tail and is
    persisted together with the whole history atomically.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, events = _load_row(row)
        new_event, verdict = build_resolution_event(
            replica_ids=replica_ids,
            events=events,
            seen={(ev.replica_id, ev.seq): ev for ev in events},
            key=body.key,
            writer=body.replica_id,
            versions=body.versions,
            put=body.put,
            delete=body.delete,
        )
        merged = events + [new_event]
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        return {"id": hid, "event": _canonical_wire(new_event, replica_ids),
                "result": verdict}


# ---------------------------------------------------------------------------
# Import sessions (/api/histories/{id}/import-sessions)
#
# Sessions stage offline events that arrive in several batches; the staged
# events only reach the persistent history when the session is committed.
# Staging applies the per-event checks (fields, replica membership, full
# clock coverage, own component == seq) to genuinely new identities only and
# tolerates not-yet-arrived predecessors or cross-replica dependencies;
# same-identity retransmissions -- against staged events or earlier entries
# of the same batch -- skip those checks and are judged by full-content
# equality (identical ignored; a missing component, an extra component even
# when zero, or a changed component -> 409, never 422).  The 120-event cap
# counts distinct identities after in-batch deduplication.  Commit re-runs
# the original merge (full-content equality, causal validation, 120-event
# cap) against the history's latest committed state.  Every mutation runs
# inside one BEGIN IMMEDIATE transaction together with the history row lock,
# so a commit serialises with the legacy write endpoints and cannot lose
# events, the merge and the session close take effect together, and any
# failure rolls everything back, leaving the open session and its staged
# events untouched for a retry.
# ---------------------------------------------------------------------------


@app.post(
    "/api/histories/{history_id}/import-sessions",
    tags=["import-sessions"],
    response_model=None,
    status_code=201,
)
def create_import_session(history_id: str) -> dict:
    """Open an empty import session on a history and return its ID."""

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        session_id = uuid.uuid4().hex
        store.insert_session(conn, session_id, history_id)
    return {"id": session_id}


@app.get(
    "/api/histories/{history_id}/import-sessions/{session_id}",
    tags=["import-sessions"],
    response_model=None,
)
def read_import_session(history_id: str, session_id: str) -> dict:
    """Return the session status and its staged events.

    Staged events come back in the original wire format, sorted by
    replica_id then seq.  Unknown history or session IDs get 404.
    """

    row = store.get_history(history_id)
    if row is None:
        raise HistoryNotFound(history_id)
    replica_ids: list[str] = json.loads(row["replica_ids"])
    srow = store.get_session(history_id, session_id)
    if srow is None:
        raise SessionNotFound(session_id)
    sid, status, staged = _load_session_row(srow, replica_ids)
    return _session_state(sid, history_id, status, staged, replica_ids)


@app.get(
    "/api/histories/{history_id}/import-sessions/{session_id}/missing-ranges",
    tags=["import-sessions"],
    response_model=None,
)
def read_import_session_missing_ranges(
    history_id: str, session_id: str
) -> dict:
    """List the seq ranges a session still must fetch before a commit.

    Read-only: the history, the session and their statuses are never
    modified and the session is not committed.  The committed history and
    the staged events come from one committed snapshot.  A staged identity
    that the committed history already holds with different full content is
    409 listing every conflicting (replica_id, seq), sorted; otherwise the
    response is 200 with ``missing_ranges`` -- closed [start_seq, end_seq]
    ranges sorted by replica_id then start_seq.  An empty list means all
    demanded identities are present, not that the commit is guaranteed to
    pass.  Closed sessions are queryable too; an unknown history or session
    is 404.
    """

    hrow, srow = store.get_history_with_session(history_id, session_id)
    if hrow is None:
        raise HistoryNotFound(history_id)
    _, replica_ids, committed = _load_row(hrow)
    if srow is None:
        raise SessionNotFound(session_id)
    _, _, staged = _load_session_row(srow, replica_ids)
    return {"missing_ranges": _missing_ranges(replica_ids, committed, staged)}


@app.post(
    "/api/histories/{history_id}/import-sessions/{session_id}/events",
    tags=["import-sessions"],
    response_model=None,
)
def stage_import_session_events(
    history_id: str, session_id: str, body: StageEventsRequest
) -> dict:
    """Stage a batch of events into an open session and return its state.

    Per-event checks (422 on unknown replica, clock coverage or own
    component) apply only to genuinely new (replica_id, seq) identities;
    missing dependencies are tolerated until commit.  Retransmissions of an
    already-staged or earlier-in-batch identity skip those checks and are
    compared by full content: a missing component, an extra component --
    even one with value 0 -- or a changed own component is different content
    and gets 409 (event_content_mismatch, with the batch event_index), never
    422; identical content is ignored.  The 120-event cap counts already
    staged identities plus the new identities after in-batch deduplication,
    so 121 identical resends of one legal event stage one and a full session
    re-sending its events still succeeds; a distinct identity beyond the cap
    is 422.  A failed append changes nothing; a closed session is 409.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, _ = _load_row(row)
        srow = store.select_session_for_update(conn, history_id, session_id)
        if srow is None:
            raise SessionNotFound(session_id)
        if srow["status"] != "open":
            raise SessionClosedError(session_id)
        _, status, staged = _load_session_row(srow, replica_ids)
        merged = _stage_events(replica_ids, staged, body.events)
        store.update_session_events(
            conn, session_id, _canonical_events(merged, replica_ids)
        )
        return _session_state(session_id, hid, status, merged, replica_ids)


@app.post(
    "/api/histories/{history_id}/import-sessions/{session_id}/commit",
    tags=["import-sessions"],
    response_model=None,
)
def commit_import_session(history_id: str, session_id: str) -> dict:
    """Merge the staged events into the history and close the session.

    The merge runs against the history's latest committed state under the
    same serialised write lock as the legacy write endpoints, with the
    original full-content equality (identical events are ignored, different
    content under a stored identity is 409), causal validation and the
    120-event total limit (violations are 422).  On success the merged
    history and the session close take effect together and the response is
    the history read structure; on failure the open session and its staged
    events are kept, so missing events can be appended and the commit
    retried.  A closed session is 409.
    """

    with store.write_transaction() as conn:
        row = store.select_for_update(conn, history_id)
        if row is None:
            raise HistoryNotFound(history_id)
        hid, replica_ids, committed = _load_row(row)
        srow = store.select_session_for_update(conn, history_id, session_id)
        if srow is None:
            raise SessionNotFound(session_id)
        if srow["status"] != "open":
            raise SessionClosedError(session_id)
        _, _, staged = _load_session_row(srow, replica_ids)
        merged = _merge_events(replica_ids, committed, staged)
        store.update_events(
            conn, hid, _canonical_events(merged, replica_ids)
        )
        store.close_session(conn, session_id)
        return _history_state(hid, replica_ids, merged)
