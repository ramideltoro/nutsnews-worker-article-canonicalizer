# Canonicalization Replay Proof

## Sanitized Corpus

The local reliability suite runs `sanitized-parity-inventory-canonicalizer-fixture`, a value-free corpus with six candidate references:

- one first-seen canonical article;
- one cross-feed duplicate with tracking parameters removed;
- one material change on the same canonical identity;
- one URL alias for the same source GUID;
- one unrelated first-seen article;
- one source GUID and URL collision that requires manual ambiguity review.

No article HTML, AI output, credential, or production identifier is included.

## Replay Rates

The replay report produced from the fixture records:

- `new`: 2
- `duplicate`: 1
- `alias`: 1
- `changed`: 1
- `ambiguous`: 1
- `invalid`: 0
- duplicate rate: `1 / 6`
- change rate: `1 / 6`

The fixture's legacy comparison baseline is one duplicate and one changed candidate, so duplicate and change deltas are both `0`.

## Crash Points

The reliability suite covers retry behavior around these crash points:

- before inbox insert;
- after inbox insert and before canonical resolution;
- before pending outbox insert, proving staged canonical state is rolled back;
- after pending outbox insert and before ack, proving replay does not create duplicate enrichment work.

The broker publish-confirm path is exercised with `@ramideltoro/nutsnews-worker-contracts@1.0.0`. New and changed decisions publish one contracted `enrichmentRequest` command after the transaction commits, record the publish receipt in the outbox interface, and replay as duplicate without publishing a second command. Runtime 1.0 conformance also covers lost claim responses, completion rejection before and after commit, stale-token release, completed-record preservation, and lease expiry/reclaim. The current process-clock store is a test double, not production lease evidence. Production admission additionally requires server-authoritative time and a finish/renew/fail-closed policy for work that can approach the 300-second lease bound; until a durable adapter provides that, mixed-mode startup remains unready and registers no consumer.

## Ambiguity Review

Ambiguous replay decisions produce a bounded review item with only:

- candidate ID;
- feed ID;
- source item ID;
- canonical article ID;
- normalized URL;
- safe reason codes.
