# Bewley 0.5 implementation and handoff — 2026-09-08

Implements the sequence in [the review](review-2026-09-08.md), prepared as
version 0.5.0. Publishing the source changes does not publish a tagged release;
release publication remains a separate milestone.

## Delivered

- **Persistence and recovery:** validate mutations in a SQLite transaction under
  the writer lock before publishing an immutable event. A conflicting undo
  leaves the accepted log unchanged. An interrupted append blocks further
  writes and supports explicit `rebuild-index --repair-head` recovery. Kernel
  locks release automatically when a process exits. Merge cycles are rejected.
- **Integrity:** hash referenced objects; verify event hashes, sequence, parent
  chain, and HEAD; replay and compare every projection table's contents; check
  structural references and exact annotation text. Rebuilding refuses corrupt
  evidence. Packing validates the store under the same writer lock.
- **Run evidence:** all ingest workflows bind returned scenarios to originating
  Jobs. Open-coding requires `--jobs`, rejects unexpected scenarios/models, and
  checks declared model coverage. Registered Jobs and execution manifests cannot
  be silently changed. Full candidate proposals, including quotations, revision,
  offsets, model, and Results attribution, survive in the ingest sidecar.
- **Review:** unreviewed candidates remain undecided. The previous
  review-by-deletion behavior requires `--accept-csv-rows`. Reviewed stable code
  IDs survive code renames between packaging and application.
- **Bundles:** successful workflow commands register immutable artifact versions.
  Custom Jobs, Results, review files, manifests, focused/feedback outputs, and
  sidecars travel with the bundle. Historical snapshots survive working-file
  changes. External input snapshots travel; their absolute locators are not
  recreated outside the restored project.
- **Research workflows:** `open-coding jobs --codebook RELEASE --document REF`
  packages a released codebook for new material. Queries add case, typed
  attribute, speaker, and accepted metadata filters. Quote exports add
  case/attribute/speaker filters and source lineage. `export matrix` reports
  code × case annotation counts, coded-document counts, eligible-document
  denominators, and shares, respecting linked speakers in group interviews.
- **Source lineage:** `source add`, `transcription`, `derive`, `metadata`, `show`,
  `boundary`, and `export-raw` preserve original bytes, exact external
  transcription output, explicit derivative boundaries, transformations, and
  field-level metadata provenance. `derive --existing` attaches provenance to an
  existing current revision. Uncertain boundaries block open-coding packaging
  until explicitly reviewed. Analysis text is never prefixed with metadata.
- **Discovery and maintenance:** guide catalogs are generated from the CLI;
  missing-value next steps return executable help; action mutation flags are
  conservative; open-coding guidance recognizes registered custom runs and
  outstanding decisions; thematic analysis avoids grounded-theory core-category
  guidance. Integrity, artifact, source, codebook, scoped-analysis, and shared
  Results-validation helpers now have owning modules.
- **Builds:** EDSL is pinned to the tested revision
  `02c9d1c8e273d9257f3f9d5f380b91a18747b8cb`. CI covers Python 3.11/3.12,
  compileall, pytest including contract sync, distribution builds, and installed
  wheel resources outside the checkout. Latest-Typer exit handling and model
  fixture/service selection work in a clean dependency environment.

## Validation

- Clean editable installation with development dependencies, without system
  site packages: Python 3.11, Typer 0.27.2, Click 8.5.0, pinned EDSL.
- Full suite: **271 passed**, up from the 247-test review baseline. After the
  final packaging service-selection change, all 17 affected packaging,
  provenance, and docs/CLI contract tests passed again. Remaining targeted-run
  warnings are two upstream EDSL/Pydantic deprecations.
- `python -m compileall -q src`, lint checks on new modules/tests, and
  `git diff --check` pass.
- Stronger `bewley fsck` passes on the existing research project: 105 documents,
  913 codes, 955 active annotations. Existing research events and evidence were
  not rewritten.
- Fault tests cover conflicting undo, interruption after event publication,
  corrupt sources and projections, HEAD/parent damage, wrong-run/changed-source
  Results, altered manifests, and missing declared models.
- Bundle round trips cover custom open-coding artifacts, the focused framework
  and mapping/apply workflow, feedback classification/aggregation/reporting, and
  source/raw-transcription/derivative metadata.
- Source distribution and wheel build successfully. The wheel installs in a
  separate clean environment and exposes its version, guide, schemas, command
  docs, and source commands outside the checkout. Distribution contents exclude
  private workspace material.
- Public Adams artifacts are regenerated through the supplied driver and index
  builder. Previous tutorial runs are archived rather than deleted. Model
  responses in the tutorial and tests are synthetic; no paid inference or
  transcription was executed.

## Migration and remaining work

The envelope remains schema 2.0. Existing events are not rewritten. New
artifact/source events require Bewley 0.5 or newer; use the migration notes in
`bewley docs show commands`. Writer locking currently requires POSIX.

The lineage work is the narrow first slice of #11. It imports externally
prepared raw output and analysis text; it does not run OCR, infer metadata,
automatically normalize/split documents, or map every transformed quote back to
an exact raw substring. Boundaries are raw byte ranges. Accepted metadata
filters currently use equality. Existing ordinary documents remain usable with
explicitly unrecorded lineage until attached. New revisions require new lineage.

Declared model coverage is available for packaged execution metadata; legacy
runs without a declared model list cannot reveal a model that never returned.
Full execution registration across every workflow and a discovery-only agent
journey remain follow-up acceptance work. Existing immutable snapshots preserve
each registered version; pre-0.5 unregistered runs use the legacy bundle filename
fallback until re-ingested or otherwise registered.

Issue disposition after review: #4's RFC and three slices are delivered; #9's
usage-recovery acceptance criteria are implemented and tested. Both are ready
for closure. Retain #1 for reconciliation of remaining audit items; #7 until the
release is published; #8 for its full discovery-only journey; and #11 for the
rest of the corpus contract. Defer #10's multi-channel substrate as proposed.
Media extraction, REFI-QDA interchange, a review-file round trip, and broader
refactoring remain separate work.
