# RFC 001 — Research objects: Study, Case, Attribute, Speaker, and entity links

- **Status:** draft, for review (issue [#4](https://github.com/expectedparrot/bewley/issues/4))
- **Date:** 2026-07-27
- **Authority note:** this document describes a design, not current behavior.
  Once a slice is implemented, the CLI (`--help`, `guide`, `capabilities`) and
  tests are authoritative and this RFC records rationale only.

## Contents

- [Motivation](#motivation)
- [Design principles](#design-principles)
- [Objects](#objects)
  - [Study and research questions](#study-and-research-questions)
  - [Case](#case)
  - [Attributes](#attributes)
  - [Speaker](#speaker)
  - [Generalized entity links](#generalized-entity-links)
- [Event types](#event-types)
- [Projection schema](#projection-schema)
- [CLI surface](#cli-surface)
- [Integration with existing machinery](#integration-with-existing-machinery)
- [REFI-QDA compatibility](#refi-qda-compatibility)
- [Decisions considered and simplified](#decisions-considered-and-simplified)
- [Out of scope](#out-of-scope)
- [Implementation slices](#implementation-slices)

## Motivation

Bewley models an auditable coding project — documents, revisions, codes,
annotations, memos, events — but not the study those documents belong to.
Nothing represents who or what a document is about, so the questions real
studies ask ("how does this theme differ by occupation, site, or wave?")
cannot be expressed. The interviewer/participant problem (#3) is one instance
of the same gap: text has voices, voices belong to people, and people have
attributes, but none of that is first-class.

These objects are defined together, before any is implemented, because they
constrain each other (a Speaker resolves to a Case; an Attribute belongs to a
Case; a Study's unit of analysis decides what a Case is) and because the
REFI-QDA interchange standard should shape their form now rather than being
retrofitted.

## Design principles

1. **The event log stays authoritative.** Every object arrives as new event
   types; SQLite tables are rebuildable projections extended through the
   existing `_apply_schema` path. `rebuild-index` must reconstruct everything.
2. **Additive only.** No existing event payload or table changes shape.
   Projects without research objects behave exactly as today.
3. **Nothing is inferred silently.** Cases are never auto-created from file
   names; case–document relationships are explicit commands; unresolved
   states (a speaker label with no role, a case-less corpus in a study that
   declares a case unit) surface through `bewley next`, fail-closed.
4. **Agent-first.** Every operation is an envelope-emitting CLI command.
   Any future UI is a client of the same events.
5. **Interchange-compatible shapes.** Field and relationship forms map onto
   REFI-QDA concepts (Cases, Variables/VariableValues, Users, Sets, Links) so
   a future `.qdpx` exporter is a projection, not a migration.

## Objects

### Study and research questions

A minimal, singleton manifest — deliberately small so it informs `bewley
next` without becoming form-filling theater. Three fields plus questions:

| Field | Meaning | Values |
|---|---|---|
| `method` | declared analytic approach | free string; suggested vocabulary: `reflexive-ta`, `codebook-ta`, `content-analysis`, `grounded-theory`, `framework`, `other` |
| `unit_of_analysis` | what one analytic case is | free string; suggested: `document`, `participant`, `organization`, `site`, `event` |
| `purpose` | one-paragraph statement | free text |

Research questions are ordered, identified objects (so themes, findings, and
queries can link to them later via entity links):

```
bewley study set --method grounded-theory --unit participant
bewley study show
bewley question add "How did the Adamses negotiate public duty and private life?"
bewley question list
```

Future fields (sampling rationale, ethics/consent policy, positionality
notes) are explicitly deferred until reporting-standard exports exist to
consume them.

### Case

A Case is a person, organization, site, or event the study is about. Cases
relate **many-to-many** to documents with a typed relationship, which covers:
one interview per participant, several participants in one focus group,
several interviews with one participant, and cases discussed in documents
they did not author.

| Field | Notes |
|---|---|
| `case_id` | generated |
| `name` | display name, unique among active cases |
| `case_type` | plain optional label (`person`, `organization`, `site`, …) |
| `description` | free text |
| `status` | active / archived |

Case–document relationships are entity links (see below) with relationship
values from a validated set: `author`, `participant`, `subject`, `site`,
`other`. Sugar commands make the workhorse path short:

```
bewley case create "Abigail Adams" --type person
bewley case link "Abigail Adams" corpus/1775-may-04-abigail-adams.txt --as author
bewley case list
bewley case show abigail        # prefix resolution, like documents and codes
```

Case-level memos reuse the existing memo machinery (memos already target
typed objects).

### Attributes

Typed, project-wide attribute definitions with per-case values. Definitions
are project-wide rather than per-case-type — this matches REFI-QDA Variables
and avoids a type system nobody asked for yet.

Definition: `attribute_id`, `name`, `value_type` (`text` | `number` |
`boolean` | `date` | `categorical`), optional `allowed_values` for
categorical.

Value: one per (case, attribute), latest-wins through events. A value is
either a typed literal or one of four explicit **special states** —
`missing`, `unknown`, `not_applicable`, `confidential` — because absence of
a row must never be conflated with "we asked and they declined."
`confidential` values are stored but excluded from exports by default.

```
bewley attribute define role --type categorical --values "correspondent,statesman"
bewley case set "Abigail Adams" role correspondent
bewley case set "John Adams" age --special unknown
bewley attribute list
```

Validation is fail-closed: a categorical value outside `allowed_values` is an
`INVALID_INPUT` error listing the allowed set.

### Speaker

Defined in detail in #3; this RFC fixes only the identity shape so #3
conforms. A speaker is **scoped to a document**: the pair
`(document_id, label)` as produced by transcript segmentation. Global
identity comes from linking a document-speaker to a Case:

```
bewley speakers set-role INT interviewer          # role: interviewer|participant|other  (#3)
bewley speakers link-case R1 "Abigail Adams"      # entity link speaker -> case
```

Roles answer "should this voice be coded?"; case links answer "whose voice is
it?". Both are events. A speaker with a role but no case link is fine
(anonymous participant); a label with no role surfaces in `next`.

### Generalized entity links

One typed link primitive between any two research entities replaces the
accumulation of special-purpose relationship tables. An entity reference is
`kind:ref` where `kind` ∈ `document`, `code`, `annotation`, `memo`, `case`,
`speaker`, `question` (extensible), and `ref` uses each kind's existing
resolution rules (ids, paths, names, prefixes).

```
bewley link add case:"Abigail Adams" document:corpus/1775-may-04-abigail-adams.txt --rel author
bewley link list --entity case:"Abigail Adams"
bewley link remove <link_id>
```

A validation table declares allowed `(source_kind, relationship,
target_kind)` combinations; unknown combinations are rejected with the
allowed set in the error context, and the table grows by design change, not
at runtime. Initial rows:

| source | relationship | target |
|---|---|---|
| case | author / participant / subject / site / other | document |
| speaker | is | case |
| memo | supports / challenges | code |
| annotation | supports / challenges | question |
| code | elaborates | question |

Existing `code link` commands and `code_link_created` events are **kept
unchanged** for compatibility; the `entity_links` projection table also
materializes rows derived from code-link events so queries have one place to
look. Whether `code link` later becomes sugar over entity links is a
separate, post-0.5 decision.

## Event types

All payloads follow existing conventions (generated ids, actor/timestamp from
the envelope machinery, compensating events for undo rather than deletion).

| Event | Payload sketch |
|---|---|
| `study_configured` | any of `method`, `unit_of_analysis`, `purpose` (partial update) |
| `research_question_added` | `question_id`, `text` |
| `research_question_updated` | `question_id`, `text?`, `status?` (`active`/`retired`) |
| `case_created` | `case_id`, `name`, `case_type?`, `description?` |
| `case_updated` | `case_id`, changed fields |
| `case_archived` | `case_id` |
| `attribute_defined` | `attribute_id`, `name`, `value_type`, `allowed_values?` |
| `attribute_value_set` | `case_id`, `attribute_id`, `value?`, `special?` (exactly one) |
| `entity_link_created` | `link_id`, `source_kind`, `source_id`, `relationship`, `target_kind`, `target_id`, `memo?` |
| `entity_link_removed` | `link_id` |
| `document_segmented` | (#3) `document_id`, `revision_id`, `rule`, `turns: [{start_byte, end_byte, label}]` |
| `speaker_role_set` | (#3) `document_id` or project scope, `label`, `role` |

`speaker link-case` emits `entity_link_created` with `source_kind:
"speaker"`, `source_id: "<document_id>:<label>"`.

## Projection schema

Additions to `_SCHEMA_SQL` (all rebuildable from events):

```sql
CREATE TABLE study_settings   (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE research_questions (question_id TEXT PRIMARY KEY, text TEXT NOT NULL,
                                 status TEXT NOT NULL DEFAULT 'active', created_at TEXT);
CREATE TABLE cases            (case_id TEXT PRIMARY KEY, name TEXT NOT NULL,
                               case_type TEXT, description TEXT,
                               status TEXT NOT NULL DEFAULT 'active', created_at TEXT);
CREATE TABLE attribute_definitions (attribute_id TEXT PRIMARY KEY, name TEXT NOT NULL,
                                    value_type TEXT NOT NULL, allowed_values TEXT);
CREATE TABLE attribute_values (case_id TEXT NOT NULL, attribute_id TEXT NOT NULL,
                               value TEXT, special TEXT,
                               PRIMARY KEY (case_id, attribute_id));
CREATE TABLE entity_links     (link_id TEXT PRIMARY KEY,
                               source_kind TEXT NOT NULL, source_id TEXT NOT NULL,
                               relationship TEXT NOT NULL,
                               target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
                               memo TEXT, created_at TEXT, is_active INTEGER NOT NULL DEFAULT 1);
-- #3 adds: document_speakers(document_id, revision_id, label, role),
--          speaker_turns(document_id, revision_id, label, start_byte, end_byte)
```

## CLI surface

New command groups `study`, `question`, `case`, `attribute`, `link` (plus
`speakers` from #3), each registered in `_COMMAND_GROUPS`, documented in
`docs_content/commands.md` (docs↔CLI contract test enforces both directions),
with `--human` Rich renderers for the list/show commands.

## Integration with existing machinery

- **`bewley next`** becomes study-aware, in priority order: study with no
  method/questions → suggest `study set` / `question add`; declared
  case-unit study with zero cases → suggest `case create`; documents
  unlinked to any case when cases exist → itemize; speaker labels without
  roles (#3) → itemize.
- **Query engine** (later, "query v2"): attribute and case predicates
  (`--where 'case.role == "correspondent"'`, `--speaker participant`) filter
  through entity links; out of scope for the first slices but the projection
  schema above is designed for those joins.
- **Plots**: code × case matrix and per-attribute facets become possible;
  they follow the query work, not the object work.
- **Exports**: quote exports gain optional case/speaker columns once links
  exist; `confidential` attribute values are excluded unless explicitly
  requested.

## REFI-QDA compatibility

Concept-level mapping (element names to be validated against the published
XSD in the implementation issue for interchange — do not treat this table as
the standard's exact vocabulary):

| Bewley | REFI-QDA concept |
|---|---|
| Case | Case |
| attribute definition / value | Variable / VariableValue |
| document | Source |
| annotation | Selection/coding on a Source |
| memo | Note |
| entity link | Link (where representable), else vendor extension |
| study manifest, questions | Project description + Notes |
| speaker turns/roles | no direct equivalent; exported as vendor extension, degradable to Notes |

The binding constraint honored now: cases are plain entities with typed
variables and many-to-many source relationships — the shape the standard
expects — rather than something bespoke that would need lossy translation.

## Decisions considered and simplified

- **Case types as declared objects** (with type-scoped attributes) —
  simplified to a plain label + project-wide attributes, matching REFI-QDA
  Variables. Revisit only if real projects hit attribute-name collisions
  across types.
- **Global Speaker objects** — rejected; a speaker is document-scoped, and
  identity across documents is exactly what Case provides. One concept fewer.
- **Auto-linking cases from document metadata headers** — rejected as a
  default (nothing inferred silently); a future importer may propose links
  through the same review-decision machinery as model proposals (#5).
- **Making `code link` sugar over entity links now** — deferred; dual
  materialization in the projection gives unified reads without touching a
  stable event type.

## Out of scope

Themes and findings as first-class objects (arrive later atop entity links),
coder branches and multi-user workspaces, multimodal anchors, sampling and
ethics manifest fields, method packs, REFI-QDA import/export implementation.

## Implementation slices

Each slice ships events + projection + CLI + tests + `commands.md` +
tutorial touch-up, independently mergeable, in this order:

1. **Study + questions** — smallest, unlocks study-aware `next`.
2. **Cases + attributes + entity links** — the analytical core; includes
   the `link` primitive and case sugar commands.
3. **Speakers** — #3 phase 1 (segmentation, roles, `speaker_scope` on
   annotations), conforming to the identity shape here.
4. **Follow-ons tracked separately:** review decisions (#5), structured
   codebook (#6), query v2 / matrices, REFI-QDA exchange (umbrella #7).
