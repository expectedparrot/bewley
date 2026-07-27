# Adams interviews example corpus (FICTIONAL)

Three entirely fictional Q&A interview transcripts — two one-on-one
interviews and one joint interview — written in 2026 as synthetic
demonstration data for the bewley tutorial. These conversations never took
place. Do not cite them as historical sources.

## Purpose

The genuine [adams-letters](../adams-letters/) example demonstrates coding
single-voice documents. This companion corpus exists to demonstrate
**speaker-aware coding**: each transcript contains line-labeled speaker
turns (`INTERVIEWER:`, `ABIGAIL ADAMS:`, `JOHN ADAMS:`), including one
multi-participant transcript, so tutorials and tests can exercise
transcript segmentation, speaker roles, and the rule that codes anchor in
participant answers rather than interviewer questions.

## Provenance and marking

- Every file's header carries `FICTIONAL` status lines stating that the
  content is synthetic.
- Themes, and a small number of quoted phrases inside invented answers,
  are drawn from the genuine 1775-1776 Adams correspondence in the
  adams-letters example (public-domain, Project Gutenberg ebook 34123).
  The interview questions and answers themselves are inventions.
- Written for Expected Parrot's bewley project; same license as the
  repository (MIT).

## Format

Line-anchored speaker labels: a turn starts with `LABEL:` at the beginning
of a line; unlabeled lines continue the preceding turn. Header lines
(`Title:`, `Format:`, `Status:`) precede the first turn.
