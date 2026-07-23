# John and Abigail Adams example corpus

This example contains 20 letters exchanged by John and Abigail Adams between
April 1775 and July 1776: ten by each correspondent.

The sample supports the tutorial research question:

> How did John and Abigail Adams negotiate public duty, household
> responsibility, political voice, danger, and emotional intimacy during the
> American Revolution?

## Source

The letters were selected from John Adams, Abigail Adams, and Charles Francis
Adams (editor), *Familiar Letters of John Adams and His Wife Abigail Adams
During the Revolution* (1876), Project Gutenberg ebook 34123:

https://www.gutenberg.org/ebooks/34123

The source edition was produced from scans of public-domain material. Its
line wrapping was normalized, its footnotes were removed, and a short metadata
header was added to each file. Spelling, capitalization, and wording were
otherwise retained.

The complete Project Gutenberg redistribution terms are included in
`PROJECT_GUTENBERG_LICENSE.txt`. The underlying 1876 edition is in the public
domain in the United States. Project Gutenberg notes that users outside the
United States should check the law in their jurisdiction.

For comparison with manuscript images and modern scholarly transcriptions, see
the Massachusetts Historical Society's Adams Electronic Archive:

https://www.masshist.org/digitaladams/archive/

## Try the corpus

From this directory:

```bash
bewley init
for letter in corpus/*.txt; do
  bewley add "$letter"
done
bewley list documents
bewley status
```

The generated, searchable demonstration report is published with the main
documentation at `docs/adams-report.html`.

## Suggested starter codes

- `public_duty`
- `household_responsibility`
- `political_voice`
- `war_and_danger`
- `health_and_scarcity`
- `separation_and_affection`
- `information_and_delay`

These are prompts for analysis, not authoritative historical categories.
Readers should inspect the full letters, compare disconfirming cases, and
record codebook decisions in memos.
