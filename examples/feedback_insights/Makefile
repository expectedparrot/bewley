# Executable runbook for Bewley's evidence-grounded feedback workflow.
# Paid model stages are explicit `*-run` targets; there is intentionally no `all` target.
BEWLEY ?= bewley
MODEL ?= gpt-5-mini
SEED ?= 20260803

.PHONY: next discovery-jobs discovery-run discovery-ingest consolidation-jobs consolidation-run consolidation-ingest classification-jobs classification-run classification-ingest aggregate explorer check

next:
	$(BEWLEY) next

discovery-jobs:
	$(BEWLEY) insights discover jobs --seed $(SEED) --bundle-size 25 --coverage 2 --output runs/001-discovery/jobs.ep
	ep models create --model $(MODEL) --max-tokens 8000 --parameter reasoning_effort='"low"' --output runs/001-discovery/models.json

discovery-run:
	ep run --jobs runs/001-discovery/jobs.ep --model_list runs/001-discovery/models.json --output runs/001-discovery/results.ep

discovery-ingest:
	$(BEWLEY) insights discover ingest runs/001-discovery/results.ep --jobs runs/001-discovery/jobs.ep

consolidation-jobs:
	$(BEWLEY) insights consolidate jobs --candidates runs/001-discovery/candidates.jsonl --output runs/002-consolidation/jobs.ep --min-codes 8 --max-codes 20
	ep models create --model $(MODEL) --max-tokens 12000 --parameter reasoning_effort='"low"' --output runs/002-consolidation/models.json

consolidation-run:
	ep run --jobs runs/002-consolidation/jobs.ep --model_list runs/002-consolidation/models.json --output runs/002-consolidation/results.ep

consolidation-ingest:
	$(BEWLEY) insights consolidate ingest runs/002-consolidation/results.ep --jobs runs/002-consolidation/jobs.ep

classification-jobs:
	$(BEWLEY) insights classify jobs --output runs/003-classification/jobs.ep
	ep models create --model $(MODEL) --max-tokens 4000 --parameter reasoning_effort='"low"' --output runs/003-classification/models.json

classification-run:
	ep run --jobs runs/003-classification/jobs.ep --model_list runs/003-classification/models.json --output runs/003-classification/results.ep

classification-ingest:
	$(BEWLEY) insights classify ingest runs/003-classification/results.ep --jobs runs/003-classification/jobs.ep

aggregate:
	$(BEWLEY) insights aggregate

explorer:
	$(BEWLEY) insights evidence-export

check:
	$(BEWLEY) fsck
