# stapel-realtime — contract emission + drift gate (contract-pipeline.md §2-3).
#
# docs/capabilities.json is HAND-WRITTEN apart from two derived parts, the same
# honest boundary stapel-attributes documents: this L1 library has no gate
# registry and no docs/schema.json (no HTTP surface of its own), so
# provides/axes/extension_points/requires are curated prose. `make contract`
# patches in module/version (from pyproject) and the derived `surface` — the
# module-level functions a product is meant to CALL. A new public function in a
# declared surface root fails `make contract` until it carries an intent line in
# docs/capabilities.meta.json.
#
# There is no flows.json/errors.json here: an L1 library with no HTTP surface
# and no registered error keys has nothing to emit into them, and an empty
# artifact would be a lie in the catalog rather than an absence.
#
# README.md is assembled by stapel_tools.readme from docs/readme.md (the human
# half) plus everything above. Badges, version, surface counts and doc links are
# generated, so a release cannot leave them behind. Edit docs/readme.md; never
# README.md.
#
# PYTHON must have stapel-tools importable (the workspace venv, or
# `pip install stapel-tools`).
PYTHON ?= python3

.PHONY: contract contract-check test lint e2e

contract:
	$(PYTHON) -m stapel_tools.surface . --patch
	$(PYTHON) -m stapel_tools.llms_txt . --out docs
	$(PYTHON) -m stapel_tools.readme .

# Drift gate: regenerate into a temp dir and diff against the committed docs/*.
contract-check:
	$(PYTHON) -m stapel_tools.surface . --patch --check
	@tmp=$$(mktemp -d); \
	$(PYTHON) -m stapel_tools.llms_txt . --out "$$tmp" || { rm -rf "$$tmp"; exit 1; }; \
	if ! diff -q docs/llms.txt "$$tmp/llms.txt" >/dev/null 2>&1; then \
		echo "DRIFT: docs/llms.txt is stale — run 'make contract' and commit it"; \
		diff docs/llms.txt "$$tmp/llms.txt" | head -20; \
		rm -rf "$$tmp"; exit 1; \
	fi; \
	rm -rf "$$tmp"; \
	$(PYTHON) -m stapel_tools.readme . --check || exit 1; \
	echo "contract-check: docs/llms.txt + README.md up to date"

lint:
	$(PYTHON) -m ruff check . --select E,F,W --ignore E501

test:
	$(PYTHON) -m pytest tests/ -q

# The live proof: two worker processes, one real redis (docker), real sockets.
# Not part of CI — it needs a container runtime. Run it before a release and
# whenever the transport or the consumers change.
e2e:
	$(PYTHON) e2e/run_e2e.py
