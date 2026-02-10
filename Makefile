.PHONY: licenses

licenses:
	cargo about generate about.hbs -o RUST_THIRD_PARTY_LICENSES.html -c about.toml
	cd python && cargo about generate ../about.hbs -o RUST_THIRD_PARTY_LICENSES.html -c ../about.toml
	cd python && uv sync --all-extras && uv tool run pip-licenses --python .venv/bin/python --format=markdown --with-urls --output-file=PYTHON_THIRD_PARTY_LICENSES.md
	cd nodejs && cargo about generate ../about.hbs -o RUST_THIRD_PARTY_LICENSES.html -c ../about.toml
	cd nodejs && npx license-checker --markdown --out NODEJS_THIRD_PARTY_LICENSES.md
	cd java && ./mvnw license:aggregate-add-third-party -q
