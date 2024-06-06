dev-install:
	python3.12 -m venv .venv && \
	source .venv/bin/activate && \
	pip install poetry keyring keyrings-google-artifactregistry-auth && \
	poetry install --no-root && \
	source .venv/bin/activate
