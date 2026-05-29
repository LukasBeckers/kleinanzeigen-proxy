.PHONY: test install-dev

install-dev:
	pip install -r requirements-dev.txt

# Runs inside the running proxy container.  ``docker compose up -d`` first.
test:
	@docker cp tests/. kleinanzeigen-proxy-kleinanzeigen-proxy-1:/app/tests/ >/dev/null
	@docker cp pytest.ini kleinanzeigen-proxy-kleinanzeigen-proxy-1:/app/pytest.ini >/dev/null
	@docker cp requirements-dev.txt kleinanzeigen-proxy-kleinanzeigen-proxy-1:/app/requirements-dev.txt >/dev/null
	@docker compose exec -T kleinanzeigen-proxy bash -c "pip install -q -r /app/requirements-dev.txt >/dev/null 2>&1 && cd /app && python -m pytest tests/"
