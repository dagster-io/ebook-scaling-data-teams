ruff:
	ruff check . --select I --fix
	ruff format .

test_unit:
	pytest tests -m "not integration"

test_integration:
	pytest tests -m "integration"

test_all:
	pytest tests