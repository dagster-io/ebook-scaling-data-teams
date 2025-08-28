ruff:
	ruff check . --select I --fix
	ruff format .

test_unit:
	pytest tests -m "not integration" -v --disable-warnings

test_integration:
	pytest tests -m "integration" -v --disable-warnings

test_all:
	pytest tests -v --disable-warnings