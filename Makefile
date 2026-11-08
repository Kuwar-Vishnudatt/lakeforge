.PHONY: venv test demo clean

venv:
	python3 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -r requirements.txt

test:
	PYTHONPATH=. .venv/bin/python -m pytest tests/ -v

demo:
	PYTHONPATH=. .venv/bin/python scripts/demo.py

clean:
	rm -rf .venv .pytest_cache **/__pycache__
