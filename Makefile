.PHONY: setup test run schedule

setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

test:
	pytest -q

run:
	python -m app.main run --run-date $(DATE)

schedule:
	python -m app.main schedule --run-now
