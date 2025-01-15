.PHONY: all clean install test lint

# Default target
all: install lint

create-virtual-env:
	python3 -m venv venv

activate-virtual-env:
	source venv/bin/activate

# Install dependencies
install: activate-virtual-env
	pip install -r requirements.txt
	pip install -e .
	python -m nltk.downloader all

# # Run tests
# test:
# 	pytest tests/ -v

# Run linting
lint:
	flake8 .
	black --check .
	isort --check-only .

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf content/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build package
build: clean install  

run:
	python app.py

# Create distribution
build-image:
	docker build -t maap-data-loader .

run-image:
	docker run -it maap-data-loader

