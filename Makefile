.PHONY: help setup clean test lint run build docker-build docker-run logs backup install-dev

# Default target
help:
	@echo "Available commands:"
	@echo "make setup         - Create virtual environment and install dependencies"
	@echo "make install-dev   - Install development dependencies"
	@echo "make clean        - Remove Python build artifacts and virtual environment"
	@echo "make test         - Run unit tests"
	@echo "make lint         - Run code linting (flake8) and formatting (black)"
	@echo "make run          - Run the application"
	@echo "make build        - Build the Python package"
	@echo "make docker-build - Build Docker image"
	@echo "make docker-run   - Run Docker container"
	@echo "make logs         - View application logs"
	@echo "make backup       - Backup processed data"

# Setup virtual environment and install dependencies
setup:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

# Install development dependencies
install-dev:
	. venv/bin/activate && pip install -r requirements-dev.txt

# Clean Python build artifacts and virtual environment
clean:
	rm -rf venv/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".tox" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +

# Run unit tests
test:
	. venv/bin/activate && python -m pytest tests/ -v --cov=. --cov-report=term-missing

# Run linting and formatting
lint:
	. venv/bin/activate && flake8 .
	. venv/bin/activate && black .
	. venv/bin/activate && isort .

# Run the application
run:
	. venv/bin/activate && python app.py

# Build Python package
build:
	. venv/bin/activate && python setup.py build

# Docker commands
docker-build:
	docker build -t maap-data-loader .

docker-run:
	docker run -d --env-file .env --name maap-data-loader maap-data-loader

# View logs
logs:
	@if [ -f local/logs/MAAP-Loader.log ]; then \
		tail -f local/logs/MAAP-Loader.log; \
	else \
		echo "Log file not found"; \
	fi

# Backup data
backup:
	@timestamp=$$(date +%Y%m%d_%H%M%S); \
	mkdir -p backups/$$timestamp; \
	cp -r local/uploaded_files/* backups/$$timestamp/ 2>/dev/null || true; \
	echo "Backup created in backups/$$timestamp"
