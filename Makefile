.PHONY: help build up down restart logs shell mysql schema data etl clean clean-all test

GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

help:
	@echo $(GREEN)CreditFlow3C0 - Makefile Commands$(NC)
	@echo.
	@echo $(YELLOW)Container Management:$(NC)
	@echo   make build      - Build Docker images
	@echo   make up         - Start all containers (detached mode)
	@echo   make down       - Stop and remove containers
	@echo   make restart    - Restart all containers
	@echo   make logs       - View container logs
	@echo   make status     - Show container status
	@echo.
	@echo $(YELLOW)Database Commands:$(NC)
	@echo   make mysql      - Open MySQL shell in container
	@echo   make schema     - Create database schema
	@echo   make reset-db   - Reset database (drop and recreate)
	@echo   make backup-db  - Backup database to file
	@echo   make restore-db - Restore database from backup
	@echo.
	@echo $(YELLOW)Data & ETL Commands:$(NC)
	@echo   make data       - Generate synthetic data
	@echo   make etl        - Run ETL pipeline
	@echo   make full-run   - Complete run (schema + data + etl)
	@echo.
	@echo $(YELLOW)Analytics Commands:$(NC)
	@echo   make reports    - Generate all analytics reports
	@echo   make executive  - Generate executive dashboard only
	@echo   make credit     - Generate credit risk report only
	@echo   make fraud      - Generate fraud report only
	@echo   make regulatory - Generate regulatory report only
	@echo.
	@echo $(YELLOW)Clean Commands:$(NC)
	@echo   make clean      - Remove generated data, reports, and JSON files
	@echo   make clean-all  - Remove everything (including Docker volumes)
	@echo   make fresh      - Complete fresh start (clean-all + build + full-run)

build:
	@echo $(GREEN)Building Docker images...$(NC)
	docker-compose build

up:
	@echo $(GREEN)Starting containers...$(NC)
	docker-compose up -d
	@echo $(GREEN)Containers started. Use 'make logs' to view logs$(NC)

down:
	@echo $(YELLOW)Stopping containers...$(NC)
	docker-compose down

restart: down up

logs:
	docker-compose logs -f

status:
	docker-compose ps

shell:
	docker-compose exec app bash

mysql:
	docker-compose exec mysql mysql -u root -p${MYSQL_ROOT_PASSWORD} creditflow360

schema:
	@echo $(GREEN)Creating database schema...$(NC)
	docker-compose exec app python scripts/create_schema.py

reset-db:
	@echo $(RED)WARNING: This will drop and recreate the database!$(NC)
	@echo $(YELLOW)Resetting database...$(NC)
	docker-compose exec mysql mysql -u root -p${MYSQL_ROOT_PASSWORD} -e "DROP DATABASE IF EXISTS creditflow360; CREATE DATABASE creditflow360 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
	@echo $(GREEN)Database reset complete! Run 'make schema' to recreate schema$(NC)

backup-db:
	@echo $(GREEN)Backing up database...$(NC)
	docker-compose exec mysql mysqldump -u root -p${MYSQL_ROOT_PASSWORD} creditflow360 > backup_$(shell powershell -Command "Get-Date -Format 'yyyyMMdd_HHmmss'").sql
	@echo $(GREEN)Backup complete$(NC)

restore-db:
	@echo $(RED)WARNING: This will overwrite the current database!$(NC)
	@set /p filename="Enter backup filename: " && docker-compose exec -T mysql mysql -u root -p${MYSQL_ROOT_PASSWORD} creditflow360 < %filename% && echo $(GREEN)Restore complete$(NC) || echo $(RED)File not found: %filename%$(NC)

data:
	@echo $(GREEN)Generating synthetic data...$(NC)
	docker-compose exec app python scripts/run_data_generation.py

etl:
	@echo $(GREEN)Running ETL pipeline...$(NC)
	docker-compose exec app python scripts/run_etl_pipeline.py

full-run: schema data etl
	@echo $(GREEN)Full pipeline completed!$(NC)

reports:
	@echo $(GREEN)Generating all analytics reports...$(NC)
	docker-compose exec app python scripts/run_analytics.py

executive:
	@echo $(GREEN)Generating executive dashboard...$(NC)
	docker-compose exec app python -c "from src.analytics.executive_dashboard import ExecutiveCommandCenter; ExecutiveCommandCenter().generate_executive_dashboard()"

credit:
	@echo $(GREEN)Generating credit risk report...$(NC)
	docker-compose exec app python -c "from src.analytics.credit_risk_monitor import CreditRiskMonitor; CreditRiskMonitor().generate_credit_risk_report()"

fraud:
	@echo $(GREEN)Generating fraud report...$(NC)
	docker-compose exec app python -c "from src.analytics.fraud_detection_center import FraudDetectionCenter; FraudDetectionCenter().generate_fraud_report()"

regulatory:
	@echo $(GREEN)Generating regulatory report...$(NC)
	docker-compose exec app python -c "from src.analytics.regulatory_reporting import RegulatoryReporting; RegulatoryReporting().generate_regulatory_report()"

clean:
	@echo $(YELLOW)Cleaning generated data and reports...$(NC)
	@if exist data\raw_csv\* del /q /s data\raw_csv\* 2>nul || echo No files in raw_csv
	@if exist data\processed\* del /q /s data\processed\* 2>nul || echo No files in processed
	@if exist data\exports\* del /q /s data\exports\* 2>nul || echo No files in exports
	@if exist data\*.json del /q /s data\*.json 2>nul || echo No JSON files in data
	@if exist data\*.csv del /q /s data\*.csv 2>nul || echo No CSV files in data root
	@if exist reports\* rmdir /s /q reports 2>nul
	@if exist logs\* del /q /s logs\* 2>nul || echo No files in logs
	@if exist analytics\reports\* rmdir /s /q analytics\reports 2>nul
	@mkdir reports 2>nul
	@mkdir reports\executive reports\credit_risk reports\fraud reports\regulatory 2>nul
	@echo $(GREEN)Clean complete!$(NC)

clean-all: down
	@echo $(RED)Removing all containers, volumes, and generated data...$(NC)
	@set /p confirm="This will delete ALL data. Are you sure? (y/N): "
	@if /i "%confirm%"=="y" ( \
		echo $(YELLOW)Cleaning...$(NC) && \
		docker-compose down -v && \
		if exist data\raw_csv\* del /q /s data\raw_csv\* 2>nul && \
		if exist data\processed\* del /q /s data\processed\* 2>nul && \
		if exist data\exports\* del /q /s data\exports\* 2>nul && \
		if exist data\*.json del /q /s data\*.json 2>nul && \
		if exist data\*.csv del /q /s data\*.csv 2>nul && \
		if exist reports\* rmdir /s /q reports 2>nul && \
		if exist logs\* del /q /s logs\* 2>nul && \
		if exist analytics\reports\* rmdir /s /q analytics\reports 2>nul && \
		mkdir reports 2>nul && \
		mkdir reports\executive reports\credit_risk reports\fraud reports\regulatory 2>nul && \
		echo $(GREEN)Clean all complete!$(NC) \
	) else ( \
		echo $(YELLOW)Cancelled$(NC) \
	)

fresh: clean-all build up schema data etl reports
	@echo $(GREEN)Fresh start completed!$(NC)
	@echo $(GREEN)Access Adminer at http://localhost:8081$(NC)

test:
	@echo $(GREEN)Running tests...$(NC)
	docker-compose exec app pytest tests/

test-connection:
	@echo $(GREEN)Testing database connection...$(NC)
	docker-compose exec app python -c "from src.database.db_connection import test_database_connection; test_database_connection()"

dev-setup: build up schema
	@echo $(GREEN)Development environment ready!$(NC)

init:
	@echo $(GREEN)Creating project directories...$(NC)
	@if not exist data\raw_csv mkdir data\raw_csv
	@if not exist data\processed mkdir data\processed
	@if not exist data\exports mkdir data\exports
	@if not exist reports mkdir reports
	@if not exist reports\executive mkdir reports\executive
	@if not exist reports\credit_risk mkdir reports\credit_risk
	@if not exist reports\fraud mkdir reports\fraud
	@if not exist reports\regulatory mkdir reports\regulatory
	@if not exist logs mkdir logs
	@if not exist config mkdir config
	@echo $(GREEN)Directories created$(NC)