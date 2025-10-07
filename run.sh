#!/bin/bash

# Script to run the Ninja Payments Reconciler service

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Ninja Payments Reconciler...${NC}"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Creating...${NC}"
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Warning: .env file not found. Please copy .env.example to .env and configure it.${NC}"
    exit 1
fi

# Run the application
APP_PORT_ENV=${APP_PORT:-${PORT:-8001}}
echo -e "${GREEN}Starting service on port ${APP_PORT_ENV}...${NC}"
uvicorn src.app:app --host 0.0.0.0 --port "${APP_PORT_ENV}" --log-level info
