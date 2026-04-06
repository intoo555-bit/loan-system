@echo off
echo === Loan System Local Dev ===
set DB_PATH=./data/loan_system.db
set CHANNEL_ACCESS_TOKEN=test_token
set ADMIN_PASSWORD=localadmin
set REPORT_PASSWORD=localreport
set PORT=10000
python main.py
pause
