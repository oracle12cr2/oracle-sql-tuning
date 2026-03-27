@echo off 
chcp 65001 >nul 
cd /d D:\oracle-sql-tuning 
echo ============================================ 
echo   SQL Tuning Report - Refresh 
echo ============================================ 
echo. 
python main.py run --skip-detect --db-password oracle 
echo. 
echo ============================================ 
echo   Complete! Check output\reports\ 
echo ============================================ 
pause
