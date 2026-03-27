@echo off 
chcp 65001 >nul 
cd /d D:\oracle-sql-tuning 
echo ============================================ 
echo   SQL Tuning Full Pipeline 
echo   Phase 1-2-3-4-10053-Excel 
echo ============================================ 
echo. 
python main.py run --db-password oracle 
echo. 
echo ============================================ 
echo   Complete! Check output\reports\ 
echo ============================================ 
pause
