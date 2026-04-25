@echo off
chcp 65001 > nul

cd /d D:\GitHub\etl_sienge

echo ========================================= >> logs\suprimentos_execucao.log
echo INICIO %date% %time% >> logs\suprimentos_execucao.log
echo ========================================= >> logs\suprimentos_execucao.log

D:\GitHub\etl_sienge\.venv\Scripts\python.exe main.py --etapa painel_suprimentos >> logs\suprimentos_execucao.log 2>&1
if %errorlevel% neq 0 (
    echo [AVISO] painel_suprimentos falhou com codigo %errorlevel% >> logs\suprimentos_execucao.log
)

echo ========================================= >> logs\suprimentos_execucao.log
echo FIM %date% %time% >> logs\suprimentos_execucao.log
echo ========================================= >> logs\suprimentos_execucao.log

exit /b 0