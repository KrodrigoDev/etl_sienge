@echo off
chcp 65001 > nul

cd /d D:\GitHub\etl_sienge

echo ========================================= >> logs\consultas_parcela_execucao.log
echo INICIO %date% %time% >> logs\consultas_parcela_execucao.log
echo ========================================= >> logs\consultas_parcela_execucao.log


D:\GitHub\etl_sienge\.venv\Scripts\python.exe main.py --etapa painel_consultas >> logs\consultas_parcela_execucao.log 2>&1
if %errorlevel% neq 0 (
    echo [AVISO] painel_consultas falhou com codigo %errorlevel% >> logs\consultas_execucao.log
)

echo ========================================= >> logs\consultas_parcela_execucao.log
echo FIM %date% %time% >> logs\consultas_parcela_execucao.log
echo ========================================= >> logs\consultas_parcela_execucao.log

exit /b 0