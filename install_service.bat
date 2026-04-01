cd /d "%~dp0"
@echo off
chcp 1251 >nul
echo ============================================
echo   S3 Folder Watcher — Установка
echo   Timeweb Cloud S3
echo ============================================
echo.

:: Проверка прав администратора
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [!] Запустите этот скрипт от имени Администратора!
    echo     Правый клик > Запуск от имени администратора
    pause
    exit /b 1
)

:: Проверка Python
python --version >nul 2>&1
if %errorLevel% neq 0 (
    echo [!] Python не найден. Установите Python 3.9+ с python.org
    echo     При установке отметьте "Add Python to PATH"
    pause
    exit /b 1
)

echo [1/3] Установка зависимостей...
pip install -r requirements.txt
if %errorLevel% neq 0 (
    echo [!] Ошибка установки зависимостей
    pause
    exit /b 1
)

echo.
echo [2/3] Проверка конфигурации...
if not exist config.json (
    echo [!] Файл config.json не найден — будет создан шаблон.
    echo     Отредактируйте config.json и запустите установку повторно.
    python s3_folder_watcher.py
    pause
    exit /b 0
)

echo.
echo [3/3] Установка Windows-службы...
python s3_folder_watcher.py install
if %errorLevel% neq 0 (
    echo [!] Ошибка установки службы
    pause
    exit /b 1
)

echo.
echo ============================================
echo   Установка завершена!
echo.
echo   Запуск службы:   net start S3FolderWatcher
echo   Остановка:       net stop S3FolderWatcher
echo   Удаление:        python s3_folder_watcher.py remove
echo.
echo   Логи: s3_watcher.log
echo ============================================
echo.

set /p START="Запустить службу сейчас? (y/n): "
if /i "%START%"=="y" (
    net start S3FolderWatcher
    echo Служба запущена!
)

pause
