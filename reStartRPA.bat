@echo off
setlocal

REM ===== 檢查參數 =====
if "%~1"=="" (
    echo [錯誤] 請輸入來源資料夾路徑
    echo 範例: restart_and_copy.bat "C:\來源" "D:\目標"
    exit /b 1
)

if "%~2"=="" (
    echo [錯誤] 請輸入目標資料夾路徑
    echo 範例: restart_and_copy.bat "C:\來源" "D:\目標"
    exit /b 1
)

REM ===== 設定來源與目標 =====
set SRC=%~1
set DEST=%~2

REM ===== 設定 Robotiive 路徑 =====
set APP_PATH=C:\Program Files\Robotiive\Robotiive.exe

REM ===== 關閉 Robotiive =====
echo 關閉 Robotiive.exe...
taskkill /IM Robotiive.exe /F >nul 2>&1

REM ===== 等待 5 秒 =====
timeout /t 5 /nobreak >nul

REM ===== 複製資料夾 =====
echo 複製資料夾中： %SRC% → %DEST% ...
robocopy "%SRC%" "%DEST%" /MIR

REM ===== 重啟 Robotiive =====
echo 啟動 Robotiive.exe...
start "" "%APP_PATH%"

pause
endlocal
exit
