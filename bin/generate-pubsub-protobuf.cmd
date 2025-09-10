@echo off

REM Prerequisites:
REM 1. Follow the instructions at https://github.com/elixir-protobuf/protobuf to install and allow usage of the tool
REM 1.A. Install protoc and ensure it's available on your PATH
REM 1.B. Install the protobuf escript: mix escript.install hex protobuf
REM      (This installs the protoc-gen-elixir plugin that generates Elixir code from .proto files)
REM 2. Ensure the googleapis repository is cloned into the tmp/googleapis directory.
REM      (This repository contains the .proto files for Google APIs, including PubSub.)
REM
REM Purpose:
REM This script generates Elixir code from Google PubSub .proto files using protoc and the protoc-gen-elixir plugin.
REM The generated files are placed in the lib/proto directory and are intended to be checked into git.
REM It cleans the output directory before generating new files to ensure no stale files remain.
REM
REM Steps:
REM 1. Verify that the required tools (protoc and protoc-gen-elixir) are available.
REM 2. Verify that the googleapis directory exists.
REM 3. Run the script to generate the Elixir files from the .proto definitions.

setlocal

echo Generating protobuf files using system PATH...

cd /d "%~dp0.."

set TMP_DIR=tmp
set OUT=lib\proto
set GOOGLEAPIS_PATH=%TMP_DIR%\googleapis
set PLUGIN_PATH=%USERPROFILE%\.mix\escripts\protoc-gen-elixir.bat

if not exist "%GOOGLEAPIS_PATH%" (
    echo Error: googleapis not found. Run update-proto.cmd first to download googleapis.
    pause
    exit /b 1
)

echo Checking if tools are available...
where protoc
echo Plugin path: %PLUGIN_PATH%
if exist "%PLUGIN_PATH%" (
    echo Plugin exists
) else (
    echo Plugin NOT found at %PLUGIN_PATH%
    pause
    exit /b 1
)

echo.
echo Cleaning output directory...
if exist "%OUT%" (
    rmdir /s /q "%OUT%"
)
mkdir "%OUT%"

echo.
echo Generating Google PubSub protobuf files...

protoc -I "%GOOGLEAPIS_PATH%" ^
  --plugin=protoc-gen-elixir="%PLUGIN_PATH%" ^
  --elixir_out=plugins=grpc:"%OUT%" ^
  "%GOOGLEAPIS_PATH%\google\pubsub\v1\pubsub.proto" ^
  "%GOOGLEAPIS_PATH%\google\pubsub\v1\schema.proto" ^
  "%GOOGLEAPIS_PATH%\google\api\annotations.proto" ^
  "%GOOGLEAPIS_PATH%\google\api\http.proto"

echo.
echo Checking results...
if exist "%OUT%\google\pubsub\v1\pubsub.pb.ex" (
    echo SUCCESS: Generated PubSub protobuf files
    echo.
    echo Generated files:
    dir /s "%OUT%\*.ex"
    echo.
    echo File sizes:
    for /r "%OUT%" %%f in (*.ex) do echo %%f: %%~zf bytes
    echo.
    echo Now try: mix compile
) else (
    echo FAILED: Could not generate PubSub files
    echo Checking if any files were created:
    dir "%OUT%" /s
)

pause
