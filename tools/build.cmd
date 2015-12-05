@echo off

if "%KEYFILE%" == "" (
    echo Error: The key file is not specified. Use
    echo.
    echo SET KEYFILE=path-to-keyfile
    echo.
    goto :EOF
)

set src=%~dp0\..\src

pushd %src%\AzureQueueAgentLib

msbuild /p:Configuration=Release;Platform=AnyCPU /m AzureQueueAgentLib.csproj /t:Clean,Build

echo Sign the output assembly ...
sn -R bin\Release\Aqua.Lib.dll %KEYFILE%

echo Verify that the output assembly was signed ...
sn -v bin\Release\Aqua.Lib.dll

popd
