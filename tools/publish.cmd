@echo off

set nuget=%~dp0\NuGet\NuGet.exe
set package=%1

%nuget% push %package% -src https://www.nuget.org/api/v2/package
