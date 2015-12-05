@echo off

set nuget=%~dp0\NuGet\NuGet.exe
set src=%~dp0\..\src
set config=Release
set props=OutputPath=bin\%config%;Configuration=%config%

rem This is a read-only operation (so no harm), but it can be used to download the latest version through the bootstrapper.
%nuget% config
%nuget% pack %src%\AzureQueueAgentLib\AzureQueueAgentLib.csproj -Prop %props% -Symbols -IncludeReferencedProjects
