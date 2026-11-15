@echo off
rem Doble clic (o "curso" en cmd) = prepara todo el laboratorio. Admite los mismos argumentos que curso.ps1.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0curso.ps1" %*
if "%~1"=="" pause
