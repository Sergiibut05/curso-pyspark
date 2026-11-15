<#
.SYNOPSIS
  Deja listo todo el laboratorio del curso de PySpark con un solo comando.

.EXAMPLE
  .\curso.ps1                      # prepara todo: Kafka, imagen de Spark, datos en Kafka y abre el curso
  .\curso.ps1 run 09_ventanas.py   # ejecuta una práctica (y deja Kafka listo si no lo estaba)
  .\curso.ps1 down                 # para el laboratorio (los datos de Kafka se conservan)
  .\curso.ps1 reset                # lo borra todo (datos de Kafka y salidas) para empezar de cero
#>
param(
    [ValidateSet('up', 'run', 'down', 'reset')][string]$Comando = 'up',
    [Parameter(ValueFromRemainingArguments)][string[]]$Args2
)

$ErrorActionPreference = 'Stop'
$raiz = $PSScriptRoot
$lab = Join-Path $raiz 'laboratorio'

function Paso($texto) { Write-Host "`n==> $texto" -ForegroundColor Cyan }
function Compose { & docker compose @args; if ($LASTEXITCODE -ne 0) { throw "docker compose $($args -join ' ') falló (código $LASTEXITCODE)" } }

function Comprobar-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'No encuentro Docker. Instala Docker Desktop (https://www.docker.com/products/docker-desktop) y vuelve a ejecutar.'
    }
    cmd /c 'docker info >nul 2>&1'   # cmd evita que PowerShell 5.1 convierta el stderr de docker en una excepción
    if ($LASTEXITCODE -ne 0) { throw 'Docker está instalado pero no está en marcha. Abre Docker Desktop, espera a que arranque y vuelve a ejecutar.' }
}

function Preparar {
    Paso 'Arrancando Kafka (espera a que esté sano)'
    Compose up -d --wait kafka
    Paso 'Construyendo la imagen de las prácticas (la primera vez tarda unos minutos: PySpark pesa 450 MB)'
    Compose --profile herramientas build spark
    Paso 'Cargando los terremotos en Kafka (si ya están cargados, no hace nada)'
    Compose run --rm spark python 00_cargar_kafka.py
}

Push-Location $lab
try {
    Comprobar-Docker
    switch ($Comando) {
        'up' {
            Preparar
            Paso 'Todo listo'
            Write-Host "Abriendo el curso en el navegador..."
            Start-Process (Join-Path $raiz 'index.html')
            Write-Host @'

Ejecuta cualquier práctica con:
    .\curso.ps1 run 01_hola_spark.py
Para parar el laboratorio:  .\curso.ps1 down
Para empezar de cero:       .\curso.ps1 reset
'@
        }
        'run' {
            if (-not $Args2) { throw 'Indica la práctica: .\curso.ps1 run 01_hola_spark.py' }
            Preparar
            Paso "Ejecutando $($Args2 -join ' ')"
            Compose run --rm spark python @Args2
        }
        'down' { Compose --profile herramientas --profile cluster down }
        'reset' {
            Compose --profile herramientas --profile cluster down -v
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue `
                (Join-Path $raiz 'practicas\_salida'), (Join-Path $raiz 'practicas\_checkpoint-*')
            Write-Host 'Laboratorio borrado. Ejecuta .\curso.ps1 para prepararlo de nuevo.'
        }
    }
}
finally { Pop-Location }
