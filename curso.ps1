<#
.SYNOPSIS
  Deja listo todo el laboratorio del curso de PySpark con un solo comando.

.EXAMPLE
  .\curso.ps1                      # prepara todo: Kafka, Jupyter, imagen de Spark, datos en Kafka y abre el curso
  .\curso.ps1 run 09_ventanas.py   # ejecuta una práctica (y deja Kafka listo si no lo estaba)
  .\curso.ps1 cuadernos            # abre JupyterLab con los cuadernos de las lecciones
  .\curso.ps1 down                 # para el laboratorio (los datos de Kafka se conservan)
  .\curso.ps1 reset                # lo borra todo (datos de Kafka y salidas) para empezar de cero
#>
param(
    [ValidateSet('up', 'run', 'cuadernos', 'down', 'reset')][string]$Comando = 'up',
    [Parameter(ValueFromRemainingArguments)][string[]]$Args2
)

$ErrorActionPreference = 'Stop'
$raiz = $PSScriptRoot
$lab = Join-Path $raiz 'laboratorio'
$urlJupyter = 'http://127.0.0.1:38888'

function Paso($texto) { Write-Host "`n==> $texto" -ForegroundColor Cyan }
function Compose { & docker compose @args; if ($LASTEXITCODE -ne 0) { throw "docker compose $($args -join ' ') falló (código $LASTEXITCODE)" } }

function Comprobar-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'No encuentro Docker. Instala Docker Desktop (https://www.docker.com/products/docker-desktop) y vuelve a ejecutar.'
    }
    cmd /c 'docker info >nul 2>&1'   # cmd evita que PowerShell 5.1 convierta el stderr de docker en una excepción
    if ($LASTEXITCODE -ne 0) { throw 'Docker está instalado pero no está en marcha. Abre Docker Desktop, espera a que arranque y vuelve a ejecutar.' }
}

# El token de Jupyter: aleatorio, se crea una vez y se guarda en laboratorio/.env (lo lee docker compose) y en
# assets/js/laboratorio-local.js (lo lee la web para ejecutar los bloques). Los dos están en .gitignore.
function Obtener-Token {
    $ficheroEnv = Join-Path $lab '.env'
    $token = $null
    if (Test-Path $ficheroEnv) {
        foreach ($linea in Get-Content $ficheroEnv) {
            if ($linea -match '^JUPYTER_TOKEN=(\w{32,})$') { $token = $Matches[1]; break }
        }
    }
    if (-not $token) {
        $bytes = New-Object byte[] 24
        [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $token = -join ($bytes | ForEach-Object { $_.ToString('x2') })
        [IO.File]::WriteAllText($ficheroEnv, "JUPYTER_TOKEN=$token`n")   # UTF-8 sin BOM: docker compose no lo quiere
    }
    $js = "// Generado por curso.ps1; no editar ni subir al repositorio.`nwindow.LABORATORIO = { url: '$urlJupyter', token: '$token' };`n"
    [IO.File]::WriteAllText((Join-Path $raiz 'assets\js\laboratorio-local.js'), $js)
    # La carpeta de tus cuadernos se crea aquí: si la creara Docker al montarla, podría quedar sin permiso de escritura
    New-Item -ItemType Directory -Force (Join-Path $raiz 'mis-cuadernos') | Out-Null
    return $token
}

function Preparar {
    $script:token = Obtener-Token
    Paso 'Construyendo la imagen de las prácticas (la primera vez tarda unos minutos: PySpark pesa 450 MB)'
    Compose --profile herramientas build spark
    Paso 'Arrancando Kafka y Jupyter (espera a que estén sanos)'
    Compose up -d --wait kafka jupyter
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

Los bloques de Python de las lecciones tienen un botón «Ejecutar»: corren en el laboratorio (Jupyter en Docker).
Si tenías una lección abierta antes de arrancar, recárgala.

Abrir las lecciones como cuadernos de JupyterLab:  .\curso.ps1 cuadernos
Ejecutar una práctica completa:                   .\curso.ps1 run 01_hola_spark.py
Parar el laboratorio:                             .\curso.ps1 down
Empezar de cero:                                  .\curso.ps1 reset
'@
        }
        'run' {
            if (-not $Args2) { throw 'Indica la práctica: .\curso.ps1 run 01_hola_spark.py' }
            Preparar
            Paso "Ejecutando $($Args2 -join ' ')"
            Compose run --rm spark python @Args2
        }
        'cuadernos' {
            $token = Obtener-Token
            Paso 'Arrancando Jupyter'
            Compose up -d --wait kafka jupyter
            Write-Host "Abriendo JupyterLab ($urlJupyter). Tus cuadernos se guardan en la carpeta mis-cuadernos."
            Start-Process "$urlJupyter/lab?token=$token"
        }
        'down' { Compose --profile herramientas --profile cluster down }
        'reset' {
            Compose --profile herramientas --profile cluster down -v
            Remove-Item -Recurse -Force -ErrorAction SilentlyContinue `
                (Join-Path $raiz 'practicas\_salida'), (Join-Path $raiz 'practicas\_checkpoint-*')
            Write-Host 'Laboratorio borrado. Ejecuta .\curso.ps1 para prepararlo de nuevo.'
            Write-Host '(Tus cuadernos de mis-cuadernos se conservan: borra los que quieras recuperar como el original.)'
        }
    }
}
finally { Pop-Location }
