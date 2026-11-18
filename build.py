"""Genera los datos del curso a partir de contenido/*.md.

Cada lección es un Markdown con cabecera:

    ---
    titulo: Topics, particiones y offsets
    resumen: Una frase que sale en el temario.
    bloque: Fundamentos
    minutos: 20
    practica: si        (opcional: «si» o un texto propio para la etiqueta de práctica)
    ---

Salida (para poder abrir las páginas con doble clic, sin servidor):
    assets/data/indice.js   -> window.CURSO y window.INDICE
    assets/data/<slug>.js   -> window.LECCION = {slug, markdown}
    cuadernos/NN-<slug>.ipynb -> la lección como cuaderno de Jupyter (solo las que tienen código ejecutable)

Bloques de Python: por defecto son **ejecutables** (botón «Ejecutar» en la web, celda de código en el
cuaderno) y se ejecutan en orden en una misma sesión, como las celdas de un cuaderno. Marcas en la valla:
    ```python preparacion   lo que la lección da por hecho (imports, sesión, DataFrames de partida); en la
                            web sale plegado y se ejecuta solo antes del primer bloque
    ```python fragmento     ilustrativo: no se ejecuta (sintaxis con «…», extractos de dentro de una función)
Los bloques ejecutables de las soluciones (:::solucion) también se ejecutan en la web; en el cuaderno van
plegados como texto, tras una celda vacía para la solución propia.

Uso: python build.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
CONTENIDO = RAIZ / "contenido"
DATOS = RAIZ / "assets" / "data"
CUADERNOS = RAIZ / "cuadernos"
CAMPOS = ("titulo", "resumen", "bloque", "minutos")
MARCAS_PYTHON = {"preparacion", "fragmento"}
CAJAS = {"nota": "Nota", "cuidado": "Cuidado", "defensa": "Para la defensa", "repo": "En este repositorio",
         "resumen": "En resumen"}


def _practica(valor: str) -> str:
    """Etiqueta de práctica: «si» usa la del curso (curso.json); otro texto se muestra tal cual."""
    if valor.lower() in {"si", "sí", "true"}:
        return "si"
    return valor


def leer_leccion(ruta: Path) -> dict:
    texto = ruta.read_text(encoding="utf-8")
    m = re.match(r"^---\n(.*?)\n---\n(.*)$", texto.replace("\r\n", "\n"), re.S)
    if not m:
        raise SystemExit(f"{ruta.name}: falta la cabecera --- ... ---")
    meta: dict[str, object] = {}
    for linea in m.group(1).splitlines():
        if not linea.strip():
            continue
        clave, _, valor = linea.partition(":")
        valor = valor.strip()
        if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in "\"'":
            valor = valor[1:-1]
        meta[clave.strip()] = valor
    faltan = [c for c in CAMPOS if not meta.get(c)]
    if faltan:
        raise SystemExit(f"{ruta.name}: faltan campos {faltan}")
    numero, _, slug = ruta.stem.partition("-")
    return {
        "numero": int(numero),
        "slug": slug,
        "titulo": meta["titulo"],
        "resumen": meta["resumen"],
        "bloque": meta["bloque"],
        "minutos": int(str(meta["minutos"])),
        "practica": _practica(str(meta.get("practica", ""))),
        "markdown": m.group(2).strip() + "\n",
    }


def comprobar(lecciones: list[dict]) -> list[str]:
    avisos = []
    for lec in lecciones:
        md = lec["markdown"]
        vallas = len(re.findall(r"^\s*```", md, re.M))
        if vallas % 2:
            avisos.append(f"{lec['slug']}: número impar de vallas ```")
        abiertos = len(re.findall(r"^:::(nota|cuidado|defensa|repo|resumen|ejercicio)\b", md, re.M))
        cerrados = len(re.findall(r"^:::\s*$", md, re.M))
        if abiertos != cerrados:
            avisos.append(f"{lec['slug']}: {abiertos} bloques ::: abiertos y {cerrados} cerrados")
        for quiz in re.findall(r"```quiz\n(.*?)```", md, re.S):
            if len(re.findall(r"^\* ", quiz, re.M)) != 1:
                avisos.append(f"{lec['slug']}: un quiz no tiene exactamente una respuesta correcta")
        preparaciones = 0
        for info, _ in bloques(md):
            lengua, marcas, _ = analizar_info(info)
            if lengua in ("python", "pyspark"):
                for marca in marcas - MARCAS_PYTHON:
                    avisos.append(f"{lec['slug']}: marca desconocida «{marca}» en un bloque de Python")
                preparaciones += "preparacion" in marcas
        if preparaciones > 1:
            avisos.append(f"{lec['slug']}: más de un bloque «python preparacion»")
    return avisos


# ── Cuadernos de Jupyter ────────────────────────────────────────────────


def bloques(md: str):
    """(info, código) de cada bloque de código vallado, en orden."""
    for m in re.finditer(r"^[ \t]*```([^\n`]*)\n(.*?)^[ \t]*```[ \t]*$", md, re.M | re.S):
        yield m.group(1).strip(), m.group(2)


def analizar_info(info: str) -> tuple[str, set[str], str]:
    """«python preparacion titulo="x.py"» -> ("python", {"preparacion"}, "x.py"), como analizarInfo de comun.js."""
    t = re.search(r'titulo="([^"]*)"', info)
    resto = re.sub(r'titulo="[^"]*"', "", info).split()
    return (resto[0] if resto else ""), set(resto[1:]), (t.group(1) if t else "")


def ejecutable(info: str) -> bool:
    lengua, marcas, _ = analizar_info(info)
    return lengua in ("python", "pyspark") and "fragmento" not in marcas


def _celda(tipo: str, texto: str, ident: str) -> dict:
    lineas = texto.strip("\n").split("\n")
    fuente = [l + "\n" for l in lineas[:-1]] + [lineas[-1]]
    celda = {"cell_type": tipo, "id": ident, "metadata": {}, "source": fuente}
    if tipo == "code":
        celda.update(execution_count=None, outputs=[])
    return celda


def _quiz(texto: str) -> list[str]:
    pregunta, opciones, explicacion = [], [], []
    for l in (l for l in texto.split("\n") if l.strip()):
        if re.match(r"^[-*] ", l):
            opciones.append((l[2:].strip(), l[0] == "*"))
        elif re.match(r"^> ?", l):
            explicacion.append(re.sub(r"^> ?", "", l))
        elif not opciones:
            pregunta.append(l)
        else:
            explicacion.append(l)
    letras = "ABCDEFG"
    correcta = next((f"**{letras[i]})** {o}" for i, (o, ok) in enumerate(opciones) if ok), "")
    return ["", f"**Comprueba.** {' '.join(pregunta)}", "",
            *[f"- **{letras[i]})** {o}" for i, (o, _) in enumerate(opciones)], "",
            "<details><summary>Ver la respuesta</summary>", "", correcta, "", " ".join(explicacion), "",
            "</details>", ""]


def celdas_cuaderno(lec: dict, soluciones: bool = False) -> list[dict]:
    """La lección como celdas: el texto en Markdown y cada bloque ejecutable en una celda de código.

    Las cajas (:::nota…) pasan a citas, los ejercicios a un título con una celda vacía para la solución propia
    y la solución plegada; las salidas de referencia, plegadas. Con soluciones=True, el código de las
    soluciones también va en celdas de código (para comprobar que todo se ejecuta).
    """
    celdas: list[dict] = []
    texto: list[str] = []
    pila: list[str] = []          # cajas abiertas: nota, cuidado…, ejercicio, detalle (solución o pista)
    hueco_puesto = False          # ya hay una celda vacía para la solución del ejercicio abierto

    def cita() -> str:
        return "> " if any(t in CAJAS for t in pila) else ""

    def poner(*lineas: str) -> None:
        for l in lineas:
            texto.append((cita() + l).rstrip() if cita() else l)

    def volcar() -> None:
        if "".join(texto).strip():
            celdas.append(_celda("markdown", "\n".join(texto), f"{lec['slug']}-{len(celdas)}"))
        texto.clear()

    def codigo(fuente: str) -> None:
        volcar()
        celdas.append(_celda("code", fuente, f"{lec['slug']}-{len(celdas)}"))

    lineas = lec["markdown"].split("\n")
    i = 0
    while i < len(lineas):
        linea = lineas[i]
        valla = re.match(r"^\s*(`{3,}|~{3,})(.*)$", linea)
        if valla:
            j = i + 1
            while j < len(lineas) and not (lineas[j].strip().startswith(valla.group(1))
                                           and lineas[j].strip().strip("`~") == ""):
                j += 1
            info, cuerpo = valla.group(2).strip(), "\n".join(lineas[i + 1 : j])
            lengua, marcas, titulo = analizar_info(info)
            en_solucion = "detalle" in pila
            en_caja = any(t in CAJAS for t in pila)
            if ejecutable(info) and not en_caja and (soluciones or not en_solucion):
                codigo(cuerpo)
            elif lengua == "salida":
                poner("", f"<details><summary>{titulo or 'Salida de referencia'}</summary>", "", "```text",
                      *cuerpo.split("\n"), "```", "", "</details>", "")
            elif lengua == "quiz":
                poner(*_quiz(cuerpo))
            elif lengua == "widget":
                nombre = cuerpo.split()[0] if cuerpo.split() else ""
                poner("", f"*Aquí la lección web tiene un simulador interactivo ({nombre}).*", "")
            else:
                if titulo:
                    poner(f"*{titulo}*", "")
                poner("```" + ("python" if lengua == "pyspark" else lengua), *cuerpo.split("\n"), "```")
            i = j + 1
            continue
        abre = re.match(r"^:::(nota|cuidado|defensa|repo|resumen|ejercicio)\s*(.*)$", linea)
        sol = re.match(r"^:::(solucion|pista)\s*(.*)$", linea)
        if abre:
            tipo, titulo = abre.group(1), abre.group(2).strip()
            pila.append(tipo)
            if tipo == "ejercicio":
                hueco_puesto = False
                poner("", f"### Práctica: {titulo or 'Ejercicio'}", "")
            else:
                poner(f"**{titulo or CAJAS[tipo]}**", "")
        elif sol:
            tipo, titulo = sol.group(1), sol.group(2).strip()
            if pila and pila[-1] == "detalle":
                pila.pop()
                poner("", "</details>", "")
            if tipo == "solucion" and "ejercicio" in pila and not hueco_puesto and not soluciones:
                codigo("# Tu solución\n")
                hueco_puesto = True
            pila.append("detalle")
            resumen = titulo or ("Ver una pista" if tipo == "pista" else "Ver la solución")
            poner("", f"<details><summary>{resumen}</summary>", "")
        elif re.match(r"^:::\s*$", linea):
            tipo = pila.pop() if pila else None
            if tipo == "detalle":
                poner("", "</details>", "")
                tipo = pila.pop() if pila else None
            if tipo in CAJAS:
                texto.append("")
        else:
            poner(linea)
        i += 1
    volcar()
    return celdas


def cuaderno(lec: dict, curso: dict, soluciones: bool = False) -> dict | None:
    """El cuaderno de una lección, o None si no tiene ningún bloque ejecutable."""
    cuerpo = celdas_cuaderno(lec, soluciones)
    if not any(c["cell_type"] == "code" and c["source"] != ["# Tu solución"] for c in cuerpo):
        return None
    cabecera = _celda("markdown", "\n".join([
        f"# {lec['numero']:02d} · {lec['titulo']}",
        "",
        f"*{lec['resumen']}*",
        "",
        f"> Cuaderno generado a partir de la lección {lec['numero']} de «{curso.get('titulo', 'el curso')}». "
        "Ejecuta las celdas **en orden** con **Mayús + Intro**: la primera celda de código prepara la sesión de "
        "Spark. Las salidas plegadas son las de referencia, para comparar. Los simuladores y las preguntas "
        "interactivas están en la versión web.",
    ]), f"{lec['slug']}-cabecera")
    return {
        "cells": [cabecera, *cuerpo],
        "metadata": {
            "kernelspec": {"display_name": "Python 3 (ipykernel)", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def nombre_cuaderno(lec: dict) -> str:
    return f"{lec['numero']:02d}-{lec['slug']}.ipynb"


def main() -> int:
    curso = json.loads((CONTENIDO / "curso.json").read_text(encoding="utf-8"))
    lecciones = [leer_leccion(r) for r in sorted(CONTENIDO.glob("[0-9][0-9]-*.md"))]
    if not lecciones:
        raise SystemExit("No hay lecciones en contenido/")
    avisos = comprobar(lecciones)
    for a in avisos:
        print(f"AVISO {a}", file=sys.stderr)

    DATOS.mkdir(parents=True, exist_ok=True)
    for viejo in DATOS.glob("*.js"):
        viejo.unlink()
    for lec in lecciones:   # nombre del cuaderno de la lección («» si no tiene): lo enlaza la web
        lec["cuaderno"] = nombre_cuaderno(lec) if cuaderno(lec, curso) else ""
    indice = [{k: v for k, v in lec.items() if k != "markdown"} for lec in lecciones]
    (DATOS / "indice.js").write_text(
        "// Generado por build.py; no editar a mano.\n"
        f"window.CURSO = {json.dumps(curso, ensure_ascii=False)};\n"
        f"window.INDICE = {json.dumps(indice, ensure_ascii=False, indent=1)};\n",
        encoding="utf-8",
    )
    for lec in lecciones:
        datos = {"slug": lec["slug"], "markdown": lec["markdown"]}
        (DATOS / f"{lec['slug']}.js").write_text(
            "// Generado por build.py; no editar a mano.\n"
            f"window.LECCION = {json.dumps(datos, ensure_ascii=False)};\n",
            encoding="utf-8",
        )
    CUADERNOS.mkdir(exist_ok=True)
    for viejo in CUADERNOS.glob("*.ipynb"):
        viejo.unlink()
    n_cuadernos = 0
    for lec in lecciones:
        nb = cuaderno(lec, curso)
        if nb:
            (CUADERNOS / nombre_cuaderno(lec)).write_text(
                json.dumps(nb, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
            n_cuadernos += 1
    minutos = sum(lec["minutos"] for lec in lecciones)
    print(f"{len(lecciones)} lecciones, {minutos} min, {len(avisos)} avisos -> {DATOS.relative_to(RAIZ)}")
    print(f"{n_cuadernos} cuadernos -> {CUADERNOS.relative_to(RAIZ)}")
    return 1 if avisos else 0


if __name__ == "__main__":
    raise SystemExit(main())
