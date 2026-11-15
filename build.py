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
CAMPOS = ("titulo", "resumen", "bloque", "minutos")


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
    return avisos


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
    minutos = sum(lec["minutos"] for lec in lecciones)
    print(f"{len(lecciones)} lecciones, {minutos} min, {len(avisos)} avisos -> {DATOS.relative_to(RAIZ)}")
    return 1 if avisos else 0


if __name__ == "__main__":
    raise SystemExit(main())
