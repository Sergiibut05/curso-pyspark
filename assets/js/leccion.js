/*
 * Página de lección: lee ?l=slug, carga assets/data/<slug>.js (funciona también abriendo el
 * fichero con doble clic, sin servidor), pinta la lección, el índice lateral, el índice de la
 * página, la navegación y el botón de «completada».
 */
(function () {
  'use strict';
  const { CURSO, LECCIONES, progreso, escapar, slugificar, renderizar, activarContenido, montarBarra, etiquetaPractica } =
    window.Curso;

  montarBarra();

  const parametros = new URLSearchParams(location.search);
  const slug = parametros.get('l') || (LECCIONES[0] && LECCIONES[0].slug);
  const posicion = LECCIONES.findIndex((l) => l.slug === slug);
  const hoja = document.getElementById('contenido');

  pintarLateral();

  if (posicion < 0) {
    hoja.innerHTML = `<div class="leccion-cabecera"><h1>Lección no encontrada</h1><p class="leccion-resumen">No existe «${escapar(slug || '')}». Vuelve al <a href="index.html">temario</a>.</p></div>`;
    return;
  }

  const script = document.createElement('script');
  script.src = `assets/data/${slug}.js`;
  script.onload = () => pintar(window.LECCION);
  script.onerror = () => {
    hoja.innerHTML =
      '<div class="caja caja-cuidado"><p class="caja-titulo">No se pudo cargar la lección</p><p>Ejecuta <code>python build.py</code> en la carpeta del curso para generar <code>assets/data/</code>.</p></div>';
  };
  document.head.appendChild(script);

  function pintarLateral() {
    const lateral = document.getElementById('lateral-lista');
    if (!lateral) return;
    const hechas = progreso.hechas();
    let bloque = null;
    const partes = [];
    LECCIONES.forEach((l) => {
      if (l.bloque !== bloque) {
        bloque = l.bloque;
        partes.push(`<p class="lateral-grupo">${escapar(bloque)}</p>`);
      }
      const actual = l.slug === slug ? ' aria-current="page"' : '';
      const hecha = hechas.has(l.slug) ? ' hecha' : '';
      partes.push(
        `<a class="lateral-enlace${hecha}" href="leccion.html?l=${encodeURIComponent(l.slug)}"${actual}>` +
          `<span class="lateral-numero"><span>${String(l.numero).padStart(2, '0')}</span></span><span>${escapar(l.titulo)}</span></a>`,
      );
    });
    lateral.innerHTML = partes.join('');
    const detalles = document.getElementById('lateral-detalles');
    if (detalles && matchMedia('(max-width: 58rem)').matches) detalles.open = false;
  }

  function pintar(lec) {
    const meta = LECCIONES[posicion];
    document.title = `${meta.titulo} | ${CURSO.titulo}`;
    const etiquetas = [meta.bloque, `${meta.minutos} minutos`];
    if (meta.practica) etiquetas.push(etiquetaPractica(meta.practica));
    hoja.innerHTML =
      `<header class="leccion-cabecera"><div class="leccion-numero"><strong>${String(meta.numero).padStart(2, '0')}</strong><span>de ${LECCIONES.length}</span></div>` +
      `<h1>${escapar(meta.titulo)}</h1><p class="leccion-resumen">${escapar(meta.resumen)}</p>` +
      `<div class="leccion-meta">${etiquetas.map((e) => `<span class="etiqueta">${escapar(e)}</span>`).join('')}</div></header>` +
      `<article class="prosa" id="prosa">${renderizar(lec.markdown)}</article>` +
      finalLeccion(meta) +
      navegacion();

    const prosa = document.getElementById('prosa');
    anclarTitulos(prosa);
    activarContenido(prosa);
    pintarIndiceLocal(prosa);
    activarCompletada(meta);
    if (location.hash) {
      const destino = document.getElementById(decodeURIComponent(location.hash.slice(1)));
      if (destino) destino.scrollIntoView();
    }
  }

  function anclarTitulos(prosa) {
    const usados = new Set();
    prosa.querySelectorAll('h2, h3').forEach((h) => {
      let id = slugificar(h.textContent) || 'seccion';
      let n = 2;
      while (usados.has(id)) id = `${id}-${n++}`;
      usados.add(id);
      h.id = id;
    });
  }

  function pintarIndiceLocal(prosa) {
    const caja = document.getElementById('indice-local');
    if (!caja) return;
    const titulos = [...prosa.querySelectorAll('h2')];
    if (titulos.length < 2) {
      caja.hidden = true;
      return;
    }
    caja.innerHTML =
      '<p>En esta lección</p>' +
      titulos.map((h) => `<a href="#${h.id}">${escapar(h.textContent)}</a>`).join('');
    const enlaces = [...caja.querySelectorAll('a')];
    if (!('IntersectionObserver' in window)) return;
    const visibles = new Map();
    const obs = new IntersectionObserver(
      (entradas) => {
        entradas.forEach((e) => visibles.set(e.target.id, e.isIntersecting));
        const primero = titulos.find((h) => visibles.get(h.id));
        if (!primero) return;
        enlaces.forEach((a) => a.classList.toggle('activo', a.getAttribute('href') === `#${primero.id}`));
      },
      { rootMargin: '-70px 0px -65% 0px' },
    );
    titulos.forEach((h) => obs.observe(h));
  }

  function finalLeccion(meta) {
    const hecha = progreso.hechas().has(meta.slug);
    return (
      `<div class="leccion-final"><span class="etiqueta" id="estado-leccion">${hecha ? '✓ Completada' : 'Sin completar'}</span>` +
      `<button type="button" class="boton ${hecha ? 'boton-secundario' : 'boton-principal'}" id="boton-completada">${hecha ? 'Marcar como pendiente' : 'Marcar como completada'}</button></div>`
    );
  }

  function activarCompletada(meta) {
    const boton = document.getElementById('boton-completada');
    boton.addEventListener('click', () => {
      const hecha = !progreso.hechas().has(meta.slug);
      progreso.marcar(meta.slug, hecha);
      boton.textContent = hecha ? 'Marcar como pendiente' : 'Marcar como completada';
      boton.className = `boton ${hecha ? 'boton-secundario' : 'boton-principal'}`;
      document.getElementById('estado-leccion').textContent = hecha ? '✓ Completada' : 'Sin completar';
      pintarLateral();
    });
  }

  function navegacion() {
    const ant = LECCIONES[posicion - 1];
    const sig = LECCIONES[posicion + 1];
    const enlace = (l, clase, texto) =>
      l
        ? `<a class="${clase}" href="leccion.html?l=${encodeURIComponent(l.slug)}"><small>${texto}</small>${escapar(l.titulo)}</a>`
        : '';
    return (
      `<nav class="navegacion" aria-label="Lecciones vecinas">${enlace(ant, 'anterior', 'Anterior')}` +
      `${sig ? enlace(sig, 'siguiente', 'Siguiente') : '<a class="siguiente" href="index.html"><small>Fin del curso</small>Volver al temario</a>'}</nav>`
    );
  }
})();
