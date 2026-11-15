/*
 * Portada: el titular animado (una sola vez, al cargar), el temario como índice, el botón de
 * continuar, la muestra de una lección y el simulador.
 */
(function () {
  'use strict';
  const { LECCIONES, progreso, escapar, renderizar, activarContenido, montarBarra, etiquetaPractica } = window.Curso;
  const sinMovimiento = window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;

  montarBarra();

  /* Kafka: un consumidor lee los registros del titular y se queda esperando el siguiente offset. */
  function animarLog() {
    const titular = document.querySelector('.titular-log');
    const marca = document.querySelector('.lector-marca');
    if (!titular || !marca) return;
    const registros = [...titular.querySelectorAll('.registro')];
    const siguiente = titular.querySelector('.registro-siguiente');
    const texto = marca.querySelector('span');
    const colocar = (el) => {
      const caja = titular.getBoundingClientRect();
      const r = el.getBoundingClientRect();
      const izquierda = r.left - caja.left;
      const cabe = caja.width - izquierda >= 190;
      // Cerca del borde derecho, la etiqueta se alinea por la derecha del registro para no salirse.
      marca.style.left = cabe ? `${izquierda}px` : 'auto';
      marca.style.right = cabe ? 'auto' : `${Math.max(0, caja.right - r.right)}px`;
      marca.style.alignItems = cabe ? 'flex-start' : 'flex-end';
      marca.style.top = `${r.bottom - caja.top + 8}px`;
    };
    const fin = () => {
      registros.forEach((r) => r.classList.add('leido'));
      colocar(siguiente);
      texto.textContent = `al día: espera el offset ${registros.length}`;
    };
    if (sinMovimiento) return fin();
    let i = 0;
    colocar(registros[0]);
    const paso = () => {
      if (i < registros.length) {
        colocar(registros[i]);
        registros[i].classList.add('leido');
        texto.textContent = `consumidor: offset ${i}`;
        i += 1;
        setTimeout(paso, 650);
      } else fin();
    };
    setTimeout(paso, 500);
    window.addEventListener('resize', () => colocar(i < registros.length ? registros[i] : siguiente));
  }

  /* PySpark: el titular es una etapa; sus cuatro tareas avanzan en paralelo a ritmos distintos. */
  function animarEtapa() {
    const tareas = [...document.querySelectorAll('.trozo')];
    const estado = document.querySelector('.etapa-estado');
    if (!tareas.length) return;
    const duraciones = [900, 1350, 1100, 1600];
    const fin = (ms) => {
      tareas.forEach((t) => {
        t.querySelector('.trozo-barra').style.setProperty('--p', '100%');
        t.classList.add('hecha');
      });
      if (estado) estado.textContent = `etapa 0 terminada: 4 particiones, 4 tareas en paralelo, ${(ms / 1000).toFixed(1).replace('.', ',')} s`;
    };
    if (sinMovimiento) return fin(1600);
    const inicio = performance.now() + 400;
    const paso = (ahora) => {
      const t = Math.max(0, ahora - inicio);
      let todas = true;
      tareas.forEach((tarea, i) => {
        const p = Math.min(100, (t / duraciones[i]) * 100);
        tarea.querySelector('.trozo-barra').style.setProperty('--p', `${p}%`);
        if (p >= 100) tarea.classList.add('hecha');
        else todas = false;
      });
      if (estado && !todas) estado.textContent = `etapa 0 en marcha: ${tareas.filter((x) => x.classList.contains('hecha')).length} de 4 tareas terminadas`;
      if (todas) fin(Math.max(...duraciones));
      else requestAnimationFrame(paso);
    };
    requestAnimationFrame(paso);
  }

  function pintarTemario() {
    const caja = document.getElementById('temario');
    if (!caja) return;
    const hechas = progreso.hechas();
    const bloques = [];
    LECCIONES.forEach((l) => {
      let b = bloques.find((x) => x.nombre === l.bloque);
      if (!b) bloques.push((b = { nombre: l.bloque, lecciones: [] }));
      b.lecciones.push(l);
    });
    caja.innerHTML = bloques
      .map((b) => {
        const minutos = b.lecciones.reduce((s, l) => s + l.minutos, 0);
        const n = b.lecciones.length;
        return (
          `<section class="indice-bloque"><div class="indice-bloque-nombre"><h3>${escapar(b.nombre)}</h3>` +
          `<p>${n} ${n === 1 ? 'lección' : 'lecciones'}, unos ${minutos} minutos</p></div><ol class="indice-lista">` +
          b.lecciones
            .map((l) => {
              const hecha = hechas.has(l.slug);
              return (
                `<li><a class="fila${hecha ? ' hecha' : ''}" href="leccion.html?l=${encodeURIComponent(l.slug)}">` +
                `<span class="fila-numero">${hecha ? '✓' : String(l.numero).padStart(2, '0')}</span>` +
                `<span><span class="fila-titulo">${escapar(l.titulo)}</span><span class="fila-resumen">${escapar(l.resumen)}</span></span>` +
                `<span class="fila-meta">${l.minutos} min${l.practica ? `<small>${escapar(etiquetaPractica(l.practica))}</small>` : ''}</span></a></li>`
              );
            })
            .join('') +
          '</ol></section>'
        );
      })
      .join('');
  }

  function pintarContinuar() {
    const boton = document.getElementById('continuar');
    const texto = document.getElementById('portada-progreso');
    if (!boton || !LECCIONES.length) return;
    const hechas = progreso.hechas();
    const siguiente = LECCIONES.find((l) => !hechas.has(l.slug));
    const { hechas: n, total } = progreso.resumen();
    if (n === 0) {
      boton.href = `leccion.html?l=${encodeURIComponent(LECCIONES[0].slug)}`;
      boton.textContent = 'Empezar el curso';
      if (texto) texto.textContent = `${total} lecciones. Tu progreso se guarda en este navegador.`;
    } else if (siguiente) {
      boton.href = `leccion.html?l=${encodeURIComponent(siguiente.slug)}`;
      boton.textContent = `Seguir con la lección ${siguiente.numero}`;
      if (texto) texto.textContent = `Llevas ${n} de ${total} lecciones. Toca «${siguiente.titulo}».`;
    } else {
      boton.href = 'leccion.html?l=chuleta';
      boton.textContent = 'Repasar la chuleta';
      if (texto) texto.textContent = `Has terminado las ${total} lecciones.`;
    }
  }

  function pintarMuestra() {
    const fuente = document.getElementById('muestra-md');
    const destino = document.getElementById('muestra');
    if (!fuente || !destino) return;
    destino.innerHTML = renderizar(fuente.textContent.replace(/^\n+/, ''));
    activarContenido(destino);
  }

  pintarTemario();
  pintarContinuar();
  pintarMuestra();
  document.addEventListener('progreso', () => {
    pintarTemario();
    pintarContinuar();
  });
  document.querySelectorAll('[data-demo]').forEach((el) => activarContenido(el));
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(() => (animarLog(), animarEtapa()));
  else (animarLog(), animarEtapa());
})();
