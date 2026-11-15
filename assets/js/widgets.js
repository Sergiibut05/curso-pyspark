/*
 * Simuladores del curso de PySpark.
 *  - plan: transformaciones perezosas, acciones, etapas y shuffle.
 *  - microlotes: Structured Streaming leyendo Kafka con checkpoint, y qué pasa si se cae.
 */
(function () {
  'use strict';
  const esc = (t) => window.Curso.escapar(t);

  const WIDGETS = {};

  /* ── 1. Plan perezoso → etapas ─────────────────────────────────────── */
  WIDGETS.plan = function (el) {
    const TRANSFORMACIONES = [
      { id: 'filter', codigo: '.filter(F.col("mag") >= 5)', shuffle: false },
      { id: 'withColumn', codigo: '.withColumn("dia", F.to_date("hora"))', shuffle: false },
      { id: 'select', codigo: '.select("id", "mag", "red")', shuffle: false },
      { id: 'groupBy', codigo: '.groupBy("red").count()', shuffle: true },
      { id: 'orderBy', codigo: '.orderBy(F.col("mag").desc())', shuffle: true },
      { id: 'join', codigo: '.join(redes, "red")', shuffle: true },
    ];
    const ACCIONES = [
      { id: 'show', codigo: '.show(5)' },
      { id: 'count', codigo: '.count()' },
      { id: 'collect', codigo: '.collect()' },
    ];
    let pasos = [];
    let ejecutando = false;
    el.innerHTML = `
      <p class="widget-titulo">Transformaciones perezosas, acciones y etapas</p>
      <p class="widget-ayuda">Añade transformaciones: Spark solo apunta el plan. Al pedir una <strong>acción</strong> lo ejecuta, partido en <strong>etapas</strong>: cada <span style="color:var(--aviso);font-weight:600">shuffle</span> (mover filas entre particiones) corta una etapa. Cada etapa son 4 tareas, una por partición.</p>
      <div class="widget-controles" data-trans></div>
      <div class="plan" data-plan></div>
      <div class="widget-controles" style="margin-top:.8rem" data-acciones></div>
      <div class="etapas" data-etapas></div>
      <div class="widget-estado" aria-live="polite" data-estado>df = spark.read.json(…) — todavía no se ha leído nada.</div>`;
    const caja = el.querySelector('[data-trans]');
    caja.innerHTML =
      TRANSFORMACIONES.map(
        (t) => `<button type="button" class="boton boton-secundario boton-pequeno" data-t="${t.id}">${esc(t.codigo)}</button>`,
      ).join('') + '<button type="button" class="boton boton-secundario boton-pequeno" data-borrar>Vaciar</button>';
    el.querySelector('[data-acciones]').innerHTML =
      '<span style="color:var(--tinta-tenue);font-size:.8rem">Acción:</span>' +
      ACCIONES.map(
        (a) => `<button type="button" class="boton boton-principal boton-pequeno" data-a="${a.id}">${esc(a.codigo)}</button>`,
      ).join('');
    const plan = el.querySelector('[data-plan]');
    const etapas = el.querySelector('[data-etapas]');
    const estado = el.querySelector('[data-estado]');

    function pintarPlan() {
      plan.innerHTML =
        '<span class="plan-paso">read.json</span>' +
        pasos
          .map(
            (p) =>
              `<span class="plan-flecha">→</span><span class="plan-paso${p.shuffle ? ' shuffle' : ''}" title="${esc(p.codigo)}">${esc(p.id)}${p.shuffle ? ' ⇄' : ''}</span>`,
          )
          .join('');
    }
    function cortarEtapas() {
      const lista = [['read.json']];
      pasos.forEach((p) => {
        if (p.shuffle) lista.push([p.id]);
        else lista[lista.length - 1].push(p.id);
      });
      return lista;
    }
    caja.addEventListener('click', (e) => {
      if (ejecutando) return;
      if (e.target.closest('[data-borrar]')) {
        pasos = [];
        etapas.innerHTML = '';
        pintarPlan();
        estado.textContent = 'Plan vacío. df = spark.read.json(…)';
        return;
      }
      const b = e.target.closest('[data-t]');
      if (!b) return;
      const t = TRANSFORMACIONES.find((x) => x.id === b.dataset.t);
      if (pasos.length >= 7) return;
      pasos.push(t);
      pintarPlan();
      etapas.innerHTML = '';
      estado.innerHTML = `<code>df = df${esc(t.codigo)}</code> tarda 0 s: devuelve un DataFrame nuevo con un paso más en el plan. <strong>No ha leído ni una fila.</strong>`;
    });
    el.querySelector('[data-acciones]').addEventListener('click', (e) => {
      const b = e.target.closest('[data-a]');
      if (!b || ejecutando) return;
      const accion = ACCIONES.find((a) => a.id === b.dataset.a);
      ejecutar(accion);
    });

    function ejecutar(accion) {
      const lista = cortarEtapas();
      ejecutando = true;
      etapas.innerHTML = lista
        .map(
          (pasosEtapa, i) =>
            `<div class="etapa"><div class="etapa-titulo">Etapa ${i + 1}: ${esc(pasosEtapa.join(' → '))}${i < lista.length - 1 ? ' → <span style="color:var(--aviso)">escribe shuffle</span>' : ''}${i > 0 ? ' (lee el shuffle)' : ''}</div>` +
            `<div class="tareas">${[0, 1, 2, 3].map((t) => `<div class="tarea" data-e="${i}" data-t="${t}"><span>tarea ${t}</span></div>`).join('')}</div></div>`,
        )
        .join('');
      estado.innerHTML = `<code>${esc(accion.codigo)}</code> es una acción: Spark optimiza el plan y lanza un <em>job</em> de ${lista.length} etapa${lista.length > 1 ? 's' : ''}.`;
      let etapa = 0;
      const correr = () => {
        if (etapa >= lista.length) {
          ejecutando = false;
          const shuffles = lista.length - 1;
          let final = `Hecho: ${lista.length} etapa${lista.length > 1 ? 's' : ''}, ${shuffles} shuffle${shuffles === 1 ? '' : 's'}, ${lista.length * 4} tareas.`;
          if (accion.id === 'collect')
            final += ' <strong>collect()</strong> trae todas las filas al driver (el proceso de Python): con muchos datos, se queda sin memoria.';
          else if (accion.id === 'show') final += ' show() solo trae al driver las filas que enseña.';
          else final += ' count() solo trae un número al driver.';
          final += ' Si repites la acción, <strong>se vuelve a ejecutar todo</strong> (salvo que hagas <code>cache()</code>).';
          estado.innerHTML = final;
          return;
        }
        const tareas = [...etapas.querySelectorAll(`.tarea[data-e="${etapa}"]`)];
        const duraciones = tareas.map(() => 500 + Math.random() * 900);
        const inicio = performance.now();
        const paso = () => {
          const t = performance.now() - inicio;
          let todas = true;
          tareas.forEach((tarea, i) => {
            const p = Math.min(100, (t / duraciones[i]) * 100);
            tarea.style.setProperty('--p', p + '%');
            if (p >= 100) tarea.classList.add('hecha');
            else todas = false;
          });
          if (todas) {
            etapa += 1;
            setTimeout(correr, 250);
          } else requestAnimationFrame(paso);
        };
        requestAnimationFrame(paso);
      };
      correr();
    }
    pintarPlan();
  };

  /* ── 2. Micro-lotes con checkpoint ─────────────────────────────────── */
  WIDGETS.microlotes = function (el) {
    let s;
    el.innerHTML = `
      <p class="widget-titulo">Micro-lotes, checkpoint y caídas</p>
      <p class="widget-ayuda">Llegan informes a Kafka. En cada disparo, Spark decide el rango de offsets del lote, lo <strong>apunta en el checkpoint</strong> (<code>offsets/N</code>), ejecuta <code>foreachBatch</code> y, si termina, apunta <code>commits/N</code>. Provoca una caída a mitad de un lote.</p>
      <div class="widget-controles">
        <button type="button" class="boton boton-secundario boton-pequeno" data-llegan>+ 3 informes</button>
        <button type="button" class="boton boton-principal boton-pequeno" data-disparo>Disparo (trigger)</button>
        <button type="button" class="boton boton-secundario boton-pequeno" data-caida>Provocar una caída en foreachBatch</button>
        <label>maxOffsetsPerTrigger <select aria-label="Tope por lote"><option>2</option><option selected>4</option><option>100</option></select></label>
        <button type="button" class="boton boton-secundario boton-pequeno" data-reinicio>Reiniciar</button>
      </div>
      <div class="particiones">
        <div class="particion"><span class="particion-nombre">eventos.usgs</span><div class="particion-log" data-log></div></div>
      </div>
      <div class="lotes" data-lotes></div>
      <div class="widget-estado" aria-live="polite" data-estado></div>`;
    const estado = el.querySelector('[data-estado]');
    const tope = () => Number(el.querySelector('select').value);

    function reiniciar() {
      s = { mensajes: 5, procesado: 0, lotes: [], siguiente: 0, caer: false, emitidos: [] };
      estado.innerHTML = 'Hay 5 informes en el topic y el checkpoint está vacío: la consulta empieza en <code>earliest</code> (offset 0).';
      pintar();
    }
    function pintar() {
      el.querySelector('[data-log]').innerHTML = Array.from({ length: s.mensajes }, (_, off) => {
        const hecho = off < s.procesado;
        return `<span class="mensaje" style="--c:${hecho ? '#17784a' : '#d24a12'};min-width:2.2rem">off ${off}</span>`;
      }).join('');
      el.querySelector('[data-lotes]').innerHTML = s.lotes
        .slice(-6)
        .map(
          (l) =>
            `<div class="lote${l.repetido ? ' repetido' : ''}"><span class="lote-id">lote ${l.id}</span>` +
            `<span>offsets ${l.desde}–${l.hasta - 1} (${l.hasta - l.desde})${l.repetido ? ' · <strong>repetido tras la caída</strong>' : ''}</span>` +
            `<span class="lote-estado ${l.commit ? 'ok' : l.caido ? 'mal' : ''}">offsets/${l.id} ✓ · ${l.commit ? 'commits/' + l.id + ' ✓' : l.caido ? 'sin commit' : '…'}</span></div>`,
        )
        .join('');
    }
    function disparo() {
      const pendiente = s.lotes.find((l) => !l.commit);
      let lote;
      if (pendiente) {
        // Al arrancar, Spark rehace el último lote planificado y no confirmado: mismo id, mismos offsets.
        lote = { ...pendiente, repetido: true, caido: false };
        s.lotes = s.lotes.filter((l) => l !== pendiente);
      } else {
        const desde = s.procesado;
        const hasta = Math.min(s.mensajes, desde + tope());
        if (hasta === desde) {
          estado.innerHTML = 'Disparo sin datos nuevos: no se crea lote (con <code>processingTime</code> Spark simplemente espera al siguiente disparo).';
          return;
        }
        lote = { id: s.siguiente++, desde, hasta, commit: false, caido: false, repetido: false };
      }
      s.lotes.push(lote);
      if (s.caer) {
        s.caer = false;
        lote.caido = true;
        estado.innerHTML = `Caída en el lote ${lote.id}: <code>offsets/${lote.id}</code> estaba escrito, pero el proceso murió dentro de <code>foreachBatch</code>. Si ya había escrito en Kafka, esos mensajes están ahí. Pulsa «Disparo» para simular el reinicio.`;
        pintar();
        return;
      }
      lote.commit = true;
      s.procesado = lote.hasta;
      estado.innerHTML = lote.repetido
        ? `Reinicio: Spark lee el checkpoint, ve <code>offsets/${lote.id}</code> sin <code>commits/${lote.id}</code> y <strong>repite el lote ${lote.id} con los mismos offsets</strong> (${lote.desde}–${lote.hasta - 1}). Si foreachBatch es determinista e idempotente, el resultado es el mismo que si no hubiera habido caída.`
        : `Lote ${lote.id}: offsets ${lote.desde}–${lote.hasta - 1}${lote.hasta - lote.desde === tope() ? ` (tope de ${tope()} por lote)` : ''}. foreachBatch terminó y se escribió <code>commits/${lote.id}</code>.`;
      pintar();
    }
    el.querySelector('[data-llegan]').addEventListener('click', () => {
      s.mensajes += 3;
      estado.innerHTML = `Llegan 3 informes: offsets ${s.mensajes - 3}–${s.mensajes - 1}. Esperan al siguiente disparo.`;
      pintar();
    });
    el.querySelector('[data-disparo]').addEventListener('click', disparo);
    el.querySelector('[data-caida]').addEventListener('click', () => {
      s.caer = true;
      disparo();
    });
    el.querySelector('[data-reinicio]').addEventListener('click', reiniciar);
    reiniciar();
  };

  window.WIDGETS = Object.assign(window.WIDGETS || {}, WIDGETS);
})();
