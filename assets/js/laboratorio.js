/*
 * Ejecutar los bloques de Python de una lección en el laboratorio, como las celdas de un cuaderno.
 *
 * PySpark necesita Java, así que no puede correr en el navegador: el código se manda a un núcleo de Jupyter
 * que corre en Docker (servicio «jupyter» de laboratorio/compose.yaml). curso.ps1 escribe su dirección y su
 * token en assets/js/laboratorio-local.js (window.LABORATORIO); sin ese fichero, la lección se ve igual que
 * siempre y los botones explican cómo arrancar el laboratorio.
 *
 * Cada lección abierta tiene su propio núcleo (su propia sesión de Spark): los bloques comparten variables y
 * se ejecutan en orden. Si ejecutas un bloque sin haber ejecutado los anteriores, se ejecutan antes los que
 * falten (salvo los de las soluciones, que son opcionales). Al cambiar de lección o recargar se empieza de
 * cero; Jupyter cierra además los núcleos que lleven 20 minutos sin uso.
 *
 * Protocolo: REST (/api/kernels) para crear, interrumpir y reiniciar el núcleo, y un WebSocket
 * (/api/kernels/<id>/channels) con los mensajes de Jupyter en JSON (execute_request, stream, error…).
 */
(function () {
  'use strict';

  const CONFIG = window.LABORATORIO && window.LABORATORIO.url && window.LABORATORIO.token ? window.LABORATORIO : null;
  const CLAVE_NUCLEO = 'curso-pyspark:nucleo';
  const MAX_SALIDA = 200000; // caracteres de salida por bloque antes de recortar
  const ORDEN_LAB = '.\\curso.ps1';

  function escapar(texto) {
    return String(texto).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function quitarAnsi(texto) {
    return String(texto).replace(/\x1b\[[0-9;]*[A-Za-z]/g, '');
  }
  function uuid() {
    const b = new Uint8Array(16);
    crypto.getRandomValues(b);
    return [...b].map((x) => x.toString(16).padStart(2, '0')).join('');
  }
  function segundos(ms) {
    return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toLocaleString('es-ES', { maximumFractionDigits: 1 })} s`;
  }
  const almacen = {
    leer() {
      try {
        return localStorage.getItem(CLAVE_NUCLEO);
      } catch {
        return null;
      }
    },
    guardar(v) {
      try {
        if (v) localStorage.setItem(CLAVE_NUCLEO, v);
        else localStorage.removeItem(CLAVE_NUCLEO);
      } catch {
        /* sin almacenamiento: solo se pierde la limpieza del núcleo anterior */
      }
    },
  };

  /* ── Cliente mínimo de un núcleo de Jupyter ── */
  class Nucleo {
    constructor(cfg, alCambiar) {
      this.cfg = cfg;
      this.id = null;
      this.ws = null;
      this.sesion = uuid();
      this.generacion = 0; // cambia con cada núcleo nuevo o reinicio: lo ejecutado antes ya no cuenta
      this.pendientes = new Map();
      this.alCambiar = alCambiar;
      this.arrancando = null;
    }

    async api(metodo, ruta, cuerpo, opciones = {}) {
      const cabeceras = { Authorization: `token ${this.cfg.token}` };
      if (cuerpo) cabeceras['Content-Type'] = 'application/json';
      const r = await fetch(this.cfg.url + ruta, {
        method: metodo,
        headers: cabeceras,
        body: cuerpo ? JSON.stringify(cuerpo) : undefined,
        cache: 'no-store',
        ...opciones,
      });
      if (!r.ok) {
        const e = new Error(`${metodo} ${ruta}: HTTP ${r.status}`);
        e.status = r.status;
        throw e;
      }
      return r.status === 204 ? null : r.json();
    }

    async disponible() {
      try {
        await this.api('GET', '/api/status');
        return true;
      } catch {
        return false;
      }
    }

    get conectado() {
      return Boolean(this.ws && this.ws.readyState === WebSocket.OPEN);
    }

    async arrancar() {
      if (this.conectado) return;
      if (!this.arrancando) {
        this.arrancando = (async () => {
          this.alCambiar('arrancando');
          if (!this.id) {
            // El núcleo que dejó otra lección (o esta misma antes de recargar) ya no lo usa nadie.
            const previo = almacen.leer();
            if (previo) await this.api('DELETE', `/api/kernels/${previo}`).catch(() => {});
            const k = await this.api('POST', '/api/kernels', { name: 'python3' });
            this.id = k.id;
            this.generacion += 1;
            almacen.guardar(k.id);
          }
          await this.conectar();
        })();
        this.arrancando.then(
          () => this.alCambiar('libre'),
          () => this.alCambiar('apagado'),
        );
        this.arrancando.finally(() => (this.arrancando = null)).catch(() => {});
      }
      return this.arrancando;
    }

    conectar() {
      return new Promise((resolver, rechazar) => {
        const url =
          this.cfg.url.replace(/^http/, 'ws') +
          `/api/kernels/${this.id}/channels?session_id=${this.sesion}&token=${encodeURIComponent(this.cfg.token)}`;
        const ws = new WebSocket(url);
        let abierto = false;
        ws.onopen = () => {
          abierto = true;
          this.ws = ws;
          resolver();
        };
        ws.onerror = () => {
          if (!abierto) rechazar(new Error('No se pudo conectar con el núcleo de Jupyter.'));
        };
        ws.onclose = () => {
          if (this.ws === ws) this.perdido();
          else if (!abierto) rechazar(new Error('No se pudo conectar con el núcleo de Jupyter.'));
        };
        ws.onmessage = (ev) => this.recibir(ev.data);
      });
    }

    /* La conexión se cerró sin pedirlo: laboratorio parado, núcleo cerrado por inactividad u otra pestaña. */
    perdido() {
      this.ws = null;
      if (this.id && almacen.leer() === this.id) almacen.guardar(null);
      this.id = null;
      const error = new Error(
        'Se perdió la conexión con la sesión (¿se paró el laboratorio, o abriste otra lección en otra pestaña?). ' +
          'Vuelve a ejecutar para empezar una sesión nueva.',
      );
      for (const p of this.pendientes.values()) p.rechazar(error);
      this.pendientes.clear();
      this.alCambiar('perdido');
    }

    recibir(datos) {
      if (typeof datos !== 'string') return; // mensajes binarios (buffers): aquí no se usan
      let msg;
      try {
        msg = JSON.parse(datos);
      } catch {
        return;
      }
      const padre = msg.parent_header && msg.parent_header.msg_id;
      const p = padre && this.pendientes.get(padre);
      if (!p) return;
      const c = msg.content || {};
      switch (msg.msg_type) {
        case 'execute_input':
          p.alEmpezar(c.execution_count);
          break;
        case 'stream':
          p.salida.texto(c.name, c.text);
          break;
        case 'execute_result':
        case 'display_data':
          p.salida.datos(c.data || {});
          break;
        case 'clear_output':
          p.salida.limpiar();
          break;
        case 'error':
          p.salida.error(c);
          break;
        case 'execute_reply':
          p.respuesta = c;
          break;
        case 'status':
          if (c.execution_state === 'idle') p.inactivo = true;
          break;
        default:
          break;
      }
      // Terminado cuando llegan la respuesta (canal shell) y el «idle» (canal iopub): ya no habrá más salida.
      if (p.respuesta && p.inactivo) {
        this.pendientes.delete(padre);
        if (!this.pendientes.size) this.alCambiar('libre');
        p.resolver(p.respuesta);
      }
    }

    async ejecutar(codigo, salida, alEmpezar) {
      await this.arrancar();
      const id = uuid();
      const msg = {
        header: {
          msg_id: id,
          username: 'curso',
          session: this.sesion,
          msg_type: 'execute_request',
          version: '5.3',
          date: new Date().toISOString(),
        },
        parent_header: {},
        metadata: {},
        content: { code: codigo, silent: false, store_history: true, user_expressions: {}, allow_stdin: false, stop_on_error: true },
        buffers: [],
        channel: 'shell',
      };
      return new Promise((resolver, rechazar) => {
        this.pendientes.set(id, { salida, alEmpezar, resolver, rechazar, respuesta: null, inactivo: false });
        this.alCambiar('ocupado');
        this.ws.send(JSON.stringify(msg));
      });
    }

    async interrumpir() {
      if (this.id) await this.api('POST', `/api/kernels/${this.id}/interrupt`);
    }

    async reiniciar() {
      if (!this.id) return;
      this.alCambiar('arrancando');
      await this.api('POST', `/api/kernels/${this.id}/restart`);
      this.generacion += 1;
      this.alCambiar('libre');
    }

    /* Al salir de la página: cerrar el núcleo para no dejar una JVM ocupando memoria. */
    soltar() {
      if (!this.id) return;
      this.api('DELETE', `/api/kernels/${this.id}`, null, { keepalive: true }).catch(() => {});
      if (almacen.leer() === this.id) almacen.guardar(null);
    }
  }

  /* ── La salida de un bloque ── */
  class Salida {
    constructor(el) {
      this.el = el;
      this.cuerpo = el.querySelector('.celda-salida-cuerpo');
      this.pie = el.querySelector('.celda-salida-pie');
      this.ultimo = null; // último <pre> de stream, para seguir escribiendo en él
      this.caracteres = 0;
    }
    limpiar() {
      this.cuerpo.innerHTML = '';
      this.pie.textContent = '';
      this.ultimo = null;
      this.caracteres = 0;
      this.el.classList.remove('con-error');
    }
    mostrar() {
      this.el.hidden = false;
    }
    hayAlgo() {
      return this.cuerpo.childElementCount > 0;
    }
    anadir(nodo) {
      this.cuerpo.appendChild(nodo);
      this.ultimo = null;
    }
    recortar(texto) {
      if (this.caracteres >= MAX_SALIDA) return '';
      this.caracteres += texto.length;
      if (this.caracteres <= MAX_SALIDA) return texto;
      return texto.slice(0, texto.length - (this.caracteres - MAX_SALIDA)) + '\n… (salida recortada)\n';
    }
    texto(nombre, texto) {
      const t = this.recortar(quitarAnsi(texto));
      if (!t) return;
      if (!this.ultimo || this.ultimo.dataset.flujo !== nombre) {
        const pre = document.createElement('pre');
        pre.className = `celda-flujo flujo-${nombre === 'stderr' ? 'stderr' : 'stdout'}`;
        pre.dataset.flujo = nombre;
        this.cuerpo.appendChild(pre);
        this.ultimo = pre;
      }
      this.ultimo.textContent += t;
    }
    datos(data) {
      if (data['image/png']) {
        const img = document.createElement('img');
        img.src = `data:image/png;base64,${data['image/png']}`;
        img.alt = 'Imagen generada por el código';
        this.anadir(img);
      } else if (data['text/plain'] !== undefined) {
        const pre = document.createElement('pre');
        pre.className = 'celda-resultado';
        pre.textContent = this.recortar(quitarAnsi(data['text/plain']));
        this.anadir(pre);
      }
    }
    error(c) {
      this.el.classList.add('con-error');
      const caja = document.createElement('div');
      caja.className = 'celda-error';
      const traza = quitarAnsi((c.traceback || []).join('\n'));
      caja.innerHTML =
        `<pre class="celda-error-mensaje"><strong>${escapar(c.ename || 'Error')}</strong>: ${escapar(quitarAnsi(c.evalue || ''))}</pre>` +
        (traza ? `<details><summary>Traza completa</summary><pre>${escapar(traza)}</pre></details>` : '');
      this.anadir(caja);
    }
    aviso(html, clase = 'celda-aviso') {
      const p = document.createElement('p');
      p.className = clase;
      p.innerHTML = html;
      this.anadir(p);
    }
  }

  /* ── Un bloque ejecutable ── */
  class Celda {
    constructor(el, indice, lab) {
      this.el = el;
      this.indice = indice;
      this.lab = lab;
      this.code = el.querySelector('code');
      this.original = this.code.textContent.replace(/\n$/, '');
      this.preparacion = el.hasAttribute('data-preparacion');
      // Las soluciones de los ejercicios son opcionales: no se ejecutan solas al ejecutar un bloque posterior.
      this.opcional = Boolean(el.closest('.ejercicio details'));
      this.generacion = -1; // generación del núcleo en la que se ejecutó bien por última vez
      this.estado = 'quieta';
      this.montar();
    }

    montar() {
      const cab = this.el.querySelector('.codigo-cabecera');
      this.contador = document.createElement('span');
      this.contador.className = 'celda-contador';
      this.contador.textContent = '[ ]';
      this.contador.title = 'Orden de ejecución en esta sesión';
      cab.prepend(this.contador);

      this.botonRestaurar = document.createElement('button');
      this.botonRestaurar.type = 'button';
      this.botonRestaurar.className = 'copiar celda-restaurar';
      this.botonRestaurar.textContent = 'Restaurar';
      this.botonRestaurar.title = 'Volver al código original de la lección';
      this.botonRestaurar.hidden = true;
      this.botonRestaurar.addEventListener('click', () => this.restaurar());

      this.boton = document.createElement('button');
      this.boton.type = 'button';
      this.boton.className = 'celda-ejecutar';
      this.boton.title = 'Ejecutar en el laboratorio (Mayús+Intro desde el código)';
      this.boton.addEventListener('click', () => (this.estado === 'ejecutando' ? this.lab.interrumpir() : this.lab.ejecutarHasta(this)));
      const copiar = cab.querySelector('[data-copiar]');
      cab.insertBefore(this.botonRestaurar, copiar);
      cab.insertBefore(this.boton, copiar);
      this.pintarBoton();

      // Código editable: se puede cambiar y volver a ejecutar.
      try {
        this.code.contentEditable = 'plaintext-only';
      } catch {
        this.code.contentEditable = 'true';
      }
      if (this.code.contentEditable !== 'plaintext-only') this.code.contentEditable = 'true';
      this.code.spellcheck = false;
      this.code.setAttribute('autocapitalize', 'off');
      this.code.setAttribute('aria-label', 'Código editable. Mayús+Intro para ejecutarlo; Escape para salir.');
      this.code.addEventListener('keydown', (ev) => this.tecla(ev));
      this.code.addEventListener('input', () => (this.botonRestaurar.hidden = this.codigo() === this.original));
      this.code.addEventListener('blur', () => this.recolorear());
      this.code.addEventListener('paste', (ev) => {
        // Pegar siempre como texto plano (contentEditable="true" pegaría HTML)
        ev.preventDefault();
        document.execCommand('insertText', false, (ev.clipboardData || window.clipboardData).getData('text/plain'));
      });

      this.salidaEl = document.createElement('div');
      this.salidaEl.className = 'celda-salida';
      this.salidaEl.hidden = true;
      this.salidaEl.setAttribute('aria-live', 'polite');
      this.salidaEl.innerHTML =
        '<div class="celda-salida-cabecera"><span>Resultado de tu ejecución</span><span class="celda-salida-pie"></span></div>' +
        '<div class="celda-salida-cuerpo"></div>';
      this.el.after(this.salidaEl);
      this.salida = new Salida(this.salidaEl);
    }

    codigo() {
      // innerText respeta los saltos de línea de la edición, pero vale «» si el bloque no se ve (la preparación,
      // dentro de un <details> cerrado): entonces textContent, que en un bloque sin editar es lo mismo.
      const plegado = Boolean(this.el.closest('details:not([open])'));
      return (plegado ? this.code.textContent : this.code.innerText).replace(/ /g, ' ').replace(/\n+$/, '');
    }

    tecla(ev) {
      if (ev.key === 'Enter' && (ev.shiftKey || ev.ctrlKey || ev.metaKey)) {
        ev.preventDefault();
        this.lab.ejecutarHasta(this);
      } else if (ev.key === 'Tab' && !ev.shiftKey) {
        ev.preventDefault();
        document.execCommand('insertText', false, '    ');
      } else if (ev.key === 'Escape') {
        this.code.blur();
      }
    }

    recolorear() {
      const texto = this.codigo();
      if (window.Curso && window.Curso.resaltar) this.code.innerHTML = window.Curso.resaltar(texto, 'python');
      this.botonRestaurar.hidden = texto === this.original;
    }

    restaurar() {
      this.code.textContent = this.original;
      this.recolorear();
    }

    pintarBoton() {
      const textos = { quieta: '▶ Ejecutar', cola: '… En cola', ejecutando: '■ Detener' };
      this.boton.textContent = textos[this.estado] || textos.quieta;
      this.boton.classList.toggle('activo', this.estado === 'ejecutando');
      this.el.classList.toggle('ejecutando', this.estado === 'ejecutando');
      this.el.classList.toggle('en-cola', this.estado === 'cola');
      if (this.estado === 'cola') this.contador.textContent = '[·]';
      if (this.estado === 'ejecutando') this.contador.textContent = '[*]';
    }

    ponerEstado(estado) {
      this.estado = estado;
      this.pintarBoton();
    }

    olvidar() {
      this.generacion = -1;
      this.contador.textContent = '[ ]';
    }

    /* Ejecuta el bloque. Devuelve true si terminó sin error. */
    async ejecutar() {
      this.ponerEstado('ejecutando');
      this.salida.limpiar();
      this.salida.mostrar();
      const inicio = performance.now();
      let ok = false;
      try {
        const r = await this.lab.nucleo.ejecutar(this.codigo(), this.salida, (n) => {
          this.contador.textContent = `[${n}]`;
        });
        ok = r.status === 'ok';
        if (ok) this.generacion = this.lab.nucleo.generacion;
        if (r.status === 'aborted') this.salida.aviso('No se ejecutó: falló un bloque anterior.');
      } catch (e) {
        this.contador.textContent = '[ ]';
        this.salida.aviso(escapar(e.message), 'celda-aviso celda-aviso-mal');
      } finally {
        if (ok && !this.salida.hayAlgo()) this.salida.aviso('Hecho, sin salida.', 'celda-aviso celda-sin-salida');
        this.salida.pie.textContent = segundos(performance.now() - inicio);
        this.ponerEstado('quieta');
      }
      // Un fallo dentro de un bloque plegado (la preparación) no debe quedar escondido
      const plegado = this.el.closest('details');
      if (!ok && plegado) plegado.open = true;
      return ok;
    }
  }

  /* ── El laboratorio de la lección: barra de estado, núcleo y orden de ejecución ── */
  class Laboratorio {
    constructor(raiz, meta) {
      this.meta = meta;
      this.celdas = [...raiz.querySelectorAll('[data-celda]')].map((el, i) => new Celda(el, i, this));
      this.nucleo = CONFIG ? new Nucleo(CONFIG, (e) => this.estado(e)) : null;
      this.tanda = 0; // se incrementa al interrumpir: lo que quedaba en cola se cancela
      this.cola = Promise.resolve();
      this.barra(raiz);
      if (this.nucleo) {
        this.estado('comprobando');
        this.nucleo.disponible().then((si) => this.estado(si ? 'listo' : 'apagado'));
        addEventListener('pagehide', () => this.nucleo.soltar());
      } else {
        this.estado('sin-configurar');
      }
    }

    barra(raiz) {
      const div = document.createElement('div');
      div.className = 'lab-barra';
      div.setAttribute('role', 'region');
      div.setAttribute('aria-label', 'Laboratorio');
      const cuaderno =
        CONFIG && this.meta && this.meta.cuaderno
          ? `<a class="boton boton-pequeno boton-secundario" target="_blank" rel="noopener" href="${escapar(
              `${CONFIG.url}/lab/tree/${encodeURIComponent(this.meta.cuaderno)}?token=${encodeURIComponent(CONFIG.token)}`,
            )}" title="Abre esta lección en JupyterLab (pestaña nueva)">Abrir como cuaderno</a>`
          : '';
      div.innerHTML =
        `<div class="lab-estado"><span class="lab-punto" aria-hidden="true"></span><span class="lab-texto"></span></div>` +
        `<div class="lab-acciones">` +
        `<button type="button" class="boton boton-pequeno boton-principal" data-lab="todo">▶ Ejecutar todo</button>` +
        `<button type="button" class="boton boton-pequeno boton-secundario" data-lab="interrumpir" title="Detiene el bloque en marcha (KeyboardInterrupt)">Interrumpir</button>` +
        `<button type="button" class="boton boton-pequeno boton-secundario" data-lab="reiniciar" title="Empieza una sesión de Spark nueva: se pierden las variables">Reiniciar sesión</button>` +
        cuaderno +
        `</div>`;
      div.addEventListener('click', (ev) => {
        const b = ev.target.closest('[data-lab]');
        if (!b) return;
        if (b.dataset.lab === 'todo') this.ejecutarTodo();
        if (b.dataset.lab === 'interrumpir') this.interrumpir();
        if (b.dataset.lab === 'reiniciar') this.reiniciar();
      });
      raiz.prepend(div);
      this.barraEl = div;
    }

    estado(e) {
      const textos = {
        'sin-configurar': `Para ejecutar el código aquí, arranca el laboratorio con <code>${ORDEN_LAB}</code> (o doble clic en <code>curso.cmd</code>) y recarga la página.`,
        comprobando: 'Buscando el laboratorio…',
        apagado: `El laboratorio no responde. Arráncalo con <code>${ORDEN_LAB}</code> (con Docker Desktop en marcha) y vuelve a probar.`,
        listo: 'Laboratorio listo. Los bloques de Python se ejecutan en una sesión de Spark propia de esta lección.',
        arrancando: 'Arrancando la sesión…',
        libre: 'Sesión activa: los bloques comparten variables, como las celdas de un cuaderno.',
        ocupado: 'Ejecutando…',
        perdido: 'La sesión se cerró. Al ejecutar un bloque se abre una nueva.',
      };
      this.barraEl.dataset.estado = e;
      this.barraEl.querySelector('.lab-texto').innerHTML = textos[e] || '';
      const sinNucleo = !this.nucleo;
      this.barraEl.querySelector('[data-lab="interrumpir"]').disabled = sinNucleo || e !== 'ocupado';
      this.barraEl.querySelector('[data-lab="reiniciar"]').disabled = sinNucleo || !['libre', 'ocupado'].includes(e);
      this.barraEl.querySelector('[data-lab="todo"]').disabled = sinNucleo;
    }

    explicarSinLaboratorio(celda) {
      celda.salida.limpiar();
      celda.salida.mostrar();
      const texto = this.nucleo
        ? `No se pudo conectar con el laboratorio. Arráncalo con <code>${ORDEN_LAB}</code> (Docker Desktop en marcha) y vuelve a probar.`
        : `El código se ejecuta en el laboratorio del curso (Jupyter en Docker). Arráncalo con <code>${ORDEN_LAB}</code> o con doble clic en <code>curso.cmd</code> y recarga la página.`;
      celda.salida.aviso(texto, 'celda-aviso celda-aviso-mal');
    }

    /* Encola: ejecutar la celda, y antes las anteriores que no se hayan ejecutado en esta sesión. */
    ejecutarHasta(celda) {
      if (celda.estado !== 'quieta') return this.cola;
      return this.encolar(celda, () => {
        const faltan = this.celdas.slice(0, celda.indice).filter((c) => !c.opcional && c.generacion !== this.nucleo.generacion);
        return [...faltan, celda];
      });
    }

    ejecutarTodo() {
      return this.encolar(this.celdas[0], () => this.celdas.filter((c) => !c.opcional));
    }

    /* elegir() decide la lista cuando ya hay núcleo (necesita su generación); aviso: dónde explicar si no lo hay. */
    encolar(aviso, elegir) {
      const tanda = this.tanda;
      const marcadas = new Set();
      this.cola = this.cola.then(async () => {
        if (tanda !== this.tanda) return;
        if (!this.nucleo) {
          this.explicarSinLaboratorio(aviso);
          return;
        }
        try {
          await this.nucleo.arrancar();
        } catch {
          this.explicarSinLaboratorio(aviso);
          return;
        }
        const lista = elegir();
        lista.forEach((c) => {
          c.ponerEstado('cola');
          marcadas.add(c);
        });
        const objetivo = lista[lista.length - 1];
        const conPreparacion = lista.slice(0, -1).some((c) => c.preparacion);
        const previas = lista.length - 1 - (conPreparacion ? 1 : 0);
        for (const c of lista) {
          if (tanda !== this.tanda) break;
          marcadas.delete(c);
          const ok = await c.ejecutar();
          if (!ok) break;
        }
        marcadas.forEach((c) => {
          c.ponerEstado('quieta');
          if (c.generacion !== this.nucleo.generacion) c.contador.textContent = '[ ]';
        });
        if (lista.length > 1 && objetivo.generacion === this.nucleo.generacion) {
          const bloques = previas === 1 ? 'el bloque anterior, que faltaba' : `los ${previas} bloques anteriores que faltaban`;
          let texto;
          if (!previas) texto = 'Antes se ejecutó la preparación de la lección.';
          else if (conPreparacion) texto = `Antes se ejecutaron la preparación de la lección y ${bloques}.`;
          else texto = `Antes se ${previas === 1 ? 'ejecutó' : 'ejecutaron'} ${bloques}.`;
          objetivo.salida.aviso(texto, 'celda-aviso celda-sin-salida');
        }
      });
      return this.cola;
    }

    async interrumpir() {
      if (!this.nucleo) return;
      this.tanda += 1;
      try {
        await this.nucleo.interrumpir();
      } catch {
        /* si no hay núcleo, no hay nada que interrumpir */
      }
    }

    async reiniciar() {
      if (!this.nucleo) return;
      this.tanda += 1;
      try {
        await this.nucleo.reiniciar();
        this.celdas.forEach((c) => c.olvidar());
      } catch {
        this.estado('apagado');
      }
    }
  }

  window.Laboratorio = {
    activar(raiz, meta) {
      if (!raiz.querySelector('[data-celda]')) return null;
      return new Laboratorio(raiz, meta);
    },
  };
})();
