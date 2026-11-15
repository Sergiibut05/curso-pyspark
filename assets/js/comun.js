/*
 * Piezas comunes a la portada y a las lecciones: tema, progreso (localStorage, opcional),
 * barra superior y el motor que convierte el Markdown de una lección en HTML con cajas,
 * ejercicios, preguntas de repaso, bloques «pruébalo» con su salida y simuladores.
 */
(function () {
  'use strict';

  const CURSO = window.CURSO || { id: 'curso', titulo: 'Curso' };
  const LECCIONES = window.INDICE || [];

  /* ── Almacenamiento tolerante (modo privado, file://, bloqueos) ── */
  const almacen = {
    leer(clave, defecto) {
      try {
        const v = localStorage.getItem(clave);
        return v === null ? defecto : JSON.parse(v);
      } catch {
        return defecto;
      }
    },
    guardar(clave, valor) {
      try {
        localStorage.setItem(clave, JSON.stringify(valor));
      } catch {
        /* sin almacenamiento: el progreso dura lo que la pestaña */
      }
    },
  };

  /* ── Tema ── */
  const CLAVE_TEMA = 'cursos-observatorio:tema';
  function temaInicial() {
    const guardado = almacen.leer(CLAVE_TEMA, null);
    if (guardado === 'noche' || guardado === 'dia') return guardado;
    return window.matchMedia && matchMedia('(prefers-color-scheme: dark)').matches ? 'noche' : 'dia';
  }
  function aplicarTema(tema) {
    document.documentElement.dataset.tema = tema;
    document.querySelectorAll('.boton-tema').forEach((b) => {
      b.setAttribute('aria-label', tema === 'noche' ? 'Cambiar a tema día' : 'Cambiar a tema noche');
      b.innerHTML = tema === 'noche' ? ICONO_SOL : ICONO_LUNA;
    });
  }
  const ICONO_LUNA =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>';
  const ICONO_SOL =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>';
  document.documentElement.dataset.tema = temaInicial();

  /* ── Progreso ── */
  const CLAVE_HECHAS = CURSO.id + ':hechas';
  const progreso = {
    hechas() {
      const lista = almacen.leer(CLAVE_HECHAS, []);
      return new Set(Array.isArray(lista) ? lista : []);
    },
    marcar(slug, hecha) {
      const s = progreso.hechas();
      if (hecha) s.add(slug);
      else s.delete(slug);
      almacen.guardar(CLAVE_HECHAS, [...s]);
      document.dispatchEvent(new CustomEvent('progreso'));
    },
    resumen() {
      const s = progreso.hechas();
      const n = LECCIONES.filter((l) => s.has(l.slug)).length;
      return { hechas: n, total: LECCIONES.length };
    },
  };

  function pintarProgresoBarra() {
    const { hechas, total } = progreso.resumen();
    document.querySelectorAll('[data-progreso-texto]').forEach((el) => {
      el.textContent = `${hechas} de ${total}`;
    });
    document.querySelectorAll('[data-progreso-barra]').forEach((el) => {
      el.max = total || 1;
      el.value = hechas;
    });
  }

  /* ── Utilidades ── */
  function escapar(texto) {
    return String(texto)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }
  function slugificar(texto) {
    return String(texto)
      .toLowerCase()
      .normalize('NFD')
      .replace(/[̀-ͯ]/g, '')
      .replace(/<[^>]+>/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 60);
  }
  function inline(texto) {
    if (window.marked) return marked.parseInline(texto);
    return escapar(texto);
  }

  /* ── Motor de Markdown ──
   * Antes de marked se convierten los bloques «:::» en HTML (dejando líneas en blanco para
   * que marked siga interpretando el Markdown de dentro). Los bloques de código especiales
   * (quiz, widget, salida, «pruébalo») los resuelve el renderizador de código.
   */
  function preprocesar(md) {
    const lineas = md.replace(/\r\n/g, '\n').split('\n');
    const salida = [];
    const pila = [];
    let valla = null;
    for (const linea of lineas) {
      const v = linea.match(/^\s*(`{3,}|~{3,})/);
      if (v) {
        if (!valla) valla = v[1];
        else if (linea.trim().startsWith(valla) && linea.trim().replace(/[`~]/g, '') === '') valla = null;
        salida.push(linea);
        continue;
      }
      if (valla) {
        salida.push(linea);
        continue;
      }
      const abre = linea.match(/^:::(nota|cuidado|defensa|repo|resumen|ejercicio)\s*(.*)$/);
      if (abre) {
        const tipo = abre[1];
        const titulo = abre[2].trim();
        pila.push(tipo);
        if (tipo === 'ejercicio') {
          salida.push(
            '',
            `<section class="ejercicio"><div class="ejercicio-cabecera"><span class="etiqueta-ejercicio">Práctica</span><h3>${inline(titulo || 'Ejercicio')}</h3></div><div class="ejercicio-cuerpo">`,
            '',
          );
        } else {
          const porDefecto = {
            nota: 'Nota',
            cuidado: 'Cuidado',
            defensa: 'Para la defensa',
            repo: 'En este repositorio',
            resumen: 'En resumen',
          }[tipo];
          salida.push(
            '',
            `<div class="caja caja-${tipo}"><p class="caja-titulo">${inline(titulo || porDefecto)}</p>`,
            '',
          );
        }
        continue;
      }
      const sol = linea.match(/^:::(solucion|pista)\s*(.*)$/);
      if (sol) {
        const texto = sol[2].trim() || (sol[1] === 'pista' ? 'Ver una pista' : 'Ver la solución');
        if (pila[pila.length - 1] === 'detalle') {
          salida.push('', '</div></details>', '');
          pila.pop();
        }
        pila.push('detalle');
        salida.push('', `<details><summary>${escapar(texto)}</summary><div>`, '');
        continue;
      }
      if (/^:::\s*$/.test(linea)) {
        let tipo = pila.pop();
        if (tipo === 'detalle') {
          salida.push('', '</div></details>', '');
          tipo = pila.pop();
        }
        if (tipo === 'ejercicio') salida.push('', '</div></section>', '');
        else if (tipo) salida.push('', '</div>', '');
        continue;
      }
      salida.push(linea);
    }
    while (pila.length) {
      const tipo = pila.pop();
      salida.push('', tipo === 'detalle' ? '</div></details>' : tipo === 'ejercicio' ? '</div></section>' : '</div>', '');
    }
    return salida.join('\n');
  }

  function analizarInfo(info) {
    // «python pruebalo titulo="backend/x.py"» → { lengua, pruebalo, titulo }
    const r = { lengua: '', pruebalo: false, titulo: '' };
    if (!info) return r;
    const t = info.match(/titulo="([^"]*)"/);
    if (t) r.titulo = t[1];
    const resto = info.replace(/titulo="[^"]*"/, '').trim().split(/\s+/);
    r.lengua = resto[0] || '';
    r.pruebalo = resto.includes('pruebalo');
    return r;
  }

  const NOMBRES_LENGUA = {
    powershell: 'PowerShell',
    bash: 'bash',
    sh: 'shell',
    python: 'Python',
    py: 'Python',
    json: 'JSON',
    yaml: 'YAML',
    sql: 'SQL',
    text: 'texto',
    properties: 'properties',
    pyspark: 'PySpark',
  };

  function bloqueCodigo(texto, info) {
    const { lengua, pruebalo, titulo } = analizarInfo(info);
    const lenguaHl = lengua === 'pyspark' ? 'python' : lengua === 'sh' ? 'bash' : lengua;
    let html = escapar(texto);
    if (window.hljs && lenguaHl && hljs.getLanguage(lenguaHl)) {
      try {
        html = hljs.highlight(texto, { language: lenguaHl, ignoreIllegals: true }).value;
      } catch {
        /* sin resaltado */
      }
    }
    const etiqueta = titulo || (pruebalo ? 'Pruébalo' : '');
    return (
      `<div class="codigo${pruebalo ? ' pruebalo' : ''}">` +
      `<div class="codigo-cabecera"><span class="codigo-titulo">${escapar(etiqueta)}</span>` +
      `<span class="codigo-tipo">${escapar(NOMBRES_LENGUA[lengua] || lengua || '')}</span>` +
      `<button type="button" class="copiar" data-copiar>Copiar</button></div>` +
      `<pre><code class="hljs">${html}</code></pre></div>`
    );
  }

  function bloqueSalida(texto, info) {
    const t = (info.match(/titulo="([^"]*)"/) || [])[1] || 'Salida real';
    return `<div class="salida"><div class="salida-cabecera">${escapar(t)}</div><pre>${escapar(texto)}</pre></div>`;
  }

  let contadorQuiz = 0;
  function bloqueQuiz(texto) {
    const lineas = texto.split('\n').filter((l) => l.trim() !== '');
    const pregunta = [];
    const opciones = [];
    const explicacion = [];
    for (const l of lineas) {
      if (/^[-*] /.test(l)) opciones.push({ texto: l.slice(2).trim(), correcta: l[0] === '*' });
      else if (/^> ?/.test(l)) explicacion.push(l.replace(/^> ?/, ''));
      else if (!opciones.length) pregunta.push(l);
      else explicacion.push(l);
    }
    contadorQuiz += 1;
    const letras = 'ABCDEFG';
    const botones = opciones
      .map(
        (o, i) =>
          `<button type="button" class="quiz-opcion" data-correcta="${o.correcta ? '1' : '0'}"><span class="letra">${letras[i]}</span><span>${inline(o.texto)}</span></button>`,
      )
      .join('');
    return (
      `<div class="quiz" data-quiz="${contadorQuiz}"><div class="quiz-etiqueta">Comprueba</div>` +
      `<p class="quiz-pregunta">${inline(pregunta.join(' '))}</p>` +
      `<div class="quiz-opciones">${botones}</div>` +
      `<div class="quiz-explicacion" hidden>${inline(explicacion.join(' '))}</div></div>`
    );
  }

  function renderizar(md) {
    contadorQuiz = 0;
    const fuente = preprocesar(md);
    if (!window.marked) {
      return `<div class="caja caja-cuidado"><p class="caja-titulo">No se pudo cargar el intérprete de Markdown</p><p>Sin conexión a la CDN se muestra el texto tal cual.</p></div><pre>${escapar(md)}</pre>`;
    }
    const renderer = {
      code(token) {
        const info = (token.lang || '').trim();
        const tipo = info.split(/\s+/)[0];
        if (tipo === 'quiz') return bloqueQuiz(token.text);
        if (tipo === 'salida') return bloqueSalida(token.text, info);
        if (tipo === 'widget') {
          const nombre = token.text.trim().split(/\s+/)[0];
          return `<div class="widget" data-widget="${escapar(nombre)}"><p class="cargando">Cargando simulador…</p></div>`;
        }
        return bloqueCodigo(token.text, info);
      },
    };
    marked.use({ renderer, gfm: true, breaks: false });
    return marked.parse(fuente);
  }

  /* Interacciones del contenido renderizado (delegadas) */
  function activarContenido(raiz) {
    raiz.addEventListener('click', (ev) => {
      const copiar = ev.target.closest('[data-copiar]');
      if (copiar) {
        const codigo = copiar.closest('.codigo').querySelector('code').innerText;
        const listo = () => {
          copiar.textContent = 'Copiado';
          copiar.classList.add('hecho');
          setTimeout(() => {
            copiar.textContent = 'Copiar';
            copiar.classList.remove('hecho');
          }, 1600);
        };
        if (navigator.clipboard) navigator.clipboard.writeText(codigo).then(listo, () => {});
        return;
      }
      const opcion = ev.target.closest('.quiz-opcion');
      if (opcion && !opcion.disabled) {
        const quiz = opcion.closest('.quiz');
        const acierto = opcion.dataset.correcta === '1';
        quiz.querySelectorAll('.quiz-opcion').forEach((b) => {
          b.disabled = true;
          if (b.dataset.correcta === '1') b.classList.add('correcta');
        });
        if (!acierto) opcion.classList.add('incorrecta');
        const exp = quiz.querySelector('.quiz-explicacion');
        exp.innerHTML = `<strong>${acierto ? 'Correcto.' : 'No exactamente.'}</strong> ` + exp.innerHTML;
        exp.hidden = false;
      }
    });
    raiz.querySelectorAll('.widget[data-widget]').forEach((el) => {
      const fabrica = (window.WIDGETS || {})[el.dataset.widget];
      el.innerHTML = '';
      if (fabrica) {
        try {
          fabrica(el);
        } catch (e) {
          el.innerHTML = `<p class="cargando">El simulador «${escapar(el.dataset.widget)}» falló: ${escapar(e.message)}</p>`;
        }
      } else {
        el.innerHTML = `<p class="cargando">Simulador «${escapar(el.dataset.widget)}» no disponible.</p>`;
      }
    });
  }

  /* ── Barra común ── */
  function montarBarra() {
    document.querySelectorAll('.boton-tema').forEach((b) =>
      b.addEventListener('click', () => {
        const nuevo = document.documentElement.dataset.tema === 'noche' ? 'dia' : 'noche';
        almacen.guardar(CLAVE_TEMA, nuevo);
        aplicarTema(nuevo);
      }),
    );
    aplicarTema(document.documentElement.dataset.tema);
    pintarProgresoBarra();
    document.addEventListener('progreso', pintarProgresoBarra);
  }

  function etiquetaPractica(valor) {
    return valor === 'si' || valor === true ? CURSO.practica || 'Con la pila en marcha' : String(valor);
  }

  window.Curso = {
    CURSO,
    etiquetaPractica,
    LECCIONES,
    almacen,
    progreso,
    escapar,
    slugificar,
    renderizar,
    activarContenido,
    montarBarra,
  };
})();
