/* ============================================
   CONTROL DE FINANZAS LOCAL - AI Assistant
   Drafts transaction suggestions from images
   and pasted/extracted text.
   ============================================ */

const FinanceAI = (() => {
  const API_URL = 'https://api.openai.com/v1/chat/completions';
  const MODEL = 'gpt-4o-mini';

  const DEFAULT_TYPE = 'expense';
  const ALLOWED_TYPES = ['expense', 'income'];
  const MAX_IMAGE_EDGE = 1600;

  function localToday() {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
  }

  function buildSystemPrompt(context = {}) {
    const today = localToday();
    const categories = (context.categories || [])
      .map((item) => `- ${item.id}: ${item.name}`)
      .join('\n');

    const accounts = (context.accounts || [])
      .map((item) => `- ${item.name} (${item.kind})`)
      .join('\n');

    const debts = (context.debts || [])
      .map((item) => `- ${item.name}`)
      .join('\n');

    return `Eres un asistente financiero que solo devuelve JSON valido.

Tu tarea es analizar una evidencia de movimiento financiero local (captura, texto pegado de correo o texto extraido de PDF) y sugerir un borrador para registrar el movimiento.

Reglas:
1. Devuelve solo JSON.
2. Usa exclusivamente una de estas categorias por id:
${categories || '- other-expense: Otros gastos'}
3. Usa solo estos tipos: ${ALLOWED_TYPES.join(', ')}.
4. Si parece un pago, compra o salida usa "expense". Si parece deposito, abono, sueldo o reembolso a favor usa "income".
5. Si el usuario envio dinero, por ejemplo "plinleaste a", "plineaste a", "yapeaste a", "yapear a", "enviaste a", "transferiste a", "mandaste a", entonces es "expense" y en "notes" debe decir "Transferencia enviada". Si el usuario recibio dinero, por ejemplo "te plinearon", "recibiste", "abono recibido", "deposito recibido", entonces es "income" y en "notes" debe decir "Transferencia recibida". Nunca contradigas el verbo principal.
6. Verifica la fecha con mucho cuidado. Usa solo la fecha real visible en la evidencia. No confundas fecha de operacion con hora, fecha actual, fecha del archivo o fecha de otro movimiento. Si la evidencia esta en espanol y usa barras, interpreta DD/MM/YYYY y nunca MM/DD/YYYY. Si falta el anio, asume ${localToday().slice(0, 4)}. Si la fecha no es clara, usa ${today} y en "notes" empieza con "Fecha no clara".
7. "description" debe ser corta, humana y util, maximo 6 palabras.
8. "accountHint" debe ser una sugerencia textual breve usando estas cuentas si aplica:
${accounts || '- Cuenta principal (bank)'}
9. La descripcion debe respetar la direccion del movimiento. Ejemplo: "Plin a Angie" es salida y no debe mapearse como ingreso.
10. "debtHint" solo si el texto menciona una deuda o cuota; si no, vacio.
11. "confidence" entre 0 y 1.
12. Si el monto no es confiable devuelve 0.

Formato exacto:
{
  "suggestedType": "expense|income",
  "amount": 0,
  "date": "YYYY-MM-DD",
  "description": "texto corto",
  "categoryId": "id-categoria",
  "accountHint": "texto breve",
  "debtHint": "texto breve",
  "confidence": 0.0,
  "notes": "detalle breve opcional"
}

Deudas registradas:
${debts || '- ninguna'}`;
  }

  async function analyzeSource(source, apiKey, context = {}) {
    if (!apiKey || !apiKey.startsWith('sk-')) {
      throw new Error('API Key de OpenAI invalida. Debe empezar con "sk-".');
    }

    const systemPrompt = buildSystemPrompt(context);
    const userContent = [{ type: 'text', text: buildUserPrompt(source) }];

    if (source.kind === 'image') {
      userContent.push({
        type: 'image_url',
        image_url: {
          url: `data:${source.mimeType};base64,${source.base64}`,
          // Las boletas y estados de cuenta tienen montos y fechas en letra chica:
          // con "low" la imagen se reduce demasiado y la IA los lee mal.
          detail: 'high'
        }
      });
    }

    const payload = {
      model: MODEL,
      response_format: { type: 'json_object' },
      temperature: 0,
      max_tokens: 400,
      messages: [
        { role: 'system', content: systemPrompt },
        {
          role: 'user',
          content: userContent
        }
      ]
    };

    const data = await requestWithRetry(payload, apiKey);
    const content = data?.choices?.[0]?.message?.content;
    if (!content) {
      throw new Error('La IA no devolvio contenido util.');
    }

    return parseDraft(content, context, source.sourceType, source);
  }

  /* ===== Lectura de estados de cuenta ===== */

  function buildStatementPrompt() {
    return `Eres un lector de estados de cuenta de tarjetas de credito peruanas. Solo devuelves JSON valido.

Tu tarea es extraer TODOS los consumos del estado de cuenta, uno por uno.

Reglas:
1. Devuelve solo JSON, sin texto alrededor.
2. Fechas en formato YYYY-MM-DD. En Peru las fechas con barras son DD/MM/YYYY.
3. "amount" siempre positivo, en la moneda del consumo.
4. Los pagos, abonos y notas de credito NO son consumos: van en "payments".
5. Ignora intereses, portes, membresias y seguros salvo que no puedas distinguirlos; si los incluyes, marcalos en "description".
6. Si el estado separa soles y dolares, usa "currency" con PEN o USD segun corresponda.
7. Si un consumo es en cuotas, pon el numero de cuota en "installment" (ej: "3/12").
8. No inventes movimientos: si no lo lees con claridad, omitelo y reportalo en "unreadable".

Formato exacto:
{
  "cardHint": "banco y ultimos 4 si aparecen",
  "closingDate": "YYYY-MM-DD",
  "dueDate": "YYYY-MM-DD",
  "totalCharged": 0,
  "minimumPayment": 0,
  "movements": [
    { "date": "YYYY-MM-DD", "description": "texto corto", "amount": 0, "currency": "PEN", "installment": "" }
  ],
  "payments": [
    { "date": "YYYY-MM-DD", "description": "texto corto", "amount": 0 }
  ],
  "unreadable": 0
}`;
  }

  async function analyzeStatement(text, apiKey) {
    if (!apiKey || !apiKey.startsWith('sk-')) {
      throw new Error('API Key de OpenAI invalida. Debe empezar con "sk-".');
    }
    if (!String(text || '').trim()) {
      throw new Error('No se pudo leer texto del estado de cuenta.');
    }

    const payload = {
      model: MODEL,
      response_format: { type: 'json_object' },
      temperature: 0,
      max_tokens: 4000,
      messages: [
        { role: 'system', content: buildStatementPrompt() },
        {
          role: 'user',
          content: `Extrae los consumos de este estado de cuenta.\n\n${String(text).slice(0, 40000)}`
        }
      ]
    };

    const data = await requestWithRetry(payload, apiKey);
    const content = data?.choices?.[0]?.message?.content;
    if (!content) throw new Error('La IA no devolvio contenido util.');

    return parseStatement(content);
  }

  function parseStatement(rawContent) {
    let parsed;
    try {
      parsed = JSON.parse(String(rawContent).replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim());
    } catch (error) {
      throw new Error('La IA devolvio un formato invalido al leer el estado de cuenta.');
    }

    const cleanMovement = (item) => ({
      date: normalizeDate(item?.date),
      description: String(item?.description || 'Consumo').trim().slice(0, 80),
      amount: roundAmount(item?.amount),
      currency: item?.currency === 'USD' ? 'USD' : 'PEN',
      installment: String(item?.installment || '').trim().slice(0, 12)
    });

    const movements = Array.isArray(parsed.movements)
      ? parsed.movements.map(cleanMovement).filter((item) => item.amount > 0)
      : [];
    const payments = Array.isArray(parsed.payments)
      ? parsed.payments.map(cleanMovement).filter((item) => item.amount > 0)
      : [];

    return {
      cardHint: String(parsed.cardHint || '').trim().slice(0, 60),
      closingDate: parsed.closingDate ? normalizeDate(parsed.closingDate) : '',
      dueDate: parsed.dueDate ? normalizeDate(parsed.dueDate) : '',
      totalCharged: roundAmount(parsed.totalCharged),
      minimumPayment: roundAmount(parsed.minimumPayment),
      movements,
      payments,
      unreadable: Math.max(0, parseInt(parsed.unreadable, 10) || 0)
    };
  }

  /* ===== Preguntas sobre tus finanzas (solo agregados) ===== */

  async function askAboutFinances(question, aggregates, apiKey) {
    if (!apiKey || !apiKey.startsWith('sk-')) {
      throw new Error('API Key de OpenAI invalida. Debe empezar con "sk-".');
    }
    if (!String(question || '').trim()) {
      throw new Error('Escribe una pregunta.');
    }

    const payload = {
      model: MODEL,
      temperature: 0.2,
      max_tokens: 500,
      messages: [
        {
          role: 'system',
          content: `Eres un asistente que responde preguntas sobre las finanzas personales del usuario, en espanol neutro de Peru, tuteando.

Reglas:
1. Responde SOLO con los datos del resumen que te paso. No inventes cifras.
2. Si el resumen no alcanza para responder, dilo claramente y sugiere que registre mas movimientos.
3. Se breve: maximo 4 frases. Usa montos con dos decimales y el prefijo S/.
4. No des consejos de inversion ni recomiendes productos financieros.
5. Los montos ya vienen totalizados: no tienes el detalle de cada compra.`
        },
        {
          role: 'user',
          content: `Resumen de mis finanzas:\n${JSON.stringify(aggregates, null, 1)}\n\nPregunta: ${String(question).slice(0, 500)}`
        }
      ]
    };

    const data = await requestWithRetry(payload, apiKey);
    const content = data?.choices?.[0]?.message?.content;
    if (!content) throw new Error('La IA no devolvio respuesta.');
    return String(content).trim();
  }

  function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function requestWithRetry(payload, apiKey, attempt = 0) {
    const MAX_ATTEMPTS = 3;
    let response;

    try {
      response = await fetch(API_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify(payload)
      });
    } catch (error) {
      if (attempt + 1 < MAX_ATTEMPTS) {
        await wait(600 * (attempt + 1));
        return requestWithRetry(payload, apiKey, attempt + 1);
      }
      throw new Error('No se pudo conectar con OpenAI. Revisa tu conexion e intenta de nuevo.');
    }

    if (response.ok) return response.json();

    const errorData = await response.json().catch(() => ({}));
    const errorMsg = errorData?.error?.message || `Error HTTP ${response.status}`;

    if (response.status === 401) {
      throw new Error('API Key invalida o expirada. Revisala en Ajustes.');
    }
    if (response.status === 402 || errorMsg.toLowerCase().includes('quota')) {
      throw new Error('Tu cuenta de OpenAI no tiene creditos disponibles.');
    }

    // 429 y 5xx son transitorios: se reintenta con espera creciente.
    const isRetryable = response.status === 429 || response.status >= 500;
    if (isRetryable && attempt + 1 < MAX_ATTEMPTS) {
      const retryAfter = parseFloat(response.headers.get('retry-after') || '0');
      const delay = Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter * 1000
        : 800 * Math.pow(2, attempt);
      await wait(Math.min(delay, 6000));
      return requestWithRetry(payload, apiKey, attempt + 1);
    }

    if (response.status === 429) {
      throw new Error('OpenAI sigue limitando la velocidad. Espera unos segundos y reintenta.');
    }

    throw new Error(`Error de OpenAI: ${errorMsg}`);
  }

  function buildUserPrompt(source) {
    const today = localToday();
    if (source.kind === 'image') {
      return `Analiza esta imagen y sugiere un borrador financiero.

Fuente: ${source.sourceType}
Fecha actual de referencia: ${today}

Antes de responder:
- verifica si el dinero entro o salio del usuario
- verifica la fecha exacta del movimiento con mucho cuidado
- no confundas transferencia enviada con transferencia recibida`;
    }

    return `Analiza este texto y sugiere un borrador financiero.

Fuente: ${source.sourceType}
Fecha actual de referencia: ${today}

Antes de responder:
- verifica si el dinero entro o salio del usuario
- verifica la fecha exacta del movimiento con mucho cuidado
- si la fecha usa barras en espanol, interpretala como DD/MM/YYYY

Texto:
${String(source.text || '').slice(0, 12000)}`;
  }

  function parseDraft(rawContent, context, sourceType, source = null) {
    let cleaned = rawContent.trim();
    cleaned = cleaned.replace(/^```(?:json)?\s*/i, '');
    cleaned = cleaned.replace(/\s*```$/i, '');

    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (error) {
      throw new Error('La IA devolvio un formato invalido. Intenta otra vez.');
    }

    const categoryIds = new Set((context.categories || []).map((item) => item.id));
    const transferDirection = inferTransferDirection([
      parsed.description,
      parsed.notes,
      source?.kind === 'text' ? source.text : ''
    ].join(' '));
    let suggestedType = ALLOWED_TYPES.includes(parsed.suggestedType)
      ? parsed.suggestedType
      : DEFAULT_TYPE;
    if (transferDirection === 'outgoing') {
      suggestedType = 'expense';
    } else if (transferDirection === 'incoming') {
      suggestedType = 'income';
    }
    const fallbackCategory = suggestedType === 'income' ? 'other-income' : 'other-expense';
    const date = normalizeDate(parsed.date);
    const amount = roundAmount(parsed.amount);
    const categoryId = categoryIds.has(parsed.categoryId)
      ? parsed.categoryId
      : inferCategoryId(parsed.categoryId, context.categories) || fallbackCategory;
    let notes = String(parsed.notes || '').trim().slice(0, 300);
    if (transferDirection === 'outgoing') {
      notes = injectDirectionNote(notes, 'Transferencia enviada');
    } else if (transferDirection === 'incoming') {
      notes = injectDirectionNote(notes, 'Transferencia recibida');
    }

    return {
      suggestedType,
      amount,
      date,
      description: String(parsed.description || 'Movimiento detectado')
        .trim()
        .slice(0, 80),
      categoryId,
      accountHint: String(parsed.accountHint || '').trim().slice(0, 60),
      debtHint: String(parsed.debtHint || '').trim().slice(0, 60),
      confidence: normalizeConfidence(parsed.confidence),
      notes,
      sourceType
    };
  }

  function inferTransferDirection(value) {
    const text = normalizeText(value);
    if (!text) return '';

    const outgoingPatterns = [
      /\b(?:plinleaste|plineaste|pinleaste|yapeaste|yapear|yapearle|transferiste|enviaste|mandaste|giraste|pagaste)\b/,
      /\b(?:plin|yape|yapear)\s+a\b/,
      /\btransferencia\s+enviada\b/,
      /\benviaste\s+dinero\b/
    ];
    const incomingPatterns = [
      /\b(?:recibiste|recibido|te\s+plinearon|te\s+yapearon|te\s+enviaron|te\s+depositaron)\b/,
      /\babono\s+recibido\b/,
      /\bdeposito\s+recibido\b/,
      /\btransferencia\s+recibida\b/
    ];

    if (outgoingPatterns.some((pattern) => pattern.test(text))) return 'outgoing';
    if (incomingPatterns.some((pattern) => pattern.test(text))) return 'incoming';
    return '';
  }

  function injectDirectionNote(notes, directionLabel) {
    const clean = String(notes || '').trim();
    if (!clean) return directionLabel;
    if (/transferencia\s+(enviada|recibida)/i.test(clean)) {
      return clean.replace(/transferencia\s+(enviada|recibida)/i, directionLabel).slice(0, 300);
    }
    return `${directionLabel} / ${clean}`.slice(0, 300);
  }

  function normalizeText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  function inferCategoryId(value, categories = []) {
    const needle = String(value || '').trim().toLowerCase();
    if (!needle) return '';
    const match = categories.find(
      (item) =>
        item.id.toLowerCase() === needle ||
        item.name.toLowerCase() === needle
    );
    return match ? match.id : '';
  }

  function normalizeConfidence(value) {
    const parsed = parseFloat(value);
    if (!Number.isFinite(parsed)) return 0;
    return Math.max(0, Math.min(1, Math.round(parsed * 100) / 100));
  }

  function roundAmount(value) {
    const parsed = parseFloat(value);
    if (!Number.isFinite(parsed)) return 0;
    return Math.max(0, Math.round(parsed * 100) / 100);
  }

  function normalizeDate(value) {
    const raw = String(value || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    return localToday();
  }

  async function prepareSourceFromFile(file) {
    if (!file) throw new Error('Selecciona un archivo primero');

    const isHeic = /heic|heif/i.test(file.type) || /\.(heic|heif)$/i.test(file.name);

    if (file.type.startsWith('image/') || isHeic) {
      const imageData = await prepareImagePayload(file);
      return {
        kind: 'image',
        base64: imageData.base64,
        mimeType: imageData.mimeType,
        sourceType: 'image-upload'
      };
    }

    if (file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf')) {
      const text = await extractPdfText(file);
      return {
        kind: 'text',
        text,
        sourceType: 'pdf-text'
      };
    }

    const text = await file.text();
    if (!text.trim()) {
      throw new Error('No se pudo leer texto util del archivo.');
    }

    return {
      kind: 'text',
      text,
      sourceType: 'text-file'
    };
  }

  // OpenAI no acepta HEIC (el formato por defecto del iPhone) y las capturas
  // de pantalla pesan de mas. Se reencodea a JPEG en canvas antes de subir.
  async function prepareImagePayload(file) {
    const needsTranscode = /heic|heif/i.test(file.type) || /\.(heic|heif)$/i.test(file.name);

    if (!needsTranscode && file.size <= 900 * 1024) {
      return fileToBase64(file);
    }

    try {
      const bitmap = await createImageBitmap(file);
      const scale = Math.min(1, MAX_IMAGE_EDGE / Math.max(bitmap.width, bitmap.height));
      const width = Math.max(1, Math.round(bitmap.width * scale));
      const height = Math.max(1, Math.round(bitmap.height * scale));

      const canvas = typeof OffscreenCanvas === 'function'
        ? new OffscreenCanvas(width, height)
        : Object.assign(document.createElement('canvas'), { width, height });

      const context = canvas.getContext('2d');
      context.drawImage(bitmap, 0, 0, width, height);
      bitmap.close?.();

      const blob = canvas.convertToBlob
        ? await canvas.convertToBlob({ type: 'image/jpeg', quality: 0.9 })
        : await new Promise((resolve) => canvas.toBlob(resolve, 'image/jpeg', 0.9));

      if (!blob) throw new Error('sin blob');

      const base64 = await blobToBase64(blob);
      return { base64, mimeType: 'image/jpeg' };
    } catch (error) {
      if (needsTranscode) {
        throw new Error('No pude convertir esta imagen HEIC. Compartela como JPG desde Fotos e intenta de nuevo.');
      }
      return fileToBase64(file);
    }
  }

  async function blobToBase64(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result).split(',')[1]);
      reader.onerror = () => reject(new Error('No se pudo procesar la imagen.'));
      reader.readAsDataURL(blob);
    });
  }

  async function fileToBase64(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const dataUrl = reader.result;
        resolve({
          base64: String(dataUrl).split(',')[1],
          mimeType: file.type || 'image/jpeg'
        });
      };
      reader.onerror = () => reject(new Error('No se pudo leer la imagen seleccionada.'));
      reader.readAsDataURL(file);
    });
  }

  // La mayoria de estados de cuenta traen el texto dentro de streams
  // comprimidos con Flate: sin descomprimirlos, el regex solo ve basura.
  async function inflateStream(bytes) {
    if (typeof DecompressionStream !== 'function') return null;
    for (const format of ['deflate', 'deflate-raw']) {
      try {
        const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream(format));
        const buffer = await new Response(stream).arrayBuffer();
        if (buffer.byteLength) return new Uint8Array(buffer);
      } catch (error) {
        // formato equivocado: se prueba el siguiente
      }
    }
    return null;
  }

  async function expandPdfStreams(bytes) {
    const latin1 = new TextDecoder('latin1');
    const raw = latin1.decode(bytes);
    const pieces = [];
    const streamRegex = /stream\r?\n?/g;
    let match;

    while ((match = streamRegex.exec(raw))) {
      const start = match.index + match[0].length;
      const end = raw.indexOf('endstream', start);
      if (end === -1) continue;

      const header = raw.slice(Math.max(0, match.index - 400), match.index);
      if (!/FlateDecode/.test(header)) continue;

      const inflated = await inflateStream(bytes.subarray(start, end));
      if (inflated) pieces.push(latin1.decode(inflated));
      streamRegex.lastIndex = end;
    }

    return pieces.join('\n');
  }

  let pdfjsPromise = null;

  // pdf.js vive en vendor/ y se carga bajo demanda: son ~2.7 MB que no tienen
  // por que descargarse si nunca analizas un PDF.
  function loadPdfJs() {
    if (!pdfjsPromise) {
      const base = document.baseURI;
      pdfjsPromise = import(new URL('vendor/pdf.min.mjs', base).href).then((lib) => {
        lib.GlobalWorkerOptions.workerSrc = new URL('vendor/pdf.worker.min.mjs', base).href;
        return lib;
      });
    }
    return pdfjsPromise;
  }

  async function extractPdfTextWithPdfJs(file, { password = '', maxPages = 12 } = {}) {
    const pdfjs = await loadPdfJs();
    const data = new Uint8Array(await file.arrayBuffer());

    let doc;
    try {
      doc = await pdfjs.getDocument({ data, password, isEvalSupported: false }).promise;
    } catch (error) {
      // pdf.js usa estos nombres para "falta clave" y "clave incorrecta".
      const name = error?.name || '';
      if (name === 'PasswordException' || /password/i.test(error?.message || '')) {
        throw new Error(
          password
            ? 'La clave no abre este PDF. En los estados de cuenta suele ser tu DNI; revisala en Ajustes.'
            : 'Este PDF esta protegido con clave. Guarda tu DNI en Ajustes o escribe la clave para abrirlo.'
        );
      }
      throw error;
    }

    try {
      const pageCount = Math.min(doc.numPages, maxPages);
      const pages = [];
      for (let index = 1; index <= pageCount; index += 1) {
        const page = await doc.getPage(index);
        const content = await page.getTextContent();
        pages.push(content.items.map((item) => item.str || '').join(' '));
        page.cleanup();
      }
      return pages.join('\n').replace(/\s+/g, ' ').trim().slice(0, 12000);
    } finally {
      await doc.destroy();
    }
  }

  async function extractPdfText(file, options = {}) {
    // pdf.js entiende layouts que el extractor casero no; si falla por lo que
    // sea (sin red la primera vez, PDF raro) se cae al metodo propio.
    try {
      const text = await extractPdfTextWithPdfJs(file, options);
      if (text && text.length >= 40) return text;
    } catch (error) {
      // Si el PDF esta cifrado no hay fallback posible: el extractor casero
      // tampoco puede leerlo, asi que el error del usuario debe sobrevivir.
      if (/clave/i.test(error.message)) throw error;
    }

    return extractPdfTextFallback(file);
  }

  async function extractPdfTextFallback(file) {
    const buffer = await file.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    const plain = new TextDecoder('latin1').decode(buffer);
    const inflated = await expandPdfStreams(bytes);
    const decoded = inflated ? `${plain}\n${inflated}` : plain;
    const chunks = [];

    const textLiteralRegex = /\(([^()]*)\)\s*Tj/g;
    let literalMatch;
    while ((literalMatch = textLiteralRegex.exec(decoded))) {
      const candidate = cleanupPdfText(literalMatch[1]);
      if (candidate.length >= 3) chunks.push(candidate);
    }

    const textArrayRegex = /\[(.*?)\]\s*TJ/g;
    let arrayMatch;
    while ((arrayMatch = textArrayRegex.exec(decoded))) {
      const literals = Array.from(arrayMatch[1].matchAll(/\(([^()]*)\)/g))
        .map((match) => cleanupPdfText(match[1]))
        .filter((item) => item.length >= 2);
      if (literals.length) chunks.push(literals.join(' '));
    }

    if (!chunks.length) {
      const fallback = cleanupPdfText(decoded)
        .replace(/[^A-Za-z0-9@:/.,\-\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

      if (fallback.length < 40) {
        throw new Error('No pude extraer texto legible del PDF. Prueba pegando el texto del correo.');
      }
      return fallback.slice(0, 12000);
    }

    return chunks.join(' ').replace(/\s+/g, ' ').trim().slice(0, 12000);
  }

  function cleanupPdfText(value) {
    return String(value || '')
      .replace(/\\([()\\])/g, '$1')
      .replace(/\\n/g, ' ')
      .replace(/\\r/g, ' ')
      .replace(/\\t/g, ' ')
      .replace(/\\([0-7]{3})/g, (_, octal) => {
        const code = parseInt(octal, 8);
        return Number.isFinite(code) ? String.fromCharCode(code) : ' ';
      });
  }

  return {
    MODEL,
    analyzeSource,
    prepareSourceFromFile,
    fileToBase64,
    prepareImagePayload,
    extractPdfText,
    extractPdfTextWithPdfJs,
    analyzeStatement,
    parseStatement,
    askAboutFinances,
    parseDraft
  };
})();
