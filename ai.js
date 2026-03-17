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

  function buildSystemPrompt(context = {}) {
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
5. Fecha en formato YYYY-MM-DD. Si falta el anio, asume ${new Date().getFullYear()}.
6. "description" debe ser corta, humana y util, maximo 6 palabras.
7. "accountHint" debe ser una sugerencia textual breve usando estas cuentas si aplica:
${accounts || '- Cuenta principal (bank)'}
8. "debtHint" solo si el texto menciona una deuda o cuota; si no, vacio.
9. "confidence" entre 0 y 1.
10. Si el monto no es confiable devuelve 0.

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
          detail: 'low'
        }
      });
    }

    const payload = {
      model: MODEL,
      response_format: { type: 'json_object' },
      max_tokens: 400,
      messages: [
        { role: 'system', content: systemPrompt },
        {
          role: 'user',
          content: userContent
        }
      ]
    };

    const response = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      const errorMsg = errorData?.error?.message || `Error HTTP ${response.status}`;

      if (response.status === 401) {
        throw new Error('API Key invalida o expirada. Revisala en Ajustes.');
      }
      if (response.status === 429) {
        throw new Error('OpenAI esta limitando la velocidad. Intenta de nuevo en unos segundos.');
      }
      if (response.status === 402 || errorMsg.toLowerCase().includes('quota')) {
        throw new Error('Tu cuenta de OpenAI no tiene creditos disponibles.');
      }

      throw new Error(`Error de OpenAI: ${errorMsg}`);
    }

    const data = await response.json();
    const content = data?.choices?.[0]?.message?.content;
    if (!content) {
      throw new Error('La IA no devolvio contenido util.');
    }

    return parseDraft(content, context, source.sourceType);
  }

  function buildUserPrompt(source) {
    if (source.kind === 'image') {
      return `Analiza esta imagen y sugiere un borrador financiero. Fuente: ${source.sourceType}.`;
    }

    return `Analiza este texto y sugiere un borrador financiero.

Fuente: ${source.sourceType}
Texto:
${String(source.text || '').slice(0, 12000)}`;
  }

  function parseDraft(rawContent, context, sourceType) {
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
    const fallbackCategory = sourceType === 'text-email' ? 'other-expense' : 'other-expense';
    const suggestedType = ALLOWED_TYPES.includes(parsed.suggestedType)
      ? parsed.suggestedType
      : DEFAULT_TYPE;
    const date = normalizeDate(parsed.date);
    const amount = roundAmount(parsed.amount);
    const categoryId = categoryIds.has(parsed.categoryId)
      ? parsed.categoryId
      : inferCategoryId(parsed.categoryId, context.categories) || fallbackCategory;

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
      notes: String(parsed.notes || '').trim().slice(0, 300),
      sourceType
    };
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
    return new Date().toISOString().slice(0, 10);
  }

  async function prepareSourceFromFile(file) {
    if (!file) throw new Error('Selecciona un archivo primero');

    if (file.type.startsWith('image/')) {
      const imageData = await fileToBase64(file);
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

  async function extractPdfText(file) {
    const buffer = await file.arrayBuffer();
    const decoded = new TextDecoder('latin1').decode(buffer);
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
    extractPdfText,
    parseDraft
  };
})();
