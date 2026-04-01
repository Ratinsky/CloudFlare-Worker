const KV_FRESH_MS = 60 * 60 * 1000; // 1 час

function makeNotionHeaders(env) {
  return {
    "Authorization": `Bearer ${env.NOTION_TOKEN}`,
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
  };
}

function extractValue(prop, relCache) {
  if (!prop) return null;
  switch (prop.type) {
    case "title":            return prop.title?.[0]?.plain_text ?? null;
    case "rich_text":        return prop.rich_text?.[0]?.plain_text ?? null;
    case "number":           return prop.number ?? null;
    case "select":           return prop.select?.name ?? null;
    case "multi_select":     return prop.multi_select?.map(s => s.name).join(", ") ?? null;
    case "date":             return prop.date?.start ?? null;
    case "checkbox":         return prop.checkbox ? 1 : 0;
    case "url":              return prop.url ?? null;
    case "email":            return prop.email ?? null;
    case "phone_number":     return prop.phone_number ?? null;
    case "formula":          return extractValue(prop.formula, relCache);
    case "rollup":           return prop.rollup?.number ?? prop.rollup?.array?.length ?? null;
    case "created_time":     return prop.created_time ?? null;
    case "last_edited_time": return prop.last_edited_time ?? null;
    case "relation":
      return prop.relation.map(r => relCache[r.id] || `[${r.id.slice(0,8)}]`).join(", ") || null;
    default: return null;
  }
}

// Полный/инкрементальный fetch из Notion → возвращает { records, savedAt }
// send — опциональная функция для стриминга прогресса клиенту
async function fetchAndSync(DATABASE_ID, env, { labelField, valueField, fullRefresh = false, send } = {}) {
  const notionHeaders = makeNotionHeaders(env);
  const kvKey = `data:${DATABASE_ID}`;

  // Для инкрементального: берём существующие данные и дату последней синхронизации
  let existingRecords = [];
  let since = null;
  if (!fullRefresh) {
    try {
      const cached = await env.NOTION_KV.get(kvKey, "json");
      if (cached?.records && cached?.savedAt) {
        existingRecords = cached.records;
        since = cached.savedAt;
      }
    } catch {}
  }

  // Шаг 1: Получаем схему для резолва relations
  let relatedDbId = null;
  if (labelField) {
    try {
      const r = await fetch(`https://api.notion.com/v1/databases/${DATABASE_ID}`, { headers: notionHeaders });
      if (r.ok) {
        const schema = await r.json();
        const fieldDef = schema.properties?.[labelField];
        if (fieldDef?.type === "relation") relatedDbId = fieldDef.relation?.database_id;
      }
    } catch {}
  }

  // Шаг 2: Пагинация основной базы
  let newResults = [];
  let cursor = null;
  let pageNum = 0;
  do {
    const body = { page_size: 100 };
    if (cursor) body.start_cursor = cursor;
    // Инкрементально: берём только изменённые с последней синхронизации
    if (since) body.filter = { timestamp: "last_edited_time", last_edited_time: { after: since } };

    const r = await fetch(`https://api.notion.com/v1/databases/${DATABASE_ID}/query`, {
      method: "POST", headers: notionHeaders, body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error("Notion API error " + r.status);
    const d = await r.json();
    newResults.push(...d.results);
    cursor = d.has_more ? d.next_cursor : null;
    pageNum++;
    if (send) await send({ type: "progress", page: pageNum, count: existingRecords.length + newResults.length, hasMore: !!cursor });
  } while (cursor);

  // Шаг 3: Резолв relations
  const relCache = {};
  if (relatedDbId) {
    if (send) await send({ type: "resolving", status: "связанная база" });
    let relCursor = null, relCount = 0;
    do {
      const body = { page_size: 100 };
      if (relCursor) body.start_cursor = relCursor;
      const r = await fetch(`https://api.notion.com/v1/databases/${relatedDbId}/query`, {
        method: "POST", headers: notionHeaders, body: JSON.stringify(body),
      });
      if (!r.ok) break;
      const d = await r.json();
      for (const page of d.results) {
        const tp = Object.values(page.properties).find(p => p.type === "title");
        relCache[page.id] = tp?.title?.[0]?.plain_text || `[${page.id.slice(0,8)}]`;
        relCount++;
      }
      relCursor = d.has_more ? d.next_cursor : null;
      if (send) await send({ type: "resolving", status: "связанная база", resolved: relCount });
    } while (relCursor);
  } else {
    const usedFields = [labelField, valueField].filter(Boolean);
    const relationIds = new Set();
    for (const page of newResults) {
      for (const [key, prop] of Object.entries(page.properties)) {
        if (prop.type === "relation" && (usedFields.length === 0 || usedFields.includes(key))) {
          for (const rel of prop.relation) relationIds.add(rel.id);
        }
      }
    }
    if (relationIds.size > 0) {
      if (send) await send({ type: "resolving", status: "страницы", total: relationIds.size });
      await Promise.all([...relationIds].map(async id => {
        try {
          const r = await fetch(`https://api.notion.com/v1/pages/${id}`, { headers: notionHeaders });
          if (r.ok) {
            const p = await r.json();
            const tp = Object.values(p.properties).find(pr => pr.type === "title");
            relCache[id] = tp?.title?.[0]?.plain_text || `[${id.slice(0,8)}]`;
          } else relCache[id] = `[${id.slice(0,8)}]`;
        } catch { relCache[id] = `[${id.slice(0,8)}]`; }
      }));
    }
  }

  // Шаг 4: Строим записи
  const newRecords = newResults.map(page => {
    const row = { _id: page.id };
    for (const [key, prop] of Object.entries(page.properties)) {
      row[key] = extractValue(prop, relCache);
    }
    if (!("Created time" in row))     row["Created time"]     = page.created_time ?? null;
    if (!("Last edited time" in row)) row["Last edited time"] = page.last_edited_time ?? null;
    return row;
  });

  // Шаг 5: Мёрдж с существующими
  let finalRecords;
  if (existingRecords.length > 0 && newRecords.length > 0) {
    const newIds = new Set(newRecords.map(r => r._id));
    // Заменяем изменённые + добавляем новые
    finalRecords = [...existingRecords.filter(r => !newIds.has(r._id)), ...newRecords];
  } else if (newRecords.length > 0) {
    finalRecords = newRecords;
  } else {
    finalRecords = existingRecords; // Ничего не изменилось
  }

  // Шаг 6: Сохраняем в KV
  const savedAt = new Date().toISOString();
  try {
    await env.NOTION_KV.put(kvKey, JSON.stringify({ records: finalRecords, savedAt, count: finalRecords.length }));
  } catch (e) {
    console.error("KV put error:", e.message);
    // Продолжаем даже если KV не записался (например превышен лимит 25MB)
  }

  // Регистрируем базу как "наблюдаемую" для cron
  try {
    const watched = (await env.NOTION_KV.get("watched", "json")) || [];
    if (!watched.includes(DATABASE_ID)) {
      watched.push(DATABASE_ID);
      await env.NOTION_KV.put("watched", JSON.stringify(watched));
    }
  } catch {}

  return { records: finalRecords, savedAt };
}

// Cron: обновляем все наблюдаемые базы
async function syncAllWatched(env) {
  const watched = (await env.NOTION_KV.get("watched", "json")) || [];
  console.log(`Cron: syncing ${watched.length} databases`);
  for (const dbId of watched) {
    try {
      const { records, savedAt } = await fetchAndSync(dbId, env, { fullRefresh: false });
      console.log(`Synced ${dbId}: ${records.length} records, savedAt ${savedAt}`);
    } catch (e) {
      console.error(`Failed to sync ${dbId}:`, e.message);
    }
  }
}

export default {
  async fetch(request, env, ctx) {
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      const url = new URL(request.url);
      const DATABASE_ID = url.searchParams.get("db") || env.DATABASE_ID;
      const notionHeaders = makeNotionHeaders(env);
      const isAdmin = env.ADMIN_PASSWORD && url.searchParams.get("admin") === env.ADMIN_PASSWORD;

      // ── Проверка пароля ───────────────────────────────────────────
      if (url.searchParams.has("checkAdmin")) {
        const provided = url.searchParams.get("checkAdmin");
        const valid = env.ADMIN_PASSWORD && provided === env.ADMIN_PASSWORD;
        return new Response(JSON.stringify({ valid }), {
          headers: { "Content-Type": "application/json", ...corsHeaders },
        });
      }

      // ── Список баз данных ─────────────────────────────────────────
      if (url.searchParams.get("databases") === "1") {
        const r = await fetch("https://api.notion.com/v1/search", {
          method: "POST",
          headers: notionHeaders,
          body: JSON.stringify({ filter: { value: "database", property: "object" }, page_size: 100 }),
        });
        if (!r.ok) return new Response(JSON.stringify({ error: "Notion API error" }), { status: 500, headers: corsHeaders });
        const d = await r.json();
        const databases = d.results.map(db => ({
          id: db.id.replace(/-/g, ""),
          title: db.title?.[0]?.plain_text || "Untitled",
        }));
        return new Response(JSON.stringify(databases), {
          headers: { "Content-Type": "application/json", ...corsHeaders },
        });
      }

      if (!DATABASE_ID) {
        return new Response(JSON.stringify({ error: "Missing 'db' parameter" }), { status: 400, headers: corsHeaders });
      }

      // ── Список полей ──────────────────────────────────────────────
      if (url.searchParams.get("fields") === "1") {
        const r = await fetch(`https://api.notion.com/v1/databases/${DATABASE_ID}/query`, {
          method: "POST", headers: notionHeaders, body: JSON.stringify({ page_size: 1 }),
        });
        if (!r.ok) return new Response(JSON.stringify({ error: "Notion API error" }), { status: 500, headers: corsHeaders });
        const d = await r.json();
        const fields = d.results.length > 0
          ? [
              ...Object.entries(d.results[0].properties).map(([name, prop]) => ({ name, type: prop.type })),
              { name: "Created time", type: "created_time" },
              { name: "Last edited time", type: "last_edited_time" },
            ]
          : [];
        return new Response(JSON.stringify(fields), {
          headers: { "Content-Type": "application/json", ...corsHeaders },
        });
      }

      const labelField = url.searchParams.get("labelField");
      const valueField = url.searchParams.get("valueField");

      // ── Принудительная синхронизация (только для admin) ───────────
      if (url.searchParams.get("sync") === "1") {
        if (!isAdmin) {
          return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401, headers: corsHeaders });
        }
        const { readable, writable } = new TransformStream();
        const writer = writable.getWriter();
        const enc = new TextEncoder();
        const send = async obj => writer.write(enc.encode(JSON.stringify(obj) + "\n"));
        (async () => {
          try {
            const { records, savedAt } = await fetchAndSync(DATABASE_ID, env, {
              labelField, valueField, fullRefresh: true, send,
            });
            await send({ type: "data", records, savedAt });
            await send({ type: "done" });
          } catch (e) {
            await send({ type: "error", message: e.message });
          }
          await writer.close();
        })();
        return new Response(readable, {
          headers: { "Content-Type": "application/x-ndjson", ...corsHeaders },
        });
      }

      // ── Основной endpoint: отдаём из KV ───────────────────────────
      const kvKey = `data:${DATABASE_ID}`;
      const cached = await env.NOTION_KV.get(kvKey, "json");

      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      const enc = new TextEncoder();
      const send = async obj => writer.write(enc.encode(JSON.stringify(obj) + "\n"));

      if (cached?.records) {
        const age = Date.now() - new Date(cached.savedAt).getTime();
        const isStale = age > KV_FRESH_MS;

        // Если данные устарели — обновляем в фоне (не блокируем ответ)
        if (isStale) {
          ctx.waitUntil(fetchAndSync(DATABASE_ID, env, { labelField, valueField, fullRefresh: false }));
        }

        // Фильтрация по since (для инкрементального запроса с фронта)
        const since = url.searchParams.get("since");
        let records = cached.records;
        if (since) {
          records = records.filter(r => {
            const t = r["Last edited time"] || r["Created time"];
            return !t || t > since;
          });
        }

        (async () => {
          await send({ type: "data", records, savedAt: cached.savedAt, fromCache: true, total: cached.records.length });
          await send({ type: "done" });
          await writer.close();
        })();

      } else {
        // KV пуст — запускаем первую синхронизацию в фоне
        ctx.waitUntil(fetchAndSync(DATABASE_ID, env, { labelField, valueField, fullRefresh: true }));

        (async () => {
          await send({ type: "syncing", message: "Идёт первая синхронизация данных. Повторите через 30–60 секунд." });
          await send({ type: "done" });
          await writer.close();
        })();
      }

      return new Response(readable, {
        headers: { "Content-Type": "application/x-ndjson", ...corsHeaders },
      });

    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
    }
  },

  // ── Cron: каждый час обновляем все базы ──────────────────────────
  async scheduled(event, env, ctx) {
    ctx.waitUntil(syncAllWatched(env));
  },
};
