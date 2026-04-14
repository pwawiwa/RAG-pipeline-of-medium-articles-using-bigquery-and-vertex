# Medium Article Scraper — Specification

> **Purpose:** Canonical reference for `fetch-medium-article.ts`.
> Read this before writing any code that fetches article content.

---

## Stack

| Library        | Role                        | Install                  |
|----------------|-----------------------------|--------------------------|
| `got-scraping` | HTTP fetch of article HTML  | `npm i got-scraping`     |
| `cheerio`      | jQuery-style DOM parsing    | `npm i cheerio`          |

No headless browser. No Puppeteer. No Playwright. Static HTML only.

---

## Required Request Headers

Medium returns 403 or a login wall without a recognizable browser signature.
Always send requests via `gotScraping`, which handles modern TLS fingerprints:

```ts
// got-scraping handles User-Agent and headers automatically to mimic a real browser
```

---

## Selector Strategy

Medium's HTML has two reliable data sources in `<head>`:
- **Open Graph meta tags** (`og:title`, `og:image`, `article:published_time`)
- **Standard meta tags** (`name="author"`)

These are more stable than class-based body selectors which break when Medium
redesigns. Always try `<head>` selectors first, fall back to body selectors.

### Field-by-field selector priority

**`title`**
```ts
$('meta[property="og:title"]').attr('content')
  ?? $('h1').first().text().trim()
  ?? $('title').text().trim()
  ?? ''
```

**`author_name`**
```ts
$('meta[name="author"]').attr('content')
  ?? $('[data-testid="authorName"]').first().text().trim()
  ?? ''
// JSON-LD fallback: parse <script type="application/ld+json"> → author.name
```

**`page_content`**
```ts
$('article').text().trim()
  ?? $('section p').map((_, el) => $(el).text()).get().join('\n')
  ?? ''
```

**`first_line`**
```ts
$('article p').first().text().trim() ?? ''
```

**`published_date`**
```ts
$('meta[property="article:published_time"]').attr('content')
  // returns ISO string e.g. "2025-09-20T10:00:00.000Z" → parse to DATE
  ?? $('time[datetime]').first().attr('datetime')
  ?? ''
```

**`clap_count`**
```ts
$('[data-testid="clapCount"]').first().text().trim() ?? ''
// Often not in static HTML — empty string is acceptable
```

**`comments_count`**
```ts
$('[data-testid="responsesCount"]').first().text().trim() ?? ''
// Often not in static HTML — empty string is acceptable
```

**`hero_image_url`**
```ts
$('meta[property="og:image"]').attr('content')
  ?? $('figure img').first().attr('src')
  ?? ''
```

**`author_avatar_url`**
```
Omitted in v1.
Requires a separate HTTP request to the author's profile page.
Store as empty string. Do not make an extra HTTP call per article.
```

---

## Paywall Detection

```ts
const content_gated = (pageContent?.trim().length ?? 0) < 200;
```

If `content_gated === true`:
- Store the record in BigQuery with all available fields + nulls
- **Exclude from Vertex AI RAG corpus sync**
- Do not discard the URL — retain for audit

---

## Throttling

Always wait `REQUEST_DELAY_MS` (env var, default `1000`) between article fetches.
Medium will rate-limit or block the scraper IP without a delay.

```ts
await new Promise(res => setTimeout(res, Number(process.env.REQUEST_DELAY_MS ?? 1000)));
```

---

## Error Handling Pattern

```ts
try {
  const { body: data } = await gotScraping.get({ url, timeout: { request: 10000 } });
  const $ = cheerio.load(data);
  // extract fields...
} catch (err) {
  // SOFT FAILURE — do not rethrow, do not exit(1)
  // Write error JSON to GCS: errors/{topic}/{date}/{url_slug}.json
  // Continue to next URL in the batch
  await writeErrorToGcs({ url, error: String(err) });
}
```

Failure on one article must never halt the batch. Exit code 0 even on article errors.

---

## Output Shape (maps to BigQuery schema)

```ts
interface ArticleRecord {
  article_url: string;       // the URL passed in
  topic: string;             // from CLI arg
  title: string | null;
  author_name: string | null;
  page_content: string | null;
  first_line: string | null;
  published_date: string | null;  // ISO date string or null — BQ will parse to DATE
  clap_count: string | null;      // raw string e.g. "1.2k" — do NOT cast to number
  comments_count: string | null;  // raw string e.g. "72"
  hero_image_url: string | null;
  author_avatar_url: null;        // always null in v1
  ingested_at: string;            // new Date().toISOString()
  content_hash: string;           // MD5 hex of page_content
  content_gated: boolean;         // true if page_content.length < 200
}
```

---

## What This Scraper Does NOT Do

| Capability                          | Available? |
|-------------------------------------|------------|
| Topic / keyword discovery           | ❌ — handled by `fetch-topic-urls.ts` via RSS |
| Fetch paywalled article full text   | ❌ — `content_gated = true` for these         |
| Fetch author avatar                 | ❌ — omitted in v1                             |
| JavaScript rendering                | ❌ — static HTML only                          |
| Auto-retry on 429                   | ✅ — implement 3x retry with backoff in got-scraping  |

---

## Full CLI Pattern (as called by KubernetesPodOperator)

```ts
// fetch-medium-article.ts
// Called as: npx ts-node fetch-medium-article.ts --url <url> --topic <topic>

import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';
import * as crypto from 'crypto';

const url = process.argv[process.argv.indexOf('--url') + 1];
const topic = process.argv[process.argv.indexOf('--topic') + 1];

if (!url || !topic) { console.error('Missing --url or --topic'); process.exit(1); }

await new Promise(res => setTimeout(res, Number(process.env.REQUEST_DELAY_MS ?? 1000)));

try {
  const { body: data } = await gotScraping.get({
    url,
    timeout: { request: 10000 },
  });

  const $ = cheerio.load(data);
  const pageContent = $('article').text().trim() || '';
  const content_gated = pageContent.length < 200;
  const content_hash = crypto.createHash('md5').update(pageContent).digest('hex');

  const record = {
    article_url: url,
    topic,
    title: $('meta[property="og:title"]').attr('content') ?? $('h1').first().text().trim() ?? null,
    author_name: $('meta[name="author"]').attr('content') ?? null,
    page_content: pageContent || null,
    first_line: $('article p').first().text().trim() || null,
    published_date: $('meta[property="article:published_time"]').attr('content') ?? null,
    clap_count: $('[data-testid="clapCount"]').first().text().trim() || null,
    comments_count: $('[data-testid="responsesCount"]').first().text().trim() || null,
    hero_image_url: $('meta[property="og:image"]').attr('content') ?? null,
    author_avatar_url: null,
    ingested_at: new Date().toISOString(),
    content_hash,
    content_gated,
  };

  // write record JSON to GCS: raw/articles/{topic}/{date}/{url_slug}.json
  await writeToGcs(record);

} catch (err) {
  await writeErrorToGcs({ url, topic, error: String(err) });
  process.exit(0); // soft failure — do not halt DAG
}
```
