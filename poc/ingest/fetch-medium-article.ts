// Path: poc/ingest/fetch-medium-article.ts
// Purpose: Scrape actual articles listed in urls.json and save to local JSON format, mimicking BQ schema.
// Idempotent: true
// Dependencies: axios, cheerio, crypto

import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

const outDir = path.join(process.cwd(), '..', 'data', 'articles');
const urlsFile = path.join(process.cwd(), '..', 'data', 'urls.json');

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function scrapeArticle(url: string, topic: string) {
  const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 1000);
  await sleep(REQUEST_DELAY_MS);

  console.log(`[INFO] Scraping: ${url}`);
  try {
    const { body: data } = await gotScraping.get({
      url,
      timeout: { request: 10000 },
    });

    const $ = cheerio.load(data);
    const pageContent = $('article').text().trim() || $('section p').map((_, el) => $(el).text()).get().join('\n') || '';
    const content_gated = pageContent.length < 200;
    const content_hash = crypto.createHash('md5').update(pageContent).digest('hex');

    const record = {
      article_url: url,
      topic,
      title: $('meta[property="og:title"]').attr('content') ?? $('h1').first().text().trim() ?? $('title').text().trim() ?? null,
      author_name: $('meta[name="author"]').attr('content') ?? $('[data-testid="authorName"]').first().text().trim() ?? null,
      page_content: pageContent || null,
      first_line: $('article p').first().text().trim() || null,
      published_date: $('meta[property="article:published_time"]').attr('content') ?? $('time[datetime]').first().attr('datetime') ?? null,
      clap_count: $('[data-testid="clapCount"]').first().text().trim() || null,
      comments_count: $('[data-testid="responsesCount"]').first().text().trim() || null,
      hero_image_url: $('meta[property="og:image"]').attr('content') ?? $('figure img').first().attr('src') ?? null,
      author_avatar_url: null,
      ingested_at: new Date().toISOString(),
      content_hash,
      content_gated,
    };

    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    // create a safe slug for the filename
    const slug = url.split('/').pop()?.split('?')[0] || content_hash;
    const recordPath = path.join(outDir, `${slug}.json`);
    
    fs.writeFileSync(recordPath, JSON.stringify(record, null, 2));
    console.log(`[SUCCESS] Saved to ${recordPath}`);

  } catch (err: any) {
    console.log(`[ERROR] Soft failure on ${url}: ${err.message}`);
    const errorDir = path.join(process.cwd(), '..', 'data', 'errors');
    if (!fs.existsSync(errorDir)) {
      fs.mkdirSync(errorDir, { recursive: true });
    }
    const slug = url.split('/').pop()?.split('?')[0] || crypto.randomUUID();
    const errorPath = path.join(errorDir, `${slug}.json`);
    fs.writeFileSync(errorPath, JSON.stringify({ url, topic, error: String(err) }, null, 2));
  }
}

async function main() {
  if (!fs.existsSync(urlsFile)) {
    console.error(`[ERROR] ${urlsFile} not found. Run fetch-topic-urls.ts first.`);
    process.exit(1);
  }

  const urlsData = JSON.parse(fs.readFileSync(urlsFile, 'utf-8'));
  const topic = urlsData.topic || 'unknown';
  const urls: string[] = urlsData.urls || [];

  console.log(`[INFO] Found ${urls.length} URLs to scrape.`);
  
  for (const url of urls) {
    await scrapeArticle(url, topic);
  }
  console.log(`[INFO] Done scraping batch.`);
}

main();
