// Path: poc/ingest/fetch-topic-urls.ts
// Purpose: Fetch article URLs from a Medium tag RSS feed and save to local JSON.
// Idempotent: true
// Dependencies: axios, fast-xml-parser

import { gotScraping } from 'got-scraping';
import { XMLParser } from 'fast-xml-parser';
import * as fs from 'fs';
import * as path from 'path';

const topic = process.argv[process.argv.indexOf('--topic') + 1] || 'data-engineering';
const MAX_RETRIES = 3;
const feedUrl = `https://medium.com/feed/tag/${topic}`;

const outDir = path.join(process.cwd(), '..', 'data');
const outPath = path.join(outDir, 'urls.json');

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchFeed(url: string, retries: number = 0): Promise<string> {
  try {
    const { body: data } = await gotScraping.get({
      url,
      timeout: { request: 10000 },
    });
    return data;
  } catch (error: any) {
    if (retries < MAX_RETRIES) {
      const waitTime = Math.pow(2, retries) * 1000;
      console.warn(`[WARN] Failed to fetch RSS (attempt ${retries + 1}). Retrying in ${waitTime}ms...`);
      await sleep(waitTime);
      return fetchFeed(url, retries + 1);
    }
    throw new Error(`Failed to fetch RSS after ${MAX_RETRIES} retries: ${error.message}`);
  }
}

async function main() {
  console.log(`[INFO] Fetching topic: ${topic}`);
  try {
    const xmlData = await fetchFeed(feedUrl);
    const parser = new XMLParser();
    const result = parser.parse(xmlData);

    let items = result?.rss?.channel?.item;
    if (!items) {
      console.warn('[WARN] No items found in RSS feed.');
      items = [];
    }
    if (!Array.isArray(items)) {
      items = [items];
    }

    const urls = items.map((item: any) => item.link).filter(Boolean);
    console.log(`[INFO] Found ${urls.length} URLs.`);

    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    fs.writeFileSync(outPath, JSON.stringify({ topic, timestamp: new Date().toISOString(), urls }, null, 2));
    console.log(`[SUCCESS] Wrote URLs to ${outPath}`);

  } catch (error) {
    console.error('[ERROR]', error);
    process.exit(1);
  }
}

main();
