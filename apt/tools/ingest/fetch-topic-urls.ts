// Path: apt/tools/ingest/fetch-topic-urls.ts
// Purpose: Fetches Medium RSS, parses URLs, and dumps to GCS raw bucket
// Idempotent: true
// Dependencies: @google-cloud/storage, fast-xml-parser, got-scraping, dotenv

import { Storage } from '@google-cloud/storage';
import { XMLParser } from 'fast-xml-parser';
import { gotScraping } from 'got-scraping';
import * as dotenv from 'dotenv';

dotenv.config();

const storage = new Storage();
const bucketName = process.env.BRONZE_BUCKET;

if (!bucketName) {
  console.error('[ERROR] BRONZE_BUCKET not set');
  process.exit(1);
}

const args = process.argv.slice(2);
const topicFlagIndex = args.indexOf('--topic');
if (topicFlagIndex === -1 || !args[topicFlagIndex + 1]) {
  console.error('[ERROR] Must provide --topic <topic-slug>');
  process.exit(1);
}

const topic = args[topicFlagIndex + 1];
const targetUrl = `https://medium.com/feed/tag/${topic}`;

const dateFlagIndex = args.indexOf('--date');
const today = (dateFlagIndex !== -1 && args[dateFlagIndex + 1]) 
  ? args[dateFlagIndex + 1] 
  : new Date().toISOString().split('T')[0];

async function fetchFeed(url: string, retries = 0): Promise<string> {
  try {
    const { body } = await gotScraping.get({ url, timeout: { request: 10000 } });
    return body as string;
  } catch (err: any) {
    if (retries < 3) {
      const delay = Math.pow(2, retries) * 1000;
      console.log(`[WARN] RSS fetch failed, retrying in ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
      return fetchFeed(url, retries + 1);
    }
    throw err;
  }
}

async function uploadToGcs(path: string, data: any) {
  const bucket = storage.bucket(bucketName!);
  const file = bucket.file(path);
  await file.save(JSON.stringify(data, null, 2), { contentType: 'application/json' });
  console.log(`[SUCCESS] Uploaded to gs://${bucketName}/${path}`);
}

async function run() {
  try {
    const xmlData = await fetchFeed(targetUrl);
    const parser = new XMLParser();
    const jsonObj = parser.parse(xmlData);

    const items = jsonObj.rss?.channel?.item;
    if (!items || items.length === 0) {
      console.error('[ERROR] Feed returned empty');
      await uploadToGcs(`errors/${topic}/${today}/rss-empty.json`, { error: 'Feed empty or failed to parse', url: targetUrl });
      process.exit(1);
    }

    const itemArray = Array.isArray(items) ? items : [items];
    const urls = itemArray.map((i: any) => i.link).filter(Boolean);

    await uploadToGcs(`raw/topic-urls/${topic}/${today}/urls.json`, { urls });
  } catch (error: any) {
    console.error('[ERROR] Fatal feed fetch execution:', error.message);
    await uploadToGcs(`errors/${topic}/${today}/rss-fatal.json`, { error: error.message });
    process.exit(1);
  }
}

run();
