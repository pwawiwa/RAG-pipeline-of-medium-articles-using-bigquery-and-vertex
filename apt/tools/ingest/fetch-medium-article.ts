// Path: apt/tools/ingest/fetch-medium-article.ts
// Purpose: Pulle single Medium article HTML via got-scraping and stores raw fields to GCS
// Idempotent: true
// Dependencies: @google-cloud/storage, cheerio, got-scraping, crypto, dotenv

import { Storage } from '@google-cloud/storage';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';
import * as crypto from 'crypto';
import * as dotenv from 'dotenv';

dotenv.config();

const storage = new Storage();
const bucketName = process.env.BRONZE_BUCKET;

if (!bucketName) {
  console.error('[ERROR] BRONZE_BUCKET not set');
  process.exit(1);
}

const args = process.argv.slice(2);
const urlIndex = args.indexOf('--url');
const topicIndex = args.indexOf('--topic');

if (urlIndex === -1 || topicIndex === -1 || !args[urlIndex + 1] || !args[topicIndex + 1]) {
  console.error('[ERROR] Must provide --url <url> and --topic <topic-slug>');
  process.exit(1);
}

const targetUrl = args[urlIndex + 1];
const topic = args[topicIndex + 1];
const delayMs = Number(process.env.REQUEST_DELAY_MS ?? 1000);
const today = new Date().toISOString().split('T')[0];

function generateSlug(url: string): string {
  const parts = url.split('?')[0].split('/');
  return parts[parts.length - 1] || crypto.createHash('md5').update(url).digest('hex');
}
const urlSlug = generateSlug(targetUrl);

async function uploadToGcs(path: string, data: any) {
  const bucket = storage.bucket(bucketName!);
  const file = bucket.file(path);
  await file.save(JSON.stringify(data, null, 2), { contentType: 'application/json' });
  console.log(`[SUCCESS] Uploaded to gs://${bucketName}/${path}`);
}

async function run() {
  const gcsPath = `raw/articles/${topic}/${today}/${urlSlug}.json`;
  
  try {
    const bucket = storage.bucket(bucketName!);
    const file = bucket.file(gcsPath);
    const [exists] = await file.exists();
    
    if (exists) {
      console.log(`[INFO] Skipping scrape: ${targetUrl} already exists in GCS at ${gcsPath}`);
      return;
    }
  } catch (err: any) {
    console.warn(`[WARN] Could not check existence in GCS (ignoring and proceeding): ${err.message}`);
  }

  console.log(`[INFO] Waiting ${delayMs}ms before scrape...`);
  await new Promise(r => setTimeout(r, delayMs));

  try {
    const { body: html } = await gotScraping.get({ url: targetUrl, timeout: { request: 10000 } });
    const $ = cheerio.load(html);

    // Extraction logic based on scraper-specification.md
    const title = $('meta[property="og:title"]').attr('content') || $('h1').first().text().trim();
    const author_name = $('meta[name="author"]').attr('content') || $('[data-testid="authorName"]').first().text().trim();
    const published_date = $('meta[property="article:published_time"]').attr('content') || $('time').first().attr('datetime');
    const hero_image_url = $('meta[property="og:image"]').attr('content');
    
    // Attempt standard selectors for body content
    let paragraphs = $('article p').map((i, el) => $(el).text().trim()).get();
    if (paragraphs.length === 0) {
      paragraphs = $('section p').map((i, el) => $(el).text().trim()).get();
    }
    const page_content = paragraphs.join('\n\n').trim();
    const first_line = paragraphs[0] || null;

    // Clap block logic
    const clap_count = $('button[data-testid="headerClapButton"]').text().trim() || null;
    const comments_count = $('button[aria-label="responses"]').text().trim() || null;
    const author_avatar_url = null; // Spec marks this as omitted in v1 commonly

    const content_hash = crypto.createHash('sha256').update(page_content).digest('hex');
    const content_gated = page_content.length < 200;

    const payload = {
      article_url: targetUrl,
      topic,
      title,
      author_name,
      page_content,
      first_line,
      published_date,
      clap_count,
      comments_count,
      hero_image_url,
      author_avatar_url,
      ingested_at: new Date().toISOString(),
      content_hash,
      content_gated
    };

    await uploadToGcs(`raw/articles/${topic}/${today}/${urlSlug}.json`, payload);

  } catch (error: any) {
    console.error(`[WARN] Soft failure on ${targetUrl}:`, error.message);
    const errorPayload = {
      url: targetUrl,
      topic,
      error: error.message,
      failed_at: new Date().toISOString()
    };
    await uploadToGcs(`errors/${topic}/${today}/${urlSlug}.json`, errorPayload);
  }
}

run();
