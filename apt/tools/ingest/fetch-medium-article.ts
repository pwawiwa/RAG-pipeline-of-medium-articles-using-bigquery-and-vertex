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
const manifestIndex = args.indexOf('--manifest');
const topicIndex = args.indexOf('--topic');

if (topicIndex === -1 || !args[topicIndex + 1]) {
  console.error('[ERROR] Must provide --topic <topic-slug>');
  process.exit(1);
}

if (urlIndex === -1 && manifestIndex === -1) {
  console.error('[ERROR] Must provide either --url <url> or --manifest <gs://path>');
  process.exit(1);
}

const topic = args[topicIndex + 1];
const delayMs = Number(process.env.REQUEST_DELAY_MS ?? 1000);

const dateFlagIndex = args.indexOf('--date');
const today = (dateFlagIndex !== -1 && args[dateFlagIndex + 1]) 
  ? args[dateFlagIndex + 1] 
  : new Date().toISOString().split('T')[0];

function generateSlug(url: string): string {
  const parts = url.split('?')[0].split('/');
  return parts[parts.length - 1] || crypto.createHash('md5').update(url).digest('hex');
}

async function uploadToGcs(path: string, data: any) {
  const bucket = storage.bucket(bucketName!);
  const file = bucket.file(path);
  await file.save(JSON.stringify(data, null, 2), { contentType: 'application/json' });
  console.log(`[SUCCESS] Uploaded to gs://${bucketName}/${path}`);
}

async function scrapeAndUpload(targetUrl: string, topic: string) {
  const urlSlug = generateSlug(targetUrl);
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
    console.warn(`[WARN] Could not check existence in GCS: ${err.message}`);
  }

  console.log(`[INFO] Scraping: ${targetUrl}`);
  try {
    const { body: html } = await gotScraping.get({ url: targetUrl, timeout: { request: 15000 } });
    const $ = cheerio.load(html);

    const title = $('meta[property="og:title"]').attr('content') || $('h1').first().text().trim();
    const author_name = $('meta[name="author"]').attr('content') || $('[data-testid="authorName"]').first().text().trim();
    const published_date = $('meta[property="article:published_time"]').attr('content') || $('time').first().attr('datetime');
    const hero_image_url = $('meta[property="og:image"]').attr('content');
    
    let paragraphs = $('article p').map((i, el) => $(el).text().trim()).get();
    if (paragraphs.length === 0) {
      paragraphs = $('section p').map((i, el) => $(el).text().trim()).get();
    }
    const page_content = paragraphs.join('\n\n').trim();
    const first_line = paragraphs[0] || null;

    const clap_count = $('button[data-testid="headerClapButton"]').text().trim() || null;
    const comments_count = $('button[aria-label="responses"]').text().trim() || null;

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
      author_avatar_url: null,
      ingested_at: new Date().toISOString(),
      content_hash,
      content_gated
    };

    await uploadToGcs(gcsPath, payload);
    // Anti-throttling delay inside the loop
    if (delayMs > 0) await new Promise(r => setTimeout(r, delayMs));

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

async function run() {
  if (urlIndex !== -1) {
    const targetUrl = args[urlIndex + 1];
    await scrapeAndUpload(targetUrl, topic);
  } else if (manifestIndex !== -1) {
    const manifestPath = args[manifestIndex + 1].replace('gs://', '');
    const [bucketPart, ...rest] = manifestPath.split('/');
    const filePath = rest.join('/');
    
    console.log(`[INFO] Loading manifest from gs://${bucketPart}/${filePath}`);
    const [content] = await storage.bucket(bucketPart).file(filePath).download();
    const urls: string[] = JSON.parse(content.toString());
    
    console.log(`[INFO] Found ${urls.length} URLs in manifest.`);
    for (const url of urls) {
      await scrapeAndUpload(url, topic);
    }
  }
}

run();
