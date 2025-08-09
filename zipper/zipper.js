import Redis from 'ioredis';
import AWS from 'aws-sdk';
import archiver from 'archiver';
import stream from 'stream';
import { v4 as uuidv4 } from 'uuid';
import dotenv from 'dotenv';
dotenv.config();

const redis = new Redis(process.env.REDIS_URL);
const endpoint = process.env.MINIO_ENDPOINT;
const s3 = new AWS.S3({
  endpoint,
  accessKeyId: process.env.MINIO_ACCESS_KEY,
  secretAccessKey: process.env.MINIO_SECRET_KEY,
  s3ForcePathStyle: true,
  signatureVersion: 'v4'
});
const BUCKET = process.env.MINIO_BUCKET;

function s3KeyFromUrl(url) {
  const idx = url.indexOf(`${BUCKET}/`);
  return url.slice(idx + BUCKET.length + 1);
}

function uploadStreamToS3(key) {
  const pass = new stream.PassThrough();
  const uploadPromise = s3.upload({ Bucket: BUCKET, Key: key, Body: pass, ContentType: 'application/zip' }).promise();
  return { writeStream: pass, uploadPromise };
}

async function createZipForBatch(batchId) {
  const results = await redis.lrange(`batch:${batchId}:results`, 0, -1);
  if (!results.length) return;

  const zipKey = `zips/${batchId}/${uuidv4()}.zip`;
  const { writeStream, uploadPromise } = uploadStreamToS3(zipKey);
  const archive = archiver('zip', { zlib: { level: 6 } });

  archive.on('error', err => { throw err; });
  archive.pipe(writeStream);

  for (const url of results) {
    const key = s3KeyFromUrl(url);
    const fileStream = s3.getObject({ Bucket: BUCKET, Key: key }).createReadStream();
    archive.append(fileStream, { name: key.split('/').pop() });
  }

  await archive.finalize();
  await uploadPromise;
  const publicUrl = `${endpoint}/${BUCKET}/${zipKey}`;
  await redis.hset(`batch:${batchId}`, 'zipUrl', publicUrl);
}

async function consumeQueue() {
  while (true) {
    const batchId = await redis.brpoplpush('zip:queue', 'zip:processing', 0);
    await createZipForBatch(batchId);
    await redis.lrem('zip:processing', 0, batchId);
  }
}

consumeQueue();
