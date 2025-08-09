import Redis from 'ioredis';
import AWS from 'aws-sdk';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import os from 'os';
import dotenv from 'dotenv';
dotenv.config();

const redis = new Redis(process.env.REDIS_URL);
const s3 = new AWS.S3({
  endpoint: process.env.MINIO_ENDPOINT,
  accessKeyId: process.env.MINIO_ACCESS_KEY,
  secretAccessKey: process.env.MINIO_SECRET_KEY,
  s3ForcePathStyle: true,
  signatureVersion: 'v4'
});
const BUCKET = process.env.MINIO_BUCKET;

async function processVideo(job) {
  const { batchId, key, start, end } = job;
  const tmpIn = path.join(os.tmpdir(), path.basename(key));
  const tmpOut = tmpIn.replace(/\.[^/.]+$/, '') + '_cut.mp4';

  const obj = await s3.getObject({ Bucket: BUCKET, Key: key }).promise();
  fs.writeFileSync(tmpIn, obj.Body);

  await new Promise((resolve, reject) => {
    const ff = spawn('ffmpeg', ['-i', tmpIn, '-ss', String(start), '-to', String(end), '-c:v', 'libx264', '-c:a', 'aac', tmpOut]);
    ff.on('close', code => code === 0 ? resolve() : reject());
  });

  const outKey = `processed/${batchId}/${path.basename(tmpOut)}`;
  await s3.putObject({ Bucket: BUCKET, Key: outKey, Body: fs.readFileSync(tmpOut) }).promise();
  await redis.rpush(`batch:${batchId}:results`, `${process.env.MINIO_ENDPOINT}/${BUCKET}/${outKey}`);

  fs.unlinkSync(tmpIn);
  fs.unlinkSync(tmpOut);

  const total = await redis.llen(`batch:${batchId}:results`);
  const queued = await redis.lrange('video:queue', 0, -1);
  const still = queued.filter(j => JSON.parse(j).batchId === batchId).length;
  if (still === 0) {
    await redis.hset(`batch:${batchId}`, 'status', 'done');
    await redis.lpush('zip:queue', batchId);
  }
}

async function loop() {
  while (true) {
    const jobStr = await redis.brpop('video:queue', 0);
    const job = JSON.parse(jobStr[1]);
    await processVideo(job);
  }
}

loop();
