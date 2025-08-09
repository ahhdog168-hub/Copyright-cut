import express from 'express';
import multer from 'multer';
import Redis from 'ioredis';
import AWS from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';
import dotenv from 'dotenv';
dotenv.config();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const redis = new Redis(process.env.REDIS_URL);

const s3 = new AWS.S3({
  endpoint: process.env.MINIO_ENDPOINT,
  accessKeyId: process.env.MINIO_ACCESS_KEY,
  secretAccessKey: process.env.MINIO_SECRET_KEY,
  s3ForcePathStyle: true,
  signatureVersion: 'v4'
});
const BUCKET = process.env.MINIO_BUCKET;

app.use(express.json());
app.use(express.static('public'));

app.post('/upload', upload.array('videos'), async (req, res) => {
  const { start, end, confirmOwnership } = req.body;
  if (confirmOwnership !== 'yes') {
    return res.status(400).json({ error: 'Must confirm ownership rights.' });
  }
  const batchId = uuidv4();
  await redis.hset(`batch:${batchId}`, 'status', 'queued');

  for (const file of req.files) {
    const key = `uploads/${batchId}/${file.originalname}`;
    await s3.putObject({ Bucket: BUCKET, Key: key, Body: file.buffer }).promise();
    await redis.rpush('video:queue', JSON.stringify({
      batchId, key, start: Number(start), end: Number(end)
    }));
  }
  res.json({ batchId });
});

app.get('/status/:batchId', async (req, res) => {
  const info = await redis.hgetall(`batch:${req.params.batchId}`);
  const results = await redis.lrange(`batch:${req.params.batchId}:results`, 0, -1);
  res.json({ ...info, results });
});

app.listen(process.env.PORT, () => {
  console.log(`Backend running on port ${process.env.PORT}`);
});
