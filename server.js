import express from 'express';
import cors from 'cors';
import pkg from 'pg';
import { createClient } from 'redis';
import { v4 as uuidv4 } from 'uuid';

const { Pool } = pkg;
const app = express();

app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.connect().catch(console.error);

async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS videos (
        id UUID PRIMARY KEY,
        user_id VARCHAR(255) NOT NULL,
        title VARCHAR(255) NOT NULL,
        description TEXT,
        script TEXT NOT NULL,
        status VARCHAR(50) DEFAULT 'pending',
        video_url TEXT,
        thumbnail_url TEXT,
        duration INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('Database initialized');
  } catch (error) {
    console.error('DB init error:', error);
  }
}

initDB();

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.post('/api/videos', async (req, res) => {
  try {
    const { title, description, script, userId } = req.body;
    
    if (!script) {
      return res.status(400).json({ error: 'Script is required' });
    }

    const videoId = uuidv4();
    
    await pool.query(
      `INSERT INTO videos (id, user_id, title, description, script, status)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [videoId, userId || 'anonymous', title || 'Untitled', description || '', script, 'pending']
    );

    await redisClient.lPush('video_queue', JSON.stringify({
      videoId,
      script,
      title,
    }));

    res.json({ videoId, status: 'queued' });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to create video' });
  }
});

app.get('/api/videos/:videoId', async (req, res) => {
  try {
    const { videoId } = req.params;
    
    const result = await pool.query(
      'SELECT * FROM videos WHERE id = $1',
      [videoId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Video not found' });
    }

    const video = result.rows[0];
    const jobStatus = await redisClient.get(`job:${videoId}`);
    
    res.json({
      ...video,
      jobStatus: jobStatus ? JSON.parse(jobStatus) : null,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to fetch video' });
  }
});

app.get('/api/videos', async (req, res) => {
  try {
    const { userId } = req.query;
    
    const result = await pool.query(
      'SELECT * FROM videos WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50',
      [userId || 'anonymous']
    );

    res.json(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to fetch videos' });
  }
});

app.post('/api/generate-script', async (req, res) => {
  try {
    const { prompt, duration = 60 } = req.body;
    
    if (!prompt) {
      return res.status(400).json({ error: 'Prompt is required' });
    }

    const script = `[0:00] Introduction\n${prompt}\n\n[0:15] Main Content\nThis is a generated video about: ${prompt}\n\n[0:45] Conclusion\nThank you for watching!`;

    res.json({ script, estimatedDuration: duration });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to generate script' });
  }
});

async function processVideoQueue() {
  while (true) {
    try {
      const job = await redisClient.rPop('video_queue');
      
      if (!job) {
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      const { videoId } = JSON.parse(job);
      
      await pool.query(
        'UPDATE videos SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2',
        ['processing', videoId]
      );

      await redisClient.set(`job:${videoId}`, JSON.stringify({
        status: 'processing',
        progress: 50,
      }));

      await new Promise(resolve => setTimeout(resolve, 3000));

      await pool.query(
        `UPDATE videos SET status = $1, video_url = $2, thumbnail_url = $3, duration = $4, updated_at = CURRENT_TIMESTAMP WHERE id = $5`,
        ['completed', `https://example.com/videos/${videoId}.mp4`, `https://example.com/thumbs/${videoId}.jpg`, 60, videoId]
      );

      await redisClient.set(`job:${videoId}`, JSON.stringify({
        status: 'completed',
        progress: 100,
      }));
    } catch (error) {
      console.error('Queue processing error:', error);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

processVideoQueue().catch(console.error);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Backend running on port ${PORT}`);
});
