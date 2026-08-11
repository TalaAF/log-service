import Fastify from 'fastify';
import { pool } from './db/client.js';
import { registerHealthRoute } from './routes/health.js';

const app = Fastify({ logger: true });

let dbReady = false;

registerHealthRoute(app, () => dbReady);

async function start() {
  try {
    await pool.query('SELECT 1');
    dbReady = true;

    const port = Number(process.env.PORT) || 8080;
    await app.listen({ port, host: '0.0.0.0' });
    console.log(`Server listening on port ${port}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();