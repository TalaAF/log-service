import Fastify from 'fastify';
import { pool } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { registerHealthRoute } from './routes/health.js';
import { registerLogsRoute } from './routes/logs.js';
import { registerAggregateRoute } from './routes/aggregate.js';

const app = Fastify({ logger: true });

let dbReady = false;

registerHealthRoute(app, () => dbReady);
registerLogsRoute(app);
registerAggregateRoute(app);

/** Waits for Postgres to accept connections; compose's healthcheck can pass a beat early. */
async function waitForDatabase(attempts = 15, delayMs = 1000): Promise<void> {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await pool.query('SELECT 1');
      return;
    } catch (err) {
      if (attempt === attempts) throw err;
      app.log.warn(`database not ready (attempt ${attempt}/${attempts}), retrying in ${delayMs}ms`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

async function start() {
  try {
    await waitForDatabase();

    await runMigrations(pool, (msg) => app.log.info(msg));
    app.log.info('Migrations applied');
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
