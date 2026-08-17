import Fastify from 'fastify';
import { aggregatePool, pool, writePool } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { drain } from './ingest/writeBuffer.js';
import { registerHealthRoute } from './routes/health.js';
import { registerLogsRoute } from './routes/logs.js';
import { registerAggregateRoute } from './routes/aggregate.js';
import { registerStatsRoute } from './routes/stats.js';

/**
 * Request logging is off by default, via the log level rather than a flag.
 *
 * Fastify's default configuration writes two JSON lines per request. At the
 * ingest rates this service targets that is close to a thousand stdout writes
 * per second through Docker's logging driver, and it was the single largest
 * consumer of the app container's half CPU — larger than JSON parsing or
 * validation. At 'warn' those lines are below the threshold and are never
 * serialised or written. Warnings and errors still surface, and LOG_LEVEL=info
 * restores per-request detail when debugging.
 */
const logLevel = process.env.LOG_LEVEL ?? 'warn';

const app = Fastify({
  logger: { level: logLevel },
  // A batch of a few thousand entries exceeds Fastify's 1MB default, and
  // rejecting an otherwise valid batch with a 413 would be a correctness
  // failure. Bodies are streamed and freed per request, so this costs nothing
  // at rest.
  bodyLimit: Number(process.env.BODY_LIMIT_BYTES) || 16 * 1024 * 1024,
});

let dbReady = false;

registerHealthRoute(app, () => dbReady);
registerLogsRoute(app);
registerAggregateRoute(app);
registerStatsRoute(app);

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

/**
 * Flushes anything still buffered before the process goes away, so a rolling
 * restart cannot drop rows that a client was told had been accepted.
 */
function installShutdownHandlers() {
  let shuttingDown = false;
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.on(signal, () => {
      if (shuttingDown) return;
      shuttingDown = true;
      void (async () => {
        try {
          await app.close();
          await drain();
        } catch (err) {
          app.log.error(err);
        } finally {
          await Promise.allSettled([pool.end(), aggregatePool.end(), writePool.end()]);
          process.exit(0);
        }
      })();
    });
  }
}

async function start() {
  try {
    await waitForDatabase();

    await runMigrations(pool, (msg) => app.log.warn(msg));
    app.log.warn('Migrations applied');
    dbReady = true;

    installShutdownHandlers();

    const port = Number(process.env.PORT) || 8080;
    await app.listen({ port, host: '0.0.0.0' });
    console.log(`Server listening on port ${port}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();
