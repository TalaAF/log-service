import type { FastifyInstance } from 'fastify';
import { pool } from '../db/client.js';

export function registerHealthRoute(app: FastifyInstance, isReady: () => boolean) {
  app.get('/health', async (request, reply) => {
    if (!isReady()) {
      return reply.status(503).send({ status: 'not ready' });
    }
    return reply.status(200).send({ status: 'ok' });
  });
}