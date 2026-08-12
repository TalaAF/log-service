import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { processLogBatch } from '../validation/logEntry.js';
import { insertLogs } from '../repositories/logsRepository.js';



export function registerLogsRoute(app: FastifyInstance) {
  app.post('/logs', handlePostLogs);
}
async function handlePostLogs(request: FastifyRequest, reply: FastifyReply) {
  const body = request.body;

  const isValidShape =
    body != null &&
    typeof body === 'object' &&
    !Array.isArray(body) &&
    Array.isArray((body as Record<string, unknown>).logs);

  if (!isValidShape) {
    return reply.status(400).send({ error: 'request body must be an object with a "logs" array' });
  }

  const rawLogs = (body as Record<string, unknown>).logs as unknown[];
  const { accepted, rejected } = processLogBatch(rawLogs);

  if (accepted.length === 0) {
    return reply.status(400).send({ error: 'all entries were rejected', rejected });
  }

  await insertLogs(accepted);

  return reply.status(200).send({
    accepted: accepted.length,
    rejected,
  });
}

