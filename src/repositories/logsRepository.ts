import { db } from '../db/client.js';
import { logs, type NewLogEntry } from '../db/schema.js';

export async function insertLogs(entries: NewLogEntry[]): Promise<void> {
  if (entries.length === 0) return;
  await db.insert(logs).values(entries);
}