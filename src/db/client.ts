import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from './schema.js';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20, // max concurrent connections in the pool
});

export const db = drizzle(pool, { schema });
export { pool };