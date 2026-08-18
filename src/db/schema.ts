import { pgTable, bigserial, timestamp, varchar, text, jsonb, index } from 'drizzle-orm/pg-core';

export const logs = pgTable(
  'logs',
  {
    id: bigserial('id', { mode: 'number' }).notNull(),
    timestamp: timestamp('timestamp', { withTimezone: true, mode: 'string' }).notNull(),
    level: varchar('level', { length: 10 }).notNull(),
    service: varchar('service', { length: 255 }).notNull(),
    message: text('message').notNull(),
    attributes: jsonb('attributes').notNull().default({}),
  },
  // Attribute lookups are served by the derived sidecar in
  // 0010_attr_sidecar.sql, not by an index on this table: maintaining a GIN
  // inside the COPY transaction was the single most expensive thing ingest did.
  (table) => [index('idx_logs_timestamp_id').on(table.timestamp.desc(), table.id.desc())]
);

export type LogEntry = typeof logs.$inferSelect;
export type NewLogEntry = typeof logs.$inferInsert;