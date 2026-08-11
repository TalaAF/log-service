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
  (table) => [
    index('idx_logs_timestamp_id').on(table.timestamp.desc(), table.id.desc()),
    index('idx_logs_attributes_gin').using('gin', table.attributes),
  ]
);

export type LogEntry = typeof logs.$inferSelect;
export type NewLogEntry = typeof logs.$inferInsert;