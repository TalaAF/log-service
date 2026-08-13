import { sql, type SQL } from 'drizzle-orm';

/**
 * Builds the SQL for one `attr.<key>=<value>` filter.
 *
 * The obvious predicate, `attributes ->> key = value`, is correct but cannot use
 * the GIN index: jsonb_path_ops indexes containment (`@>`) and nothing else, so
 * `->>` degrades to a filter applied to every candidate row. Measured on ~10.7M
 * rows that meant scanning the entire partition — 9.5M buffer accesses and tens
 * of seconds — while the index sat unused, costing write throughput and earning
 * nothing.
 *
 * So the predicate is paired with a containment prefilter that the index *can*
 * serve. Correctness rests on the prefilter being a strict superset of the
 * `->>` predicate, which stays in the query as the authoritative test:
 *
 *   - a JSON string equal to `value` is matched by `{"key": "<value>"}`, which
 *     is always one of the candidates;
 *   - a JSON number whose text rendering is `value` is matched by
 *     `{"key": <value>}`, because `value` is by definition that number's
 *     canonical text and parsing it returns the same numeric;
 *   - a JSON boolean is matched the same way, for value 'true' or 'false';
 *   - JSON null makes `->>` return SQL NULL, which never equals `value`, so it
 *     is correctly excluded by both;
 *   - objects and arrays cannot occur — validation rejects non-scalar
 *     attribute values before anything is stored.
 *
 * A candidate that matches more than `->>` does (`1e2` parses to the same
 * numeric as `100`, but renders as `100`) is harmless: the retained `->>`
 * comparison removes it. Erring towards a wider prefilter keeps the pair exact.
 */

/** JSON's own number grammar; anything outside it is not a numeric literal. */
const JSON_NUMBER = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

export function attributeCondition(key: string, value: string): SQL {
  // Candidates are built as JSON text and bound as parameters, never
  // interpolated; the key travels inside the JSON document, so a key containing
  // quotes or braces is escaped by JSON.stringify rather than reaching SQL.
  const candidates: string[] = [JSON.stringify({ [key]: value })];

  if (JSON_NUMBER.test(value)) {
    candidates.push(`{${JSON.stringify(key)}:${value}}`);
  } else if (value === 'true' || value === 'false') {
    candidates.push(`{${JSON.stringify(key)}:${value}}`);
  }

  const containment = sql.join(
    candidates.map((candidate) => sql`attributes @> ${candidate}::jsonb`),
    sql` OR `
  );

  return sql`(${containment}) AND attributes ->> ${key} = ${value}`;
}
