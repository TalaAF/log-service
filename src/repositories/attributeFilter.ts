import { sql, type SQL } from 'drizzle-orm';

/**
 * The two halves of an `attr.<key>=<value>` filter.
 *
 * `attributeCondition` is the authoritative test and is applied to the row
 * itself, always. `attributeTokens` is a prefilter over the derived sidecar
 * (see 0010_attr_sidecar.sql) that lets the indexed id range be found without
 * reading it, and is never trusted on its own.
 *
 * Keeping them separate is what makes the sidecar safe to be approximate. A
 * token is a 64-bit hash of the pair, so two different pairs can collide; a
 * sidecar row can also be missing, out of date, or left behind by retention.
 * None of that can produce a wrong answer, because every candidate the
 * prefilter proposes has to satisfy the exact predicate against the original
 * JSONB before it is returned.
 *
 * The exact predicate is `attributes ->> key = value` and nothing more. It used
 * to be paired with a `@>` containment prefilter, which was there purely to
 * give the GIN index on logs.attributes something it could serve —
 * jsonb_path_ops indexes containment and not `->>`. That index is gone, so the
 * containment term is now a second, more expensive way of computing an answer
 * the `->>` comparison already has.
 */

export function attributeCondition(key: string, value: string): SQL {
  return sql`attributes ->> ${key} = ${value}`;
}

/**
 * Sidecar prefilter for a whole set of attribute filters.
 *
 * Every pair becomes one token and the array is tested with `@>`, so a row has
 * to carry all of them — which is the AND the caller asked for. The tokens are
 * built by the same SQL function the indexer uses, rather than hashed in the
 * application, so the two can never disagree about what a pair hashes to.
 *
 * `attr_token` is immutable and its arguments are bound parameters, so a custom
 * plan folds the whole expression to an array constant before planning. That
 * matters: with a constant the planner can use the array element statistics to
 * decide between the GIN index and a newest-first walk of the sidecar, which is
 * the difference between answering a rare attribute value and a frequent one
 * efficiently.
 */
export function attributeTokens(attributes: Record<string, string>): SQL {
  const tokens = Object.entries(attributes).map(
    ([key, value]) => sql`attr_token(${key}, ${value})`
  );
  return sql`ARRAY[${sql.join(tokens, sql`, `)}]::bigint[]`;
}
