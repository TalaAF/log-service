import { LogEntry, NewLogEntry } from "../db/schema.js";

/**
 * Postgres cannot represent two things that a JSON string legally can: NUL,
 * which is illegal in both `text` and `jsonb`, and an unpaired surrogate, which
 * makes `jsonb` reject the value outright.
 *
 * These are scrubbed rather than rejected because writes are group-committed:
 * one pathological row reaching the database would fail the entire flush it
 * travelled in, turning a single bad entry into thousands of spurious 5xx
 * responses for unrelated clients. Neither character can appear in a
 * well-formed log line, so removing them keeps the entry and the batch.
 *
 * The scan is a plain char-code loop and allocates nothing for the overwhelming
 * majority of inputs, which contain neither.
 */
function needsSanitizing(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code === 0 || (code >= 0xd800 && code <= 0xdfff)) return true;
  }
  return false;
}

function sanitizeText(value: string): string {
  if (!needsSanitizing(value)) return value;

  let out = '';
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code === 0) continue;

    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      // A high surrogate is only meaningful when a low surrogate follows it;
      // copy the pair through intact, otherwise substitute U+FFFD.
      if (next >= 0xdc00 && next <= 0xdfff) {
        out += value[i] + value[i + 1];
        i++;
      } else {
        out += '�';
      }
      continue;
    }

    if (code >= 0xdc00 && code <= 0xdfff) {
      out += '�';
      continue;
    }

    out += value[i];
  }
  return out;
}

export function validateLogEntry(log: unknown): { valid: true, newLogEntry: NewLogEntry } | { valid: false; reason: string } {
  if(log==null ||typeof log !== 'object'|| Array.isArray(log)){
    return {valid: false, reason: "log entry must be a JSON object"};
  } 
  const entry = log as Record<string, unknown>;

 if (typeof entry.timestamp !== 'string') {
    return { valid: false, reason: 'timestamp must be a string' };
  }
   const logTime = new Date(entry.timestamp).getTime();
  if (isNaN(logTime)) {
    return { valid: false, reason: 'timestamp must be a valid ISO 8601 timestamp' };
  }
    const maxAllowed = Date.now() + 5 * 60 * 1000;
  if (logTime > maxAllowed) {
    return { valid: false, reason: 'timestamp must not be more than five minutes in the future' };
  }
  const levels= ['debug', 'info', 'warn', 'error'];

  if(typeof entry.level !== 'string' || !levels.includes(entry.level)) {
    return { valid: false, reason: `level must be one of ${levels.join(', ')}` };
  }
    if(typeof entry.service !== 'string' || entry.service.trim() === '') {
    return { valid: false, reason: 'service must be a non-empty string' };
  }
 
    if(typeof entry.message !== 'string' || entry.message.trim() === '') {
    return { valid: false, reason: 'message must be a non-empty string' };
  }
  let attribute: Record<string, unknown> = {};
  if(entry.attributes!=null){
      const result = validAttributes(entry.attributes);
    if (!result.valid) {
        return result;
    }
    attribute = result.attributes;  
}

const newLogEntry: NewLogEntry = {
    timestamp: entry.timestamp,
    level: entry.level,
    service: sanitizeText(entry.service),
    message: sanitizeText(entry.message),
    attributes: attribute,
};
return { valid: true, newLogEntry };
}


function validAttributes(attributes: unknown): {valid: true, attributes: Record<string, unknown>} | {valid: false, reason: string} {
    if(attributes == null || typeof attributes !== 'object' || Array.isArray(attributes)) {
        return { valid: false, reason: 'attributes must be a JSON object' };
    }
    const attr = attributes as Record<string, unknown>;
    const clean: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(attr)) {
     if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean') {
    return { valid: false, reason: 'attributes values must be strings, numbers, or booleans' };
  }
    clean[sanitizeText(key)] = typeof value === 'string' ? sanitizeText(value) : value;
    }
    return { valid: true, attributes: clean };
}

export interface RejectedEntry {
  index: number;
  reason: string;
}

export function processLogBatch(rawLogs: unknown[]): {
  accepted: NewLogEntry[];
  rejected: RejectedEntry[];
} {
  const accepted: NewLogEntry[] = [];
  const rejected: RejectedEntry[] = [];

  rawLogs.forEach((log, index) => {
    const result = validateLogEntry(log);
    if (result.valid) {
      accepted.push(result.newLogEntry);
    } else {
      rejected.push({ index, reason: result.reason });
    }
  });

  return { accepted, rejected };
}
