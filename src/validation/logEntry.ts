import { LogEntry, NewLogEntry } from "../db/schema.js";


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
    service: entry.service,
    message: entry.message,
    attributes: attribute,
};
return { valid: true, newLogEntry };
}


function validAttributes(attributes: unknown): {valid: true, attributes: Record<string, unknown>} | {valid: false, reason: string} {
    if(attributes == null || typeof attributes !== 'object' || Array.isArray(attributes)) {
        return { valid: false, reason: 'attributes must be a JSON object' };
    }
    const attr = attributes as Record<string, unknown>;
    for (const value of Object.values(attr)) {
     if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean') {
    return { valid: false, reason: 'attributes values must be strings, numbers, or booleans' };
  }
    }
    return { valid: true, attributes: attr };
}
