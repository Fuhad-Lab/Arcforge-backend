/**
 * Forgvi Engine — dynamic schema construction (Vube spec pillar 3, the
 * pydantic.create_model equivalent for the JS kernel).
 *
 * Builds validated input shapes at RUNTIME from field specs, so registry
 * nodes and orchestration dispatch can define their contracts dynamically
 * (no static state machine files). Validation is self-contained — no
 * dependency on TypeBox at the construction boundary.
 *
 *   const schema = createSchema("DispatchTrack", [
 *     { name: "persona", type: "string", required: true, description: "…" },
 *     { name: "task",    type: "string", required: true },
 *     { name: "urgency", type: "enum", values: ["low", "high"], required: false },
 *   ]);
 *   const { ok, value, errors } = validate(schema, { persona: "frontend_expert" });
 */

/**
 * @typedef {Object} FieldSpec
 * @property {string} name
 * @property {"string"|"number"|"boolean"|"array"|"object"|"enum"} type
 * @property {boolean} [required=false]
 * @property {string[]} [values] enum choices
 * @property {number} [min] min length (string/array) or min value (number)
 * @property {number} [max] max length (string/array) or max value (number)
 * @property {string} [description]
 */

/** @typedef {{name: string, fields: FieldSpec[]}} DynSchema */

/** Construct a runtime schema from field specs. */
export function createSchema(name, fields) {
  const clean = (fields ?? []).filter(
    (f) => f && typeof f.name === "string" && f.name.length > 0,
  );
  return { name: String(name ?? "dynamic"), fields: clean };
}

/** Coerce + validate a value against a dynamic schema. Never throws. */
export function validate(schema, value) {
  const errors = [];
  const out = {};
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  for (const field of schema.fields) {
    let v = source[field.name];
    const present = v !== undefined && v !== null && v !== "";
    if (!present) {
      if (field.required) errors.push(`${field.name}: required`);
      continue;
    }
    switch (field.type) {
      case "string":
        if (typeof v !== "string") v = String(v);
        if (field.min != null && v.length < field.min) errors.push(`${field.name}: shorter than ${field.min}`);
        if (field.max != null && v.length > field.max) errors.push(`${field.name}: longer than ${field.max}`);
        out[field.name] = v;
        break;
      case "number": {
        const n = Number(v);
        if (!Number.isFinite(n)) {
          errors.push(`${field.name}: not a number`);
        } else {
          if (field.min != null && n < field.min) errors.push(`${field.name}: < ${field.min}`);
          if (field.max != null && n > field.max) errors.push(`${field.name}: > ${field.max}`);
          out[field.name] = n;
        }
        break;
      }
      case "boolean":
        out[field.name] = v === true || v === "true" || v === 1 || v === "1";
        break;
      case "enum": {
        const s = String(v);
        const allowed = field.values ?? [];
        if (!allowed.includes(s)) {
          errors.push(`${field.name}: must be one of ${allowed.join(" | ")}`);
        } else {
          out[field.name] = s;
        }
        break;
      }
      case "array":
        if (Array.isArray(v)) {
          out[field.name] = v.map((x) => (typeof x === "string" ? x : String(x)));
        } else if (typeof v === "string") {
          try {
            const parsed = JSON.parse(v);
            out[field.name] = Array.isArray(parsed) ? parsed : [String(v)];
          } catch {
            out[field.name] = v.split(",").map((s) => s.trim()).filter(Boolean);
          }
        } else {
          errors.push(`${field.name}: not an array`);
        }
        break;
      case "object":
        if (typeof v === "string") {
          try {
            v = JSON.parse(v);
          } catch {
            errors.push(`${field.name}: not valid JSON`);
            break;
          }
        }
        if (v && typeof v === "object" && !Array.isArray(v)) out[field.name] = v;
        else errors.push(`${field.name}: not an object`);
        break;
      default:
        out[field.name] = v;
    }
  }
  return { ok: errors.length === 0, value: out, errors };
}
