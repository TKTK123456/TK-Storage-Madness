// db-proxy.js
import Knex from "knex";

const INTERNAL = Symbol("internalProxy");

function createDeepProxy(target, onChange) {
  if (typeof target !== "object" || target === null) return target;
  if (target[INTERNAL]) return target;

  const proxy = new Proxy(target, {
    get(t, prop, receiver) {
      const value = Reflect.get(t, prop, receiver);
      if (typeof value === "object" && value !== null) {
        const proxied = createDeepProxy(value, onChange);
        Reflect.set(t, prop, proxied);
        return proxied;
      }
      return value;
    },
    set(t, prop, value, receiver) {
      const oldValue = t[prop];
      const result = Reflect.set(t, prop, value, receiver);
      if (oldValue !== value) onChange();
      return result;
    },
    deleteProperty(t, prop) {
      const result = Reflect.deleteProperty(t, prop);
      onChange();
      return result;
    },
  });

  proxy[INTERNAL] = true;
  return proxy;
}

export class TableProxy {
  constructor({ dbUrl, schema = "tk", table, logging = false }) {
    if (table.includes(".")) {
      [schema, table] = table.split(".");
    }

    this.schema = schema;
    this.table = table;

    this.operationQueue = new Map();
    this.queueTimer = null;

    this.knex = Knex({
      client: "pg",
      connection: dbUrl,
      debug: logging === true,
    });

    this.rows = null;
    return this.init();
  }

  async init() {
    const data = await this.knex.withSchema(this.schema).select("*").from(this.table);

    data.forEach((row, i) => {
      if (row._idx === undefined) row._idx = i;
    });

    this.rows = data.map((row) => this._wrapRow(row));
    return this;
  }

  _wrapRow(row) {
    if (row[INTERNAL]) return row;

    const onChange = () => this._queueOperation(row, "save");
    const proxiedRow = createDeepProxy(row, onChange);

    Object.defineProperty(proxiedRow, "_internal", {
      value: { _isProxy: true },
      enumerable: false,
      writable: true,
    });

    return proxiedRow;
  }

  _queueOperation(row, type) {
    if (row._idx === undefined) row._idx = this.rows.length;
    this.operationQueue.set(row._idx, { row, type });

    if (this.queueTimer) return;

    this.queueTimer = setTimeout(async () => {
      const operations = Array.from(this.operationQueue.values());
      this.operationQueue.clear();
      this.queueTimer = null;

      const saves = operations.filter((op) => op.type === "save");
      const deletes = operations.filter((op) => op.type === "delete");

      // Save JSON/JSONB fields dynamically
      if (saves.length > 0) {
        const insertRows = saves.map((op) => {
          const rowToSave = { _idx: op.row._idx };
          for (const key of Object.keys(op.row)) {
            if (key === "_internal") continue;
            const value = op.row[key];
            rowToSave[key] = typeof value === "object" && value !== null ? JSON.stringify(value) : value;
          }
          return rowToSave;
        });

        await this.knex(this.table)
          .withSchema(this.schema)
          .insert(insertRows)
          .onConflict("_idx")
          .merge();
      }

      if (deletes.length > 0) {
        const ids = deletes.map((op) => op.row._idx);
        await this.knex(this.table).withSchema(this.schema).whereIn("_idx", ids).del();
      }
    }, 500);
  }

  getAll() {
    return this.rows;
  }

  push(row) {
    row._idx = this.rows.length;
    const proxied = this._wrapRow(row);
    this.rows.push(proxied);
    this._queueOperation(proxied, "save");
    return proxied;
  }

  splice(start, deleteCount, ...items) {
    const removed = this.rows.splice(
      start,
      deleteCount,
      ...items.map((i, idx) => {
        i._idx = start + idx;
        return this._wrapRow(i);
      })
    );

    removed.forEach((r) => this._queueOperation(r, "delete"));
    items.forEach((i) => this._queueOperation(i, "save"));

    this.rows.forEach((row, idx) => (row._idx = idx));
    return removed;
  }
}
