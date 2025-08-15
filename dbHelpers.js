// dbHelpers.js
import { Pool } from "pg";

const pool = new Pool({
  host: process.env.DB_HOSTNAME,
  port: parseInt(process.env.DB_PORT || '5432', 10),
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB
});

const DEFAULT_WRITE_SCHEMA = 'tk';

/**
 * Normalize table name with schema for writes
 */
function writeTableName(tableName) {
  if (tableName.includes('.')) return tableName;
  return `${DEFAULT_WRITE_SCHEMA}.${tableName}`;
}

/**
 * List all user tables, optionally for a specific schema
 */
export async function listTables(schema = null) {
  const schemaFilter = schema
    ? `AND table_schema = $1`
    : `AND table_schema NOT IN ('pg_catalog', 'information_schema')`;

  const params = schema ? [schema] : [];

  const res = await pool.query(`
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      ${schemaFilter}
    ORDER BY table_schema, table_name;
  `, params);

  return res.rows.map(r => `${r.table_schema}.${r.table_name}`);
}

/**
 * Get column info for a table
 */
export async function getTableColumns(tableName, schema = null) {
  let table = tableName;
  let tableSchema = schema;

  if (tableName.includes('.')) {
    [tableSchema, table] = tableName.split('.');
  }

  if (!tableSchema) tableSchema = 'public'; // default for reads

  const res = await pool.query(`
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position;
  `, [tableSchema, table]);

  return res.rows;
}

/**
 * Check if table exists
 */
async function tableExists(tableName) {
  const tables = await listTables(); // read all user tables
  return tables.includes(tableName);
}

/**
 * Filter object to only include columns that exist in a table
 */
export async function prepareDataForTable(tableName, data) {
  const columns = await getTableColumns(tableName);
  const columnNames = columns.map(c => c.column_name);
  const filtered = {};

  for (const key of Object.keys(data)) {
    if (columnNames.includes(key)) filtered[key] = data[key];
  }
  return filtered;
}

/**
 * Insert a row safely (default schema tk)
 */
export async function insertRow(tableName, data) {
  tableName = writeTableName(tableName);

  if (!(await tableExists(tableName))) throw new Error(`Table "${tableName}" does not exist`);

  const safeData = await prepareDataForTable(tableName, data);
  if (!Object.keys(safeData).length) throw new Error('No valid columns to insert');

  const columns = Object.keys(safeData);
  const values = Object.values(safeData);
  const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');

  const query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders}) RETURNING *;`;
  const res = await pool.query(query, values);
  return res.rows[0];
}

/**
 * Update rows safely (default schema tk)
 */
export async function updateRows(tableName, data, where) {
  tableName = writeTableName(tableName);

  if (!(await tableExists(tableName))) throw new Error(`Table "${tableName}" does not exist`);

  const safeData = await prepareDataForTable(tableName, data);
  if (!Object.keys(safeData).length) throw new Error('No valid columns to update');

  const dataColumns = Object.keys(safeData);
  const dataValues = Object.values(safeData);

  const whereColumns = Object.keys(where);
  const whereValues = Object.values(where);

  const setClause = dataColumns.map((col, i) => `"${col}" = $${i + 1}`).join(', ');
  const whereClause = whereColumns.map((col, i) => `"${col}" = $${dataColumns.length + i + 1}`).join(' AND ');

  const query = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause} RETURNING *;`;
  const res = await pool.query(query, [...dataValues, ...whereValues]);
  return res.rows;
}

/**
 * Delete rows safely (default schema tk)
 */
export async function deleteRows(tableName, where) {
  tableName = writeTableName(tableName);

  if (!(await tableExists(tableName))) throw new Error(`Table "${tableName}" does not exist`);

  const columns = Object.keys(where);
  const values = Object.values(where);
  const whereClause = columns.map((col, i) => `"${col}" = $${i + 1}`).join(' AND ');

  const query = `DELETE FROM ${tableName} WHERE ${whereClause} RETURNING *;`;
  const res = await pool.query(query, values);
  return res.rows;
}

/**
 * Create a table (default schema tk)
 * columns = [{ name: 'id', type: 'SERIAL PRIMARY KEY' }, ...]
 */
export async function addTable(tableName, columns) {
  tableName = writeTableName(tableName);

  if (await tableExists(tableName)) throw new Error(`Table "${tableName}" already exists`);
  if (!columns.length) throw new Error('Must provide at least one column');

  const cols = columns.map(c => `"${c.name}" ${c.type}`).join(', ');
  const query = `CREATE TABLE ${tableName} (${cols});`;
  await pool.query(query);
  return `Table "${tableName}" created`;
}

/**
 * Read rows (optional schema for reads)
 */
export async function readTable(tableName, where = {}, limit = 100, schema = null) {
  let fullName = tableName.includes('.') ? tableName : `${schema || 'public'}.${tableName}`;

  if (!(await tableExists(fullName))) throw new Error(`Table "${fullName}" does not exist`);

  const columns = Object.keys(where);
  const values = Object.values(where);

  let query = `SELECT * FROM ${fullName}`;
  if (columns.length) {
    const conditions = columns.map((col, i) => `"${col}" = $${i + 1}`).join(' AND ');
    query += ` WHERE ${conditions}`;
  }
  query += ` LIMIT ${limit}`;

  const res = await pool.query(query, values);
  return res.rows;
}

/**
 * Close the pool
 */
export async function closePool() {
  await pool.end();
}
/**
 * Remove a table (drop table) from tk schema
 */
export async function removeTable(tableName) {
  tableName = writeTableName(tableName);

  if (!(await tableExists(tableName))) {
    throw new Error(`Table "${tableName}" does not exist`);
  }

  const query = `DROP TABLE ${tableName} CASCADE;`; // CASCADE will remove dependent objects
  await pool.query(query);
  return `Table "${tableName}" has been removed`;
}
