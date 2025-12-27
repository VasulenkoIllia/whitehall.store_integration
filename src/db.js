const { Pool } = require('pg');
const { databaseUrl } = require('./config');

if (!databaseUrl) {
  console.warn('DATABASE_URL is not set');
}

const pool = new Pool({ connectionString: databaseUrl });

module.exports = {
  pool,
  query: (text, params) => pool.query(text, params)
};
