import { Pool } from 'pg';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Only POST allowed' });
  }

  const { rows } = req.body;
  if (!rows || !Array.isArray(rows)) {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  const client = new Pool({
    connectionString: process.env.NEON_DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    for (const row of rows) {
      const columns = Object.keys(row);
      const values = Object.values(row);

      const columnList = columns.map(c => `"${c}"`).join(', ');
      const paramList = values.map((_, i) => `$${i + 1}`).join(', ');

      // Upsert using UID
      const updateList = columns
        .filter(c => c !== 'UID')
        .map((c, i) => `"${c}" = EXCLUDED."${c}"`)
        .join(', ');

      const sql = `
        INSERT INTO database (${columnList})
        VALUES (${paramList})
        ON CONFLICT ("UID")
        DO UPDATE SET ${updateList};
      `;

      await client.query(sql, values);
    }

    res.status(200).json({ success: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.end();
  }
}