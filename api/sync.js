import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.NEON_DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Only POST allowed' });
  }

  const { rows } = req.body;
  if (!rows || !Array.isArray(rows)) {
    return res.status(400).json({ error: 'Invalid payload “rows” missing' });
  }

  try {
    const client = await pool.connect();

    for (const row of rows) {
      const columns = Object.keys(row);
      const values = Object.values(row);

      const columnList = columns.map(c => `"${c}"`).join(', ');
      const paramList = values.map((_, i) => `$${i + 1}`).join(', ');

      // Filter out UID to avoid breaking UPSERT
      const updateCols = columns.filter(c => c !== "UID");

      const updateList = updateCols
        .map(c => `"${c}" = EXCLUDED."${c}"`)
        .join(', ');

      const sql = `
        INSERT INTO "charging" (${columnList})
        VALUES (${paramList})
        ON CONFLICT ("UID")
        ${updateList ? `DO UPDATE SET ${updateList}` : `DO NOTHING`};
      `;

      await client.query(sql, values);
    }

    res.status(200).json({ success: true });
  } catch (err) {
    console.error("SYNC ERROR:", err);
    res.status(500).json({ error: err.message });
  }
}
