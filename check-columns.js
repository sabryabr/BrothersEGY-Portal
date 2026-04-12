'use strict';

const path = require('path');
const { google } = require('googleapis');

const SA_PATH = path.join(__dirname, 'service-account.json');
const serviceAccount = require(SA_PATH);

async function main() {
  const auth = new google.auth.GoogleAuth({
    keyFile: SA_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: '1T6j2xnRBTY31crQcJHioKurs4Rvaj-VlEQkm6joGxGM',
    range: `'صفحة الإدخالات للإيجارات'!A:BF`,
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = res.data.values || [];
  console.log(`Total rows fetched: ${rows.length}\n`);

  // Helper: 0-based index → column letter (A, B, …, Z, AA, AB, …)
  function colLetter(idx) {
    let letter = '';
    let n = idx + 1;
    while (n > 0) {
      const rem = (n - 1) % 26;
      letter = String.fromCharCode(65 + rem) + letter;
      n = Math.floor((n - 1) / 26);
    }
    return letter;
  }

  const LAST_COL = 57; // BF = index 57
  const header = rows[0] || [];

  console.log('══════════════════════════════════════════════════════════════');
  console.log('ALL COLUMNS A–BF: header + first 3 data rows');
  console.log('══════════════════════════════════════════════════════════════');
  console.log(`${'Col'.padEnd(5)} ${'Idx'.padEnd(4)} ${'Header'.padEnd(42)} Row2            Row3            Row4`);
  console.log('─'.repeat(110));

  for (let c = 0; c <= LAST_COL; c++) {
    const letter  = colLetter(c);
    const hdr     = String(header[c] !== undefined ? header[c] : '');
    const v = (rowIdx) => {
      const row = rows[rowIdx] || [];
      const val = row[c] !== undefined ? row[c] : '';
      return String(val).slice(0, 15).padEnd(16);
    };
    console.log(`${('col_'+letter).padEnd(5)} [${String(c).padEnd(2)}] ${hdr.slice(0,42).padEnd(42)} ${v(1)} ${v(2)} ${v(3)}`);
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
