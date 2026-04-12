/**
 * sync-sheets.js
 * Reads 6 Google Sheets and writes them to Firestore (clean-slate each run).
 */

'use strict';

const path = require('path');
const { google } = require('googleapis');
const admin = require('firebase-admin');

// ── Config ────────────────────────────────────────────────────────────────────
const SA_PATH = path.join(__dirname, 'service-account.json');
const serviceAccount = require(SA_PATH);

const SHEETS = [
  {
    name: 'Cars',
    spreadsheetId: '1tQVkPj7tCnrKsHEIs04a1WzzC04jpOWuLsXgXOkVMkk',
    tab: 'صفحة الإدخالات لقاعدة البيانات',
    collection: 'fleet',
    idCol: 0,            // column A (0-indexed)
    isCars: true,
  },
  {
    name: 'Orders',
    spreadsheetId: '1T6j2xnRBTY31crQcJHioKurs4Rvaj-VlEQkm6joGxGM',
    tab: 'صفحة الإدخالات للإيجارات',
    collection: 'bookings',
    idCol: 0,
  },
  {
    name: 'Clients',
    spreadsheetId: '13YZOGdRCEy7IMZHiTmjLFyO417P8dD0m5Sh9xwKI8js',
    tab: 'صفحة الإدخالات لقاعدة البيانات',
    collection: 'customers',
    idCol: 0,
  },
  {
    name: 'Expenses',
    spreadsheetId: '1hZoymf0CN1wOssc3ddQiZXxbJTdzJZBnamp_aCobl1Q',
    tab: 'صفحة الإدخالات لقاعدة البيانات',
    collection: 'gen_expenses',
    idCol: 0,
  },
  {
    name: 'Car Expenses',
    spreadsheetId: '1vDKKOywOEGfmLcHr4xk7KMTChHJ0_qquNopXpD81XVE',
    tab: 'صفحة الإدخالات لقاعدة البيانات',
    collection: 'car_expenses',
    idCol: 0,
  },
  {
    name: 'Collections',
    spreadsheetId: '1jtp-ihtAOt9NNHETZ5muiL5OA9yW3WrpBIIDAf5UAyg',
    tab: 'صفحة الإدخالات لقاعدة البيانات',
    collection: 'collections',
    idCol: 0,
  },
];

// Column letter → 0-based index (A=0, Z=25, AA=26 … CZ=103)
function colLetterToIndex(letter) {
  letter = letter.toUpperCase();
  let n = 0;
  for (const ch of letter) n = n * 26 + (ch.charCodeAt(0) - 64);
  return n - 1;
}

// 0-based index → column letter
function indexToColLetter(idx) {
  let letter = '';
  idx += 1; // 1-based
  while (idx > 0) {
    const rem = (idx - 1) % 26;
    letter = String.fromCharCode(65 + rem) + letter;
    idx = Math.floor((idx - 1) / 26);
  }
  return letter;
}

const CZ_IDX = colLetterToIndex('CZ'); // 103  (columns A–CZ)

// ── Cars-specific computed fields ─────────────────────────────────────────────
function buildCarExtras(row, headers) {
  const get = (letter) => (row[colLetterToIndex(letter)] || '').toString().trim();

  // Plate: columns W X Y Z AA AB AC joined with spaces (non-empty parts only)
  const plateLetters = ['W','X','Y','Z','AA','AB','AC'];
  const plate = plateLetters.map(get).filter(Boolean).join(' ');

  const carType  = get('B');
  const model    = get('E');
  const year     = get('H');
  const color    = get('I');
  const azVal    = get('AZ');   // "Valid" or other

  const carLabel = [carType, model, year ? `(${year})` : '', color, plate ? `/ ${plate}` : '']
    .filter(Boolean).join(' ');

  return {
    car_label: carLabel,
    is_active: azVal === 'Valid',
    archived:  azVal !== 'Valid',
    owner_name: [get('BP'), get('BQ')].filter(Boolean).join(' '),
    contract_end_date: get('BC'),
    license_end_date:  get('AQ'),
    insurance_end_date: get('BJ'),
    monthly_fee:             get('CJ'),
    payment_frequency_days:  get('CK'),
    deduction_pct:           get('CL'),
  };
}

// ── Firestore batch delete (500 docs per batch) ───────────────────────────────
async function deleteCollection(db, collectionName) {
  const colRef = db.collection(collectionName);
  let deleted = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const snap = await colRef.limit(400).get();
    if (snap.empty) break;
    const batch = db.batch();
    snap.docs.forEach(d => batch.delete(d.ref));
    await batch.commit();
    deleted += snap.size;
  }
  return deleted;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  // Firebase Admin init
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: 'brothers-egy-portal',
  });
  const db = admin.firestore();

  // Google Sheets auth
  const auth = new google.auth.GoogleAuth({
    keyFile: SA_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const summary = {};

  for (const cfg of SHEETS) {
    console.log(`\n──────────────────────────────────────`);
    console.log(`📄  ${cfg.name}  →  Firestore: ${cfg.collection}`);

    // 1. Read sheet
    let rows;
    try {
      const range = `'${cfg.tab}'!A:CZ`;
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: cfg.spreadsheetId,
        range,
        valueRenderOption: 'UNFORMATTED_VALUE',
      });
      rows = res.data.values || [];
    } catch (err) {
      console.error(`  ❌  Failed to read sheet: ${err.message}`);
      summary[cfg.name] = { written: 0, errors: 1, note: 'Sheet read failed' };
      continue;
    }

    if (rows.length < 2) {
      console.log(`  ⚠️  Sheet has no data rows — skipping.`);
      summary[cfg.name] = { written: 0, errors: 0, note: 'Empty sheet' };
      continue;
    }

    const headers = rows[0]; // row 1
    const dataRows = rows.slice(1); // row 2+
    console.log(`  📊  ${dataRows.length} data rows, ${headers.length} header columns`);

    // 2. Delete existing collection
    console.log(`  🗑️   Clearing existing documents…`);
    const deletedCount = await deleteCollection(db, cfg.collection);
    console.log(`  ✅  Deleted ${deletedCount} old documents`);

    // 3. Write new documents in batches of 400
    let written = 0;
    let errors  = 0;
    const now   = Date.now();
    const BATCH  = 400;

    for (let batchStart = 0; batchStart < dataRows.length; batchStart += BATCH) {
      const batch = db.batch();
      const chunk = dataRows.slice(batchStart, batchStart + BATCH);

      for (let i = 0; i < chunk.length; i++) {
        const row       = chunk[i];
        const sheetRow  = batchStart + i + 2; // 1-based, offset by header row

        // Skip fully-empty rows
        if (!row || row.every(c => c === '' || c === null || c === undefined)) continue;

        const idVal = (row[cfg.idCol] || '').toString().trim();
        if (!idVal) continue; // no ID → skip

        // Build document
        const doc = {
          _source:    'google_sheets',
          _synced_at: now,
          _sheet_row: sheetRow,
        };

        // Store every column value: col_A, col_B … col_CZ
        for (let c = 0; c <= CZ_IDX; c++) {
          const letter  = indexToColLetter(c);
          const colKey  = `col_${letter}`;
          const val     = row[c] !== undefined ? row[c] : '';

          doc[colKey] = val;

          // Also store under the header name (if present and different from colKey)
          const headerName = (headers[c] || '').toString().trim();
          if (headerName && headerName !== colKey) {
            doc[headerName] = val;
          }
        }

        // Cars-specific computed fields
        if (cfg.isCars) {
          Object.assign(doc, buildCarExtras(row, headers));
        }

        // Explicit field mappings for ERP compatibility
        const r = (letter) => { const v = row[colLetterToIndex(letter)]; return v !== undefined ? v : ''; };
        if (cfg.collection === 'bookings') {
          doc['No.']                               = r('A');
          doc['كود العميل']                        = r('B');
          doc['اسم العميل']                        = r('C');
          doc['كود السيارة']                       = r('D');
          doc['اسم السيارة']                       = r('E');
          doc['بداية التعاقد']                     = r('L');
          doc['نهاية التعاقد']                     = r('T');
          doc['سعر السيارة اليومي بالجنيه المصري'] = r('W');
          doc['إجمالي المستحق (Total)']            = r('AU');
          doc['المدفوع EGP']                       = r('AX');
          doc['المدفوع USD']                       = r('AY');
          doc['المدفوع EUR']                       = r('AZ');
          doc['مكان الاستلام']                     = r('M');
          doc['مكان التسليم']                      = r('U');
        }
        if (cfg.collection === 'fleet') {
          doc['ID']           = r('A');
          doc['حالة التعاقد'] = r('AZ');
          doc['نهاية الترخيص']= r('AQ');
          doc['نهاية التأمين']= r('BJ');
          doc['تاريخ التسليم']= r('BC');
          doc['النوع']        = r('B');
          doc['الطراز']       = r('E');
          doc['سنة الصنع']    = r('H');
          doc['اللون']        = r('I');
        }

        const docRef = db.collection(cfg.collection).doc(idVal);
        batch.set(docRef, doc);
      }

      try {
        await batch.commit();
        written += chunk.length;
        process.stdout.write(`  ✍️   Written ${written}/${dataRows.length}\r`);
      } catch (err) {
        errors++;
        console.error(`\n  ❌  Batch write failed: ${err.message}`);
      }
    }

    console.log(`\n  ✅  Done — ${written} written, ${errors} batch errors`);
    summary[cfg.name] = { written, errors };
  }

  // ── Final summary ─────────────────────────────────────────────────────────
  console.log(`\n${'═'.repeat(45)}`);
  console.log('📋  SYNC SUMMARY');
  console.log('═'.repeat(45));
  let totalWritten = 0, totalErrors = 0;
  for (const [name, s] of Object.entries(summary)) {
    const note = s.note ? ` (${s.note})` : '';
    console.log(`  ${name.padEnd(15)} → ${String(s.written).padStart(5)} written  |  ${s.errors} errors${note}`);
    totalWritten += s.written;
    totalErrors  += s.errors;
  }
  console.log('─'.repeat(45));
  console.log(`  ${'TOTAL'.padEnd(15)} → ${String(totalWritten).padStart(5)} written  |  ${totalErrors} errors`);
  console.log('═'.repeat(45));

  await admin.app().delete();
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
