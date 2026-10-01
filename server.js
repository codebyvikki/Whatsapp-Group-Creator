
const express = require('express');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const QRCode = require('qrcode');
const pino = require('pino');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require('@whiskeysockets/baileys');

const PORT = Number(process.env.PORT || 3000);
const ACCESS_KEY = String(process.env.ACCESS_KEY || 'change-me');

const BATCH_MAX = 25;
const MAX_PER_DAY = 100;

// 5 seconds is the fixed gap requested for the original fast workflow.
const FIXED_GAP = 5;

// A second batch started shortly after the previous one gets a soft warning only.
const BATCH_WARNING_WINDOW = 30 * 1000;

// Keep the creation path lean: one group-create request per group,
// followed by a lightweight invite-code lookup. Rate-limit retries on the
// create operation are intentionally disabled because they amplify load.
const GROUP_CREATE_TIMEOUT = 45000;
const LINK_RETRY_DELAY = 2500;
const LINK_RECOVERY_ATTEMPTS = 2;
const linkRecoveryQueue = [];
let linkRecoveryRunning = false;

const DATA_FILE = path.join(process.cwd(), 'panel-data.json');
const AUTH_DIR = process.env.AUTH_DIR || 'auth';

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '256kb' }));
app.use(express.static(path.join(process.cwd(), 'public')));

let sock = null;
let status = 'disconnected';
let qrDataUrl = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let connecting = false;
let disconnecting = false;

let logs = [];
let currentJob = null;
let tokens = new Map();
let store = { daily: {}, history: [] };

let db = null;
let collection = null;
let lastBatchFinishedAt = 0;

const now = () => new Date();
const iso = () => now().toISOString();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function log(message, level = 'INFO') {
  const line = `${new Date().toLocaleTimeString()}  ${level.padEnd(7)} ${message}`;
  logs.push(line);
  if (logs.length > 120) logs.shift();
  console.log(line);
}

function normaliseStore(input) {
  if (!input || typeof input !== 'object') return { daily: {}, history: [] };

  const daily = input.daily && typeof input.daily === 'object'
    ? input.daily
    : {};

  // Migrate the old numeric-per-day format into an account bucket named "legacy".
  for (const [date, value] of Object.entries(daily)) {
    if (typeof value === 'number') {
      daily[date] = { legacy: Math.max(0, Number(value) || 0) };
    }
  }

  return {
    daily,
    history: Array.isArray(input.history) ? input.history : []
  };
}

function loadStore() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      store = normaliseStore(
        JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))
      );
    }
  } catch (e) {
    log(`Local data load failed: ${e.message}`, 'WARN');
    store = { daily: {}, history: [] };
  }
}

function saveStore() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(store, null, 2));
  } catch (e) {
    log(`Local data save failed: ${e.message}`, 'WARN');
  }
}

async function initDb() {
  const uri = process.env.MONGODB_URI || process.env.MONGODB_URL;
  if (!uri) return;

  try {
    db = new MongoClient(uri, {
      maxPoolSize: 5,
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000,
    });

    await db.connect();

    collection = db
      .db(process.env.MONGODB_DB || 'wa_group_panel')
      .collection('panel');

    const saved = await collection.findOne({ _id: 'state' });

    if (saved?.store) {
      store = normaliseStore(saved.store);
      saveStore();
    } else {
      await collection.updateOne(
        { _id: 'state' },
        { $set: { store } },
        { upsert: true }
      );
    }

    log('MongoDB persistence enabled', 'OK');
  } catch (e) {
    db = null;
    collection = null;
    log(`MongoDB unavailable; using local persistence: ${e.message}`, 'WARN');
  }
}

let saveTimer = null;

function persistSoon() {
  clearTimeout(saveTimer);

  saveTimer = setTimeout(async () => {
    saveStore();

    if (collection) {
      try {
        await collection.updateOne(
          { _id: 'state' },
          { $set: { store } },
          { upsert: true }
        );
      } catch (e) {
        log(`MongoDB save failed: ${e.message}`, 'WARN');
      }
    }
  }, 250);
}

const todayKey = () => now().toISOString().slice(0, 10);

function accountBucket(accountNumber) {
  const date = todayKey();

  if (!store.daily[date] || typeof store.daily[date] !== 'object') {
    store.daily[date] = {};
  }

  const key = cleanPhone(accountNumber) || 'unknown';

  if (!Number.isFinite(Number(store.daily[date][key]))) {
    store.daily[date][key] = 0;
  }

  return store.daily[date];
}

function usage(accountNumber) {
  const bucket = accountBucket(accountNumber);
  const key = cleanPhone(accountNumber) || 'unknown';
  return Math.max(0, Number(bucket[key] || 0));
}

function incrementUsage(accountNumber, amount = 1) {
  const bucket = accountBucket(accountNumber);
  const key = cleanPhone(accountNumber) || 'unknown';

  bucket[key] = usage(accountNumber) + amount;
  persistSoon();
}

function pruneOldDaily() {
  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - 7);
  const cutoffKey = cutoff.toISOString().slice(0, 10);

  for (const key of Object.keys(store.daily)) {
    if (key < cutoffKey) delete store.daily[key];
  }
}

function getRecentBatchWarning() {
  if (!lastBatchFinishedAt) return null;

  const elapsed = Date.now() - lastBatchFinishedAt;
  if (elapsed >= BATCH_WARNING_WINDOW) return null;

  const seconds = Math.max(
    1,
    Math.ceil((BATCH_WARNING_WINDOW - elapsed) / 1000)
  );

  return `Previous batch just finished. For smoother operation, try again after about ${seconds} seconds. You can still create this batch now.`;
}

function cleanPhone(value) {
  const digits = String(value || '').replace(/\D/g, '');
  return digits.length >= 10 && digits.length <= 15 ? digits : null;
}

function uniqueMembers(input) {
  const seen = new Set();
  const valid = [];
  let invalid = 0;
  let duplicate = 0;

  for (const raw of input) {
    const n = cleanPhone(raw);

    if (!n) {
      invalid++;
      continue;
    }

    if (seen.has(n)) {
      duplicate++;
      continue;
    }

    seen.add(n);
    valid.push(n);
  }

  return { valid, invalid, duplicate };
}

function getConnectedNumber() {
  const raw = sock?.user?.id;
  if (!raw) return null;

  const number = String(raw)
    .split(':')[0]
    .replace(/\D/g, '');

  return cleanPhone(number);
}

function buildNames(prefix, start, amount) {
  const cleanPrefix = String(prefix || '').trim();

  return Array.from(
    { length: amount },
    (_, i) => `${cleanPrefix} ${start + i}`.trim()
  );
}

function publicJob(job) {
  if (!job) return null;

  return {
    id: job.id,
    prefix: job.prefix,
    start: job.start,
    requested: job.requested,
    total: job.total,
    completed: job.completed,
    failed: job.failed,
    pending: job.total - job.completed - job.failed,
    paused: job.paused,
    cancelled: job.cancelled,
    running: job.running,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    current: job.current,
    results: job.results,
    validation: job.validation,
    accountNumber: job.accountNumber,
  };
}

function auth(req, res, next) {
  const token = req.headers['x-token'];
  const expires = tokens.get(token);

  if (token && expires && expires > Date.now()) return next();

  if (token) tokens.delete(token);

  return res.status(401).json({
    error: 'Session expired. Please log in again.'
  });
}

function isRateLimitError(error) {
  const text = String(error?.message || error || '').toLowerCase();

  return (
    text.includes('rate') ||
    text.includes('429') ||
    text.includes('too many') ||
    text.includes('temporarily unavailable') ||
    text.includes('thrott') ||
    text.includes('frequency') ||
    text.includes('spam')
  );
}

function isPermissionError(error) {
  const text = String(error?.message || error || '').toLowerCase();

  return (
    text.includes('not-authorized') ||
    text.includes('not authorized') ||
    text.includes('forbidden') ||
    text.includes('admin') ||
    text.includes('not enough')
  );
}

async function connectWhatsApp() {
  if (connecting || disconnecting || status === 'connected') return;

  connecting = true;
  clearTimeout(reconnectTimer);

  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    const next = makeWASocket({
      version,
      auth: state,
      logger: pino({ level: 'silent' }),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      browser: ['Group Creator Pro', 'Chrome', '1.0'],
    });

    sock = next;
    next.ev.on('creds.update', saveCreds);

    next.ev.on(
      'connection.update',
      async ({ connection, qr, lastDisconnect }) => {
        if (qr) {
          status = 'qr';

          qrDataUrl = await QRCode.toDataURL(qr, {
            margin: 1,
            width: 420,
            errorCorrectionLevel: 'M',
          });

          log('QR ready');
        }

        if (connection === 'open') {
          status = 'connected';
          qrDataUrl = null;
          reconnectAttempt = 0;
          connecting = false;
          disconnecting = false;

          log(
            `WhatsApp connected${getConnectedNumber() ? `: +${getConnectedNumber()}` : ''}`,
            'OK'
          );
        }

        if (connection === 'close') {
          connecting = false;
          sock = null;

          if (disconnecting) {
            status = 'disconnected';
            qrDataUrl = null;
            disconnecting = false;

            log('WhatsApp disconnected by user', 'WARN');

            setTimeout(connectWhatsApp, 350);
            return;
          }

          status = 'disconnected';
          qrDataUrl = null;

          const code = lastDisconnect?.error?.output?.statusCode;

          if (code === DisconnectReason.loggedOut) {
            log(
              'WhatsApp logged out. Scan the QR code again.',
              'WARN'
            );

            setTimeout(connectWhatsApp, 350);
            return;
          }

          reconnectAttempt++;

          const wait = Math.min(
            30000,
            2000 * (2 ** Math.min(reconnectAttempt - 1, 4))
          );

          log(
            `Connection closed; reconnecting in ${Math.round(wait / 1000)}s`,
            'WARN'
          );

          reconnectTimer = setTimeout(connectWhatsApp, wait);
        }
      }
    );
  } catch (e) {
    connecting = false;
    status = 'disconnected';
    reconnectAttempt++;

    const wait = Math.min(
      30000,
      2000 * (2 ** Math.min(reconnectAttempt - 1, 4))
    );

    log(`Connection error: ${e.message}`, 'ERROR');

    reconnectTimer = setTimeout(connectWhatsApp, wait);
  }
}

async function resetAuthFolder() {
  try {
    await fs.promises.rm(AUTH_DIR, {
      recursive: true,
      force: true,
    });
  } catch (e) {
    log(`Auth cleanup failed: ${e.message}`, 'WARN');
  }
}

async function disconnectWhatsApp() {
  if (disconnecting) return;

  disconnecting = true;
  clearTimeout(reconnectTimer);

  const oldSock = sock;
  sock = null;
  status = 'disconnected';
  qrDataUrl = null;

  try {
    if (oldSock) {
      try {
        await oldSock.logout();
      } catch (e) {
        log(`Logout handshake ended: ${e.message}`, 'INFO');
      }
    }
  } finally {
    await resetAuthFolder();

    disconnecting = false;
    connecting = false;

    log(
      'WhatsApp session reset. Starting a fresh QR session.',
      'OK'
    );

    setTimeout(connectWhatsApp, 350);
  }
}

function withTimeout(promise, ms, message) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error(message)), ms);
    })
  ]);
}

async function createGroup(job, name) {
  if (job.cancelled) throw new Error('Job cancelled.');

  if (!sock || status !== 'connected') {
    throw new Error('WhatsApp is no longer connected.');
  }

  const jidList = job.members.map(
    n => `${n}@s.whatsapp.net`
  );

  // IMPORTANT: do not retry groupCreate on rate-limit errors. A retry here
  // sends another creation request and can make an otherwise healthy batch
  // hit WhatsApp's overlimit protection much faster.
  return withTimeout(
    sock.groupCreate(name, jidList),
    GROUP_CREATE_TIMEOUT,
    `Group creation timed out after ${GROUP_CREATE_TIMEOUT / 1000}s.`
  );
}

async function getInviteLinkFast(job, groupId, name) {
  if (!sock || status !== 'connected') return '';

  try {
    const code = await sock.groupInviteCode(groupId);
    return code ? `https://chat.whatsapp.com/${code}` : '';
  } catch (e) {
    // IMPORTANT: link lookup failure must never slow or fail group creation.
    // The group already exists. Put only the link lookup into a background
    // queue so the next group can start on the normal fixed 5-second cadence.
    log(
      `${name}: group created; invite link will be recovered in background (${e.message})`,
      'WARN'
    );
    return '';
  }
}

function enqueueLinkRecovery(job, result) {
  if (!result?.id || result.link) return;

  linkRecoveryQueue.push({
    job,
    result,
    accountNumber: job.accountNumber,
    attempts: 0,
    nextAt: Date.now() + LINK_RETRY_DELAY,
  });

  processLinkRecovery().catch(e => {
    log(`Invite-link recovery worker stopped: ${e.message}`, 'WARN');
  });
}

async function processLinkRecovery() {
  if (linkRecoveryRunning) return;
  linkRecoveryRunning = true;

  try {
    while (linkRecoveryQueue.length) {
      const item = linkRecoveryQueue[0];
      const wait = item.nextAt - Date.now();
      if (wait > 0) await sleep(wait);

      linkRecoveryQueue.shift();

      if (item.result.link) continue;
      if (status !== 'connected' || !sock) {
        item.nextAt = Date.now() + 5000;
        linkRecoveryQueue.push(item);
        continue;
      }

      item.attempts++;

      try {
        const code = await sock.groupInviteCode(item.result.id);
        if (code) {
          item.result.link = `https://chat.whatsapp.com/${code}`;
          item.result.linkAvailable = true;
          item.result.linkPending = false;
          item.result.linkError = null;
          persistSoon();
          log(`Invite link recovered: ${item.result.name}`, 'OK');
          continue;
        }
      } catch (e) {
        item.result.linkError = String(e?.message || e);
      }

      if (item.attempts < LINK_RECOVERY_ATTEMPTS) {
        item.nextAt = Date.now() + (item.attempts === 1 ? 5000 : 10000);
        linkRecoveryQueue.push(item);
      } else {
        item.result.linkPending = false;
        item.result.linkAvailable = false;
        persistSoon();
        log(`Invite link still unavailable: ${item.result.name}`, 'WARN');
      }
    }
  } finally {
    linkRecoveryRunning = false;
  }
}
async function createOne(job, name, index) {
  const group = await createGroup(job, name);
  const link = await getInviteLinkFast(job, group.id, name);

  const result = {
    index,
    name,
    id: group.id,
    link,
    linkAvailable: Boolean(link),
    linkPending: !link,
    status: 'success',
    createdAt: iso(),
  };

  if (!link) enqueueLinkRecovery(job, result);

  return result;
}
async function waitBetweenGroups(job, ms) {
  const end = Date.now() + ms;

  while (Date.now() < end) {
    if (job.cancelled) return;

    while (job.paused && !job.cancelled) {
      await sleep(250);
    }

    if (job.cancelled) return;

    const remaining = end - Date.now();

    await sleep(
      Math.min(
        250,
        Math.max(50, remaining)
      )
    );
  }
}


function addHistory(job) {
  const historyEntry = {
    id: job.id,
    accountNumber: job.accountNumber,
    prefix: job.prefix,
    start: job.start,
    requested: job.requested,
    total: job.total,
    members: job.members,
    delay: job.delay,
    completed: job.completed,
    failed: job.failed,
    results: job.results,
    running: false,
    paused: false,
    cancelled: job.cancelled,
    current: null,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    validation: job.validation,
  };

  store.history.unshift(historyEntry);
  store.history = store.history.slice(0, 50);

  persistSoon();
}

async function runJob(job) {
  job.running = true;
  job.startedAt = iso();

  persistSoon();

  log(
    `Job ${job.id} started: ${job.total} groups on +${job.accountNumber}`,
    'INFO'
  );

  for (let i = 0; i < job.names.length; i++) {
    if (job.cancelled) break;

    while (job.paused && !job.cancelled) {
      await sleep(250);
    }

    if (job.cancelled) break;

    const name = job.names[i];
    job.current = {
      index: i + 1,
      name,
    };

    try {
      const result = await createOne(
        job,
        name,
        i + 1
      );

      job.results.push(result);
      job.completed++;

      incrementUsage(job.accountNumber, 1);

      if (result.linkAvailable) {
        log(
          `Created ${i + 1}/${job.total}: ${name}`,
          'OK'
        );
      } else {
        log(
          `Created ${i + 1}/${job.total}: ${name}; invite link unavailable`,
          'WARN'
        );
      }
    } catch (e) {
      job.results.push({
        index: i + 1,
        name,
        status: 'failed',
        error: e.message,
        rateLimited: isRateLimitError(e),
        createdAt: iso(),
      });

      job.failed++;

      log(
        `Failed ${i + 1}/${job.total}: ${name} — ${e.message}`,
        'ERROR'
      );
    }

    persistSoon();

    if (
      i < job.names.length - 1 &&
      !job.cancelled
    ) {
      await waitBetweenGroups(
        job,
        FIXED_GAP * 1000
      );
    }
  }

  job.current = null;
  job.running = false;
  job.finishedAt = iso();

  if (job.cancelled) {
    log(
      `Job ${job.id} cancelled`,
      'WARN'
    );
  } else {
    log(
      `Job ${job.id} completed: ${job.completed} success, ${job.failed} failed`,
      'OK'
    );
  }

  addHistory(job);

  lastBatchFinishedAt = Date.now();
  currentJob = null;

  persistSoon();
}

app.post('/api/login', (req, res) => {
  if (String(req.body?.key || '') !== ACCESS_KEY) {
    return res.status(401).json({
      error: 'Invalid access key.',
    });
  }

  const token = crypto
    .randomBytes(32)
    .toString('hex');

  tokens.set(
    token,
    Date.now() + 1000 * 60 * 60 * 12
  );

  res.json({ token });
});

app.post('/api/logout', auth, (req, res) => {
  tokens.delete(req.headers['x-token']);
  res.json({ ok: true });
});

app.get('/api/status', auth, (req, res) => {
  const connectedNumber = getConnectedNumber();

  res.json({
    status,
    qr: qrDataUrl,
    logs,
    reconnectAttempt,
    accountNumber: connectedNumber,
    createdToday: usage(connectedNumber),
    maxPerDay: MAX_PER_DAY,
    batchMax: BATCH_MAX,
    fixedGap: FIXED_GAP,
    job: publicJob(currentJob),
    history: store.history.slice(0, 20),
  });
});

app.post('/api/disconnect', auth, async (req, res) => {
  if (currentJob?.running) {
    return res.status(409).json({
      error:
        'Please complete or cancel the current job before disconnecting WhatsApp.',
    });
  }

  try {
    await disconnectWhatsApp();

    res.json({
      ok: true,
      status: 'disconnected',
    });
  } catch (e) {
    disconnecting = false;
    connecting = false;

    log(
      `Disconnect failed: ${e.message}`,
      'ERROR'
    );

    res.status(500).json({
      error: 'Unable to disconnect WhatsApp.',
    });
  }
});

app.post('/api/create', auth, (req, res) => {
  if (status !== 'connected') {
    return res.status(400).json({
      error: 'WhatsApp is not connected.',
    });
  }

  if (currentJob) {
    return res.status(409).json({
      error: 'Another job is already active.',
    });
  }

  const accountNumber = getConnectedNumber();

  if (!accountNumber) {
    return res.status(400).json({
      error:
        'Unable to identify the connected WhatsApp account. Please reconnect WhatsApp.',
    });
  }

  const prefix = String(
    req.body?.prefix ?? ''
  ).trim();

  const start = Number.parseInt(
    req.body?.start,
    10
  );

  const rawAmount = Number.parseInt(
    req.body?.amount,
    10
  );

  if (!prefix) {
    return res.status(400).json({
      error:
        'Please enter a group prefix.',
    });
  }

  if (
    !Number.isInteger(start) ||
    start < 0
  ) {
    return res.status(400).json({
      error:
        'Please enter a valid starting number.',
    });
  }

  if (
    !Number.isInteger(rawAmount) ||
    rawAmount < 1
  ) {
    return res.status(400).json({
      error:
        'Group amount must be at least 1.',
    });
  }

  if (rawAmount > BATCH_MAX) {
    return res.status(400).json({
      error:
        `One batch can contain maximum ${BATCH_MAX} groups.`,
    });
  }

  const membersRaw = Array.isArray(req.body?.members)
    ? req.body.members
    : String(req.body?.members || '')
        .split(/\r?\n/);

  const cleanedInput = membersRaw
    .map(v => String(v).trim())
    .filter(Boolean);

  let members = uniqueMembers(cleanedInput);
  let usedConnectedNumber = false;

  if (
    !members.valid.length &&
    cleanedInput.length === 0
  ) {
    const ownNumber = accountNumber;

    members = {
      valid: [ownNumber],
      invalid: 0,
      duplicate: 0,
    };

    usedConnectedNumber = true;
  }

  if (!members.valid.length) {
    return res.status(400).json({
      error:
        'Please enter at least one valid member number.',
    });
  }

  const createdToday = usage(accountNumber);
  const remaining =
    MAX_PER_DAY - createdToday;

  if (remaining <= 0) {
    return res.status(400).json({
      error:
        `Daily limit reached for +${accountNumber}. This WhatsApp account has already created ${MAX_PER_DAY}/${MAX_PER_DAY} groups today.`,
    });
  }

  if (rawAmount > remaining) {
    return res.status(400).json({
      error:
        `Only ${remaining} group creation slot${remaining === 1 ? '' : 's'} remain today for +${accountNumber}.`,
    });
  }

  const names = buildNames(
    prefix,
    start,
    rawAmount
  );

  const job = {
    id:
      `JOB-${Date.now().toString(36).toUpperCase()}`,

    accountNumber,
    prefix,
    start,
    requested: rawAmount,
    total: rawAmount,
    names,
    members: members.valid,
    delay: FIXED_GAP,
    completed: 0,
    failed: 0,
    results: [],
    running: false,
    paused: false,
    cancelled: false,
    current: null,
    startedAt: null,
    finishedAt: null,

    validation: {
      validMembers: members.valid.length,
      invalidMembers: members.invalid,
      duplicateMembers: members.duplicate,
      effectiveAmount: rawAmount,
      batchMax: BATCH_MAX,
      fixedGapSeconds: FIXED_GAP,
      usedConnectedNumber,
      accountNumber,
      dailyUsedBefore: createdToday,
      dailyRemainingBefore: remaining,
    },
  };

  currentJob = job;

  const batchWarning =
    getRecentBatchWarning();

  res.json({
    ok: true,
    job: publicJob(job),
    validation: job.validation,
    warning: batchWarning,
  });

  runJob(job).catch(e => {
    log(
      `Fatal job error: ${e.message}`,
      'ERROR'
    );

    job.running = false;
    job.finishedAt = iso();

    addHistory(job);
    currentJob = null;
  });
});


app.post('/api/link/retry', auth, async (req, res) => {
  const jobId = String(req.body?.jobId || '');
  const index = Number.parseInt(req.body?.index, 10);

  if (!jobId || !Number.isInteger(index) || index < 1) {
    return res.status(400).json({
      error: 'Invalid link retry request.',
    });
  }

  if (status !== 'connected') {
    return res.status(400).json({
      error: 'WhatsApp is not connected.',
    });
  }

  const accountNumber = getConnectedNumber();
  const job = store.history.find(j => j.id === jobId);

  if (!job) {
    return res.status(404).json({
      error: 'The requested batch was not found.',
    });
  }

  if (job.accountNumber !== accountNumber) {
    return res.status(409).json({
      error:
        'Reconnect the same WhatsApp account that created this batch before retrying its link.',
    });
  }

  const result = (job.results || []).find(
    r => Number(r.index) === index
  );

  if (!result) {
    return res.status(404).json({
      error: 'The requested group result was not found.',
    });
  }

  if (result.link) {
    return res.json({
      ok: true,
      result,
    });
  }

  try {
    let link = '';
    try {
      const code = await sock.groupInviteCode(result.id);
      link = code ? `https://chat.whatsapp.com/${code}` : '';
    } catch (firstError) {
      await sleep(LINK_RETRY_DELAY);
      try {
        const code = await sock.groupInviteCode(result.id);
        link = code ? `https://chat.whatsapp.com/${code}` : '';
      } catch (secondError) {
        link = '';
      }
    }

    if (!link) {
      return res.status(503).json({
        error:
          'WhatsApp did not return an invite link yet. Please try the link retry again after a short while.',
      });
    }

    result.link = link;
    result.linkAvailable = true;
    result.linkError = null;

    persistSoon();

    return res.json({
      ok: true,
      result,
    });
  } catch (e) {
    return res.status(500).json({
      error: `Invite-link retry failed: ${e.message}`,
    });
  }
});

app.post('/api/job/pause', auth, (req, res) => {
  if (!currentJob) {
    return res.status(404).json({
      error: 'No active job.',
    });
  }

  currentJob.paused = true;

  log(
    `Job ${currentJob.id} paused`,
    'WARN'
  );

  persistSoon();

  res.json({ ok: true });
});

app.post('/api/job/resume', auth, (req, res) => {
  if (!currentJob) {
    return res.status(404).json({
      error: 'No active job.',
    });
  }

  currentJob.paused = false;

  log(
    `Job ${currentJob.id} resumed`,
    'INFO'
  );

  persistSoon();

  res.json({ ok: true });
});

app.post('/api/job/cancel', auth, (req, res) => {
  if (!currentJob) {
    return res.status(404).json({
      error: 'No active job.',
    });
  }

  currentJob.cancelled = true;
  currentJob.paused = false;

  log(
    `Job ${currentJob.id} cancellation requested`,
    'WARN'
  );

  persistSoon();

  res.json({ ok: true });
});

app.post('/api/job/retry', auth, (req, res) => {
  if (currentJob) {
    return res.status(409).json({
      error:
        'Another job is already active.',
    });
  }

  const id = String(
    req.body?.id || ''
  );

  const old = store.history.find(
    j => j.id === id
  );

  if (!old) {
    return res.status(404).json({
      error:
        'The requested job was not found.',
    });
  }

  const accountNumber =
    getConnectedNumber();

  if (
    !accountNumber ||
    accountNumber !== old.accountNumber
  ) {
    return res.status(409).json({
      error:
        'Reconnect the same WhatsApp account that created this batch before retrying it.',
    });
  }

  const failed = (old.results || [])
    .filter(r => r.status === 'failed')
    .map(r => r.name);

  if (!failed.length) {
    return res.status(400).json({
      error:
        'This job has no failed groups to retry.',
    });
  }

  const remaining =
    MAX_PER_DAY - usage(accountNumber);

  if (remaining <= 0) {
    return res.status(400).json({
      error:
        `Daily limit reached for +${accountNumber}.`,
    });
  }

  const names = failed.slice(
    0,
    Math.min(BATCH_MAX, remaining)
  );

  currentJob = {
    id:
      `RETRY-${Date.now().toString(36).toUpperCase()}`,

    accountNumber,
    prefix: old.prefix,
    start: old.start,
    requested: names.length,
    total: names.length,
    names,
    members: old.members,
    delay: FIXED_GAP,
    completed: 0,
    failed: 0,
    results: [],
    running: false,
    paused: false,
    cancelled: false,
    current: null,
    startedAt: null,
    finishedAt: null,

    validation: {
      retryOf: old.id,
      batchMax: BATCH_MAX,
      fixedGapSeconds: FIXED_GAP,
      accountNumber,
    },
  };

  res.json({
    ok: true,
    job: publicJob(currentJob),
  });

  runJob(currentJob).catch(e => {
    log(
      `Retry fatal error: ${e.message}`,
      'ERROR'
    );

    currentJob.running = false;
    currentJob.finishedAt = iso();

    addHistory(currentJob);
    currentJob = null;
  });
});

app.listen(PORT, async () => {
  loadStore();
  pruneOldDaily();
  await initDb();

  console.log(
    `Panel on port ${PORT}`
  );

  log(
    `Limits: ${BATCH_MAX}/batch, ${MAX_PER_DAY}/account/day, ${FIXED_GAP}s normal gap`,
    'INFO'
  );

  connectWhatsApp();
});
