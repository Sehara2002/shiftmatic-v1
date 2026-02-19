require("dotenv").config();
const express = require("express");
const cors = require("cors");
const path = require("path");
const mqtt = require("mqtt");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

// ===== DB =====
const pool = new Pool({
  connectionString: process.env.CONNECTION_STRING,
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
});

// ===== CORS =====
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

app.use(express.json({ limit: "50kb" }));
app.use(express.static(path.join(__dirname, "public")));

// ===== Request Logger =====
app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - start;
    const iso = new Date().toISOString();
    const local = new Date().toLocaleString();
    console.log(`[${iso}] [${local}] ${req.method} ${req.originalUrl} -> ${res.statusCode} (${ms}ms)`);
  });
  next();
});

// =====================================================
// In-memory cache
// =====================================================
const activeSessionByDevice = new Map(); // deviceId -> sessionId(number)

// latest live data caches (no DB read needed for UI)
const latestTelemetryByDevice = new Map(); // deviceId -> {lat, lon, sid, deviceTs, serverTs}
const latestStatusByDevice = new Map();    // deviceId -> { ...statusPayload, serverTs }

async function loadActiveSessionsIntoCache() {
  const { rows } = await pool.query(`SELECT id, device_id FROM sessions WHERE is_active = true`);
  activeSessionByDevice.clear();
  for (const r of rows) activeSessionByDevice.set(String(r.device_id), Number(r.id));
  console.log(`✅ Loaded active sessions: ${rows.length}`);
}

// =====================================================
// MQTT
// =====================================================
const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://35.182.195.177:1883";

// Subscribe to ALL devices under shiftmatic/test/+/telemetry and shiftmatic/test/+/status
// (works even if you add more devices later)
const MQTT_SUB_TELEMETRY = process.env.MQTT_SUB_TELEMETRY || "shiftmatic/test/+/telemetry";
const MQTT_SUB_STATUS    = process.env.MQTT_SUB_STATUS    || "shiftmatic/test/+/status";

const MQTT_CMD_TOPIC_PREFIX = process.env.MQTT_CMD_TOPIC_PREFIX || "shiftmatic/test";
// device command topic: `${MQTT_CMD_TOPIC_PREFIX}/${deviceId}/cmd`

const mqttClient = mqtt.connect(MQTT_BROKER, {
  keepalive: 30,
  reconnectPeriod: 2000,
  connectTimeout: 10000,
});

mqttClient.on("connect", async () => {
  console.log("✅ MQTT connected:", MQTT_BROKER);

  mqttClient.subscribe(MQTT_SUB_TELEMETRY, { qos: 0 }, (err) => {
    if (err) console.error("❌ MQTT subscribe telemetry error:", err);
    else console.log("✅ MQTT subscribed telemetry:", MQTT_SUB_TELEMETRY);
  });

  mqttClient.subscribe(MQTT_SUB_STATUS, { qos: 0 }, (err) => {
    if (err) console.error("❌ MQTT subscribe status error:", err);
    else console.log("✅ MQTT subscribed status:", MQTT_SUB_STATUS);
  });

  try {
    await loadActiveSessionsIntoCache();
  } catch (e) {
    console.error("❌ loadActiveSessionsIntoCache:", e.message);
  }
});

mqttClient.on("error", (e) => console.error("❌ MQTT error:", e.message));

// =====================================================
// DB insert
// =====================================================
async function insertCoordinate({ deviceId, sessionId, lat, lon, ts }) {
  const serverTs = Date.now();
  const q = `
    INSERT INTO coordinates(session_id, device_id, lat, lon, device_ts, server_ts)
    VALUES ($1,$2,$3,$4,$5,$6)
    RETURNING id, session_id, device_id, lat, lon, device_ts, server_ts, created_at
  `;
  const vals = [sessionId, deviceId, lat, lon, ts ?? null, serverTs];
  const { rows } = await pool.query(q, vals);
  return rows[0];
}

// =====================================================
// MQTT message handler (supports BOTH device JSON formats)
// =====================================================
function normalizeTelemetry(obj) {
  // Device format: {id, sid, la, lo, t}
  // Server expected: {deviceId, sessionId, lat, lon, ts}
  const deviceId = String(obj.deviceId ?? obj.id ?? "");
  const sessionId = Number(obj.sessionId ?? obj.sid ?? 0);

  const lat = Number(obj.lat ?? obj.la);
  const lon = Number(obj.lon ?? obj.lo);

  const ts = Number(obj.ts ?? obj.t ?? obj.device_ts ?? 0) || null;

  return { deviceId, sessionId, lat, lon, ts };
}

function normalizeStatus(obj) {
  // Your device publishes: {"id","sid","up","heap","gps","age","sa","hd","sp","net","gprs","csq","dbm","mq","mqs","pl","pok","t"}
  // Keep as-is, just normalize id field
  const deviceId = String(obj.deviceId ?? obj.id ?? "");
  return { deviceId, ...obj };
}

mqttClient.on("message", async (topic, message) => {
  const text = message.toString();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    console.error("❌ MQTT bad JSON:", text);
    return;
  }

  // Decide type by topic ending
  const isStatus = topic.endsWith("/status");
  const isTelemetry = topic.endsWith("/telemetry");

  if (isStatus) {
    const st = normalizeStatus(data);
    if (!st.deviceId) return;

    latestStatusByDevice.set(st.deviceId, { ...st, serverTs: Date.now() });
    // Optional log:
    // console.log("📟 STATUS:", st.deviceId, st.gps, st.net, st.gprs, st.csq, st.dbm);
    return;
  }

  if (isTelemetry) {
    const t = normalizeTelemetry(data);
    if (!t.deviceId) return;

    if (!Number.isFinite(t.lat) || !Number.isFinite(t.lon)) return;

    // sessionId preference: from device, else active cache
    let sid = Number(t.sessionId);
    if (!Number.isFinite(sid) || sid <= 0) sid = activeSessionByDevice.get(t.deviceId) || 0;

    // cache latest telemetry even if no session (for live marker if you want)
    latestTelemetryByDevice.set(t.deviceId, {
      lat: t.lat,
      lon: t.lon,
      sid: sid || null,
      deviceTs: t.ts,
      serverTs: Date.now(),
    });

    if (!sid) {
      console.log("ℹ️ Telemetry ignored (no active session):", { deviceId: t.deviceId, lat: t.lat, lon: t.lon });
      return;
    }

    try {
      const row = await insertCoordinate({
        deviceId: t.deviceId,
        sessionId: sid,
        lat: t.lat,
        lon: t.lon,
        ts: t.ts,
      });

      console.log("📩 MQTT SAVED:", row.id, row.device_id, row.session_id, row.lat, row.lon);
    } catch (e) {
      console.error("❌ DB insert failed:", e.message);
    }
  }
});

// =====================================================
// API
// =====================================================

// Health
app.get("/api/health", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, status: "UP", now: Date.now(), dbNow: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Expose Google Maps key to frontend
app.get("/config.js", (req, res) => {
  const key = process.env.GOOGLE_MAP_API_KEY || "";
  res.setHeader("Content-Type", "application/javascript; charset=utf-8");
  res.end(`window.__CONFIG__ = ${JSON.stringify({ GOOGLE_MAP_API_KEY: key })};`);
});

// Latest point (fast: from memory first, fallback to DB)
app.get("/api/latest", async (req, res) => {
  const deviceId = String(req.query.deviceId || "");
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  const cached = latestTelemetryByDevice.get(deviceId);
  if (cached) {
    const ageSeconds = Math.floor((Date.now() - Number(cached.serverTs)) / 1000);
    return res.json({
      ok: true,
      latest: {
        device_id: deviceId,
        session_id: cached.sid,
        lat: cached.lat,
        lon: cached.lon,
        device_ts: cached.deviceTs,
        server_ts: cached.serverTs,
      },
      ageSeconds,
      source: "memory",
    });
  }

  // fallback
  try {
    const { rows } = await pool.query(
      `SELECT c.*
       FROM coordinates c
       WHERE c.device_id=$1
       ORDER BY c.created_at DESC
       LIMIT 1`,
      [deviceId]
    );

    if (!rows.length) return res.status(404).json({ ok: false, error: "no data for device" });

    const latest = rows[0];
    const ageSeconds = Math.floor((Date.now() - Number(latest.server_ts)) / 1000);

    res.json({ ok: true, latest, ageSeconds, source: "db" });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Device status (from MQTT status cache)
app.get("/api/device-status", async (req, res) => {
  const deviceId = String(req.query.deviceId || "");
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  const st = latestStatusByDevice.get(deviceId);
  if (!st) return res.status(404).json({ ok: false, error: "no status yet" });

  const ageSeconds = Math.floor((Date.now() - Number(st.serverTs)) / 1000);
  res.json({ ok: true, deviceId, status: st, ageSeconds });
});

// Start new session
app.post("/api/sessions/start", async (req, res) => {
  const { deviceId, title } = req.body || {};
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  const device = String(deviceId);

  try {
    await pool.query(
      `UPDATE sessions SET is_active=false, ended_at=NOW()
       WHERE device_id=$1 AND is_active=true`,
      [device]
    );

    const { rows } = await pool.query(
      `INSERT INTO sessions(device_id, title, is_active)
       VALUES ($1,$2,true)
       RETURNING id, device_id, title, started_at, is_active`,
      [device, title || null]
    );

    const session = rows[0];
    const sessionId = Number(session.id);

    activeSessionByDevice.set(device, sessionId);

    const cmdTopic = `${MQTT_CMD_TOPIC_PREFIX}/${device}/cmd`;
    const cmd = JSON.stringify({ cmd: "START", sessionId });
    mqttClient.publish(cmdTopic, cmd);

    console.log("▶️ SESSION START:", { device, sessionId, cmdTopic });

    res.json({ ok: true, session, cmdTopic });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Stop session
app.post("/api/sessions/stop", async (req, res) => {
  const { deviceId } = req.body || {};
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  const device = String(deviceId);

  try {
    const sid = activeSessionByDevice.get(device);

    const { rows: activeRows } = sid
      ? { rows: [{ id: sid }] }
      : await pool.query(
          `SELECT id FROM sessions WHERE device_id=$1 AND is_active=true ORDER BY started_at DESC LIMIT 1`,
          [device]
        );

    if (!activeRows.length) {
      return res.status(404).json({ ok: false, error: "no active session" });
    }

    const sessionId = Number(activeRows[0].id);

    await pool.query(
      `UPDATE sessions SET is_active=false, ended_at=NOW()
       WHERE id=$1`,
      [sessionId]
    );

    activeSessionByDevice.delete(device);

    const cmdTopic = `${MQTT_CMD_TOPIC_PREFIX}/${device}/cmd`;
    const cmd = JSON.stringify({ cmd: "STOP", sessionId });
    mqttClient.publish(cmdTopic, cmd);

    console.log("⏹️ SESSION STOP:", { device, sessionId, cmdTopic });

    res.json({ ok: true, deviceId: device, sessionId, cmdTopic });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// List sessions
app.get("/api/sessions", async (req, res) => {
  const deviceId = String(req.query.deviceId || "");
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  try {
    const { rows } = await pool.query(
      `SELECT id, device_id, title, started_at, ended_at, is_active
       FROM sessions
       WHERE device_id=$1
       ORDER BY started_at DESC
       LIMIT 200`,
      [deviceId]
    );

    res.json({
      ok: true,
      deviceId,
      activeSessionId: activeSessionByDevice.get(deviceId) || null,
      sessions: rows,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Session history
app.get("/api/session-history", async (req, res) => {
  const sessionId = Number(req.query.sessionId || 0);
  const limit = Math.min(Number(req.query.limit || 5000), 20000);

  if (!Number.isFinite(sessionId) || sessionId <= 0) {
    return res.status(400).json({ ok: false, error: "sessionId required" });
  }

  try {
    const { rows } = await pool.query(
      `SELECT lat, lon, device_ts, server_ts, created_at
       FROM coordinates
       WHERE session_id=$1
       ORDER BY created_at ASC
       LIMIT $2`,
      [sessionId, limit]
    );

    res.json({
      ok: true,
      sessionId,
      count: rows.length,
      points: rows,
      serverNow: Date.now(),
      serverIso: new Date().toISOString(),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Start server
(async () => {
  try {
    await pool.query("SELECT 1");
    console.log("✅ DB connected");
    await loadActiveSessionsIntoCache();
  } catch (e) {
    console.error("❌ DB connect error:", e.message);
  }

  app.listen(PORT, () => {
    console.log(`✅ Express tracker running on http://localhost:${PORT}`);
    console.log(`✅ Map UI: http://localhost:${PORT}/`);
  });
})();
