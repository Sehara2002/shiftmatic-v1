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

// Serve frontend
app.use(express.static(path.join(__dirname, "public")));

// =====================================================
// In-memory cache
// =====================================================
const activeSessionByDevice = new Map(); // deviceId -> sessionId(number)
const latestStatusByDevice = new Map();  // deviceId -> status payload + serverTs
const latestTelemetryByDevice = new Map(); // deviceId -> {lat,lon,sid,deviceTs,serverTs}

async function loadActiveSessionsIntoCache() {
  const { rows } = await pool.query(`SELECT id, device_id FROM sessions WHERE is_active = true`);
  activeSessionByDevice.clear();
  for (const r of rows) activeSessionByDevice.set(String(r.device_id), Number(r.id));
  console.log(`✅ Loaded active sessions: ${rows.length}`);
}

// =====================================================
// MQTT SUBSCRIBER
// =====================================================
const MQTT_BROKER = process.env.MQTT_BROKER || "mqtt://35.182.195.177:1883";

// ✅ Subscribe ALL devices
const MQTT_SUB_TELEMETRY = process.env.MQTT_SUB_TELEMETRY || "shiftmatic/test/+/telemetry";
const MQTT_SUB_STATUS    = process.env.MQTT_SUB_STATUS    || "shiftmatic/test/+/status";

const MQTT_CMD_TOPIC_PREFIX = process.env.MQTT_CMD_TOPIC_PREFIX || "shiftmatic/test";

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
// MQTT message handler (supports your Arduino JSON)
// =====================================================
mqttClient.on("message", async (topic, message) => {
  const text = message.toString();

  let obj;
  try {
    obj = JSON.parse(text);
  } catch {
    console.error("❌ MQTT bad JSON:", text);
    return;
  }

  // ---------- STATUS ----------
  if (topic.endsWith("/status")) {
    const deviceId = String(obj.deviceId ?? obj.id ?? "");
    if (!deviceId) return;

    latestStatusByDevice.set(deviceId, { ...obj, deviceId, serverTs: Date.now() });
    return;
  }

  // ---------- TELEMETRY ----------
  if (topic.endsWith("/telemetry")) {
    const deviceId = String(obj.deviceId ?? obj.id ?? "");
    if (!deviceId) return;

    const lat = Number(obj.lat ?? obj.la);
    const lon = Number(obj.lon ?? obj.lo);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

    const sessionFromDevice = Number(obj.sessionId ?? obj.sid ?? 0);
    let sid = Number.isFinite(sessionFromDevice) && sessionFromDevice > 0
      ? sessionFromDevice
      : (activeSessionByDevice.get(deviceId) || 0);

    const deviceTs = Number(obj.ts ?? obj.t ?? 0) || null;

    // cache latest telemetry for fast /api/latest
    latestTelemetryByDevice.set(deviceId, { lat, lon, sid: sid || null, deviceTs, serverTs: Date.now() });

    if (!sid) {
      console.log("ℹ️ Telemetry ignored (no active session):", { deviceId, lat, lon });
      return;
    }

    try {
      const row = await insertCoordinate({
        deviceId,
        sessionId: sid,
        lat,
        lon,
        ts: deviceTs,
      });
      console.log("📩 MQTT SAVED:", row.id, row.device_id, row.session_id);
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

// Client config
app.get("/config.js", (req, res) => {
  const key = process.env.GOOGLE_MAP_API_KEY || "";
  res.setHeader("Content-Type", "application/javascript; charset=utf-8");
  res.end(`window.__CONFIG__ = ${JSON.stringify({ GOOGLE_MAP_API_KEY: key })};`);
});

// Latest telemetry (memory first, fallback DB)
app.get("/api/latest", async (req, res) => {
  const deviceId = String(req.query.deviceId || "");
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  const mem = latestTelemetryByDevice.get(deviceId);
  if (mem) {
    const ageSeconds = Math.floor((Date.now() - Number(mem.serverTs)) / 1000);
    return res.json({
      ok: true,
      latest: {
        device_id: deviceId,
        session_id: mem.sid,
        lat: mem.lat,
        lon: mem.lon,
        device_ts: mem.deviceTs,
        server_ts: mem.serverTs,
      },
      ageSeconds,
      source: "memory",
    });
  }

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

// ✅ Device status (from MQTT status cache)
app.get("/api/device-status", (req, res) => {
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

    // publish START command to device
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

    // publish STOP command
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

// History by session
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

// Optional HTTP telemetry (kept)
app.post("/api/telemetry", async (req, res) => {
  const { deviceId, lat, lon, ts, sessionId } = req.body || {};
  if (!deviceId) return res.status(400).json({ ok: false, error: "deviceId required" });

  const latNum = Number(lat);
  const lonNum = Number(lon);
  if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) {
    return res.status(400).json({ ok: false, error: "lat/lon must be numbers" });
  }

  let sid = Number(sessionId);
  if (!Number.isFinite(sid)) sid = activeSessionByDevice.get(String(deviceId)) || 0;

  if (!sid) {
    return res.status(409).json({ ok: false, error: "no active session" });
  }

  try {
    const row = await insertCoordinate({
      deviceId: String(deviceId),
      sessionId: sid,
      lat: latNum,
      lon: lonNum,
      ts: Number(ts) || null,
    });

    console.log("📩 HTTP SAVED:", row.id, row.device_id, row.session_id);
    res.json({ ok: true, received: row });
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
