const express = require("express");
const cors = require("cors");
const path = require("path");
const mqtt = require("mqtt");

const app = express();
const PORT = process.env.PORT || 3000;

// ===== CORS =====
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

// JSON body parser
app.use(express.json({ limit: "50kb" }));

// ===== Request Logger (ALL API CALLS) =====
app.use((req, res, next) => {
  const start = Date.now();

  res.on("finish", () => {
    const ms = Date.now() - start;
    const iso = new Date().toISOString();
    const local = new Date().toLocaleString();
    const url = req.originalUrl;

    console.log(
      `[${iso}] [${local}] ${req.method} ${url} -> ${res.statusCode} (${ms}ms)`
    );
  });

  next();
});

// Serve frontend
app.use(express.static(path.join(__dirname, "public")));

// ===== In-memory storage =====
const store = [];
const MAX_POINTS = 20000;

function prune() {
  if (store.length > MAX_POINTS) store.splice(0, store.length - MAX_POINTS);
}

// =====================================================
// =============== MQTT SUBSCRIBER SECTION ==============
// =====================================================
// Public broker (testing)
const MQTT_BROKER = "mqtt://broker.hivemq.com:1883";
const MQTT_TOPIC = "shiftmatic/test/esp32-001/telemetry"; // MUST match ESP

const mqttClient = mqtt.connect(MQTT_BROKER, {
  keepalive: 30,
  reconnectPeriod: 2000,
  connectTimeout: 10000,
});

mqttClient.on("connect", () => {
  console.log("✅ MQTT connected:", MQTT_BROKER);
  mqttClient.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
    if (err) console.error("❌ MQTT subscribe error:", err);
    else console.log("✅ MQTT subscribed:", MQTT_TOPIC);
  });
});

mqttClient.on("error", (e) => {
  console.error("❌ MQTT error:", e.message);
});

mqttClient.on("message", (topic, message) => {
  const text = message.toString();

  try {
    const data = JSON.parse(text);
    const { deviceId, lat, lon, ts } = data || {};

    const latNum = Number(lat);
    const lonNum = Number(lon);

    if (!deviceId) return;
    if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) return;

    const entry = {
      deviceId: String(deviceId),
      lat: latNum,
      lon: lonNum,
      ts: Number(ts) || Date.now(),   // device timestamp (optional)
      serverTs: Date.now(),           // server receive time
      iso: new Date().toISOString(),  // readable
      via: "MQTT",
    };

    store.push(entry);
    prune();

    console.log("📩 MQTT TELEMETRY:", entry);
  } catch (e) {
    console.error("❌ MQTT bad JSON:", text);
  }
});

// ===== Endpoints =====

// Health
app.get("/api/health", (req, res) => {
  res.json({ ok: true, status: "UP", now: Date.now() });
});

// Receive telemetry via HTTP (optional — still works)
app.post("/api/telemetry", (req, res) => {
  const { deviceId, lat, lon, ts } = req.body || {};

  console.log("   TELEMETRY BODY:", {
    deviceId,
    lat,
    lon,
    ts,
    serverTs: Date.now(),
  });

  if (!deviceId) {
    return res.status(400).json({ ok: false, error: "deviceId required" });
  }

  const latNum = Number(lat);
  const lonNum = Number(lon);

  if (!Number.isFinite(latNum) || !Number.isFinite(lonNum)) {
    return res
      .status(400)
      .json({ ok: false, error: "lat/lon must be numbers" });
  }

  const entry = {
    deviceId: String(deviceId),
    lat: latNum,
    lon: lonNum,
    ts: Number(ts) || Date.now(),
    serverTs: Date.now(),
    iso: new Date().toISOString(),
    via: "HTTP",
  };

  store.push(entry);
  prune();

  return res.json({ ok: true, received: entry });
});

// Latest
app.get("/api/latest", (req, res) => {
  const deviceId = String(req.query.deviceId || "");
  if (!deviceId)
    return res.status(400).json({ ok: false, error: "deviceId required" });

  for (let i = store.length - 1; i >= 0; i--) {
    if (store[i].deviceId === deviceId) {
      const latest = store[i];
      const ageSeconds = Math.floor((Date.now() - latest.serverTs) / 1000);
      return res.json({ ok: true, latest, ageSeconds });
    }
  }

  return res.status(404).json({ ok: false, error: "no data for device" });
});

// History
app.get("/api/history", (req, res) => {
  const deviceId = String(req.query.deviceId || "");
  const limit = Math.min(Number(req.query.limit || 2000), 5000);

  if (!deviceId)
    return res.status(400).json({ ok: false, error: "deviceId required" });

  const points = store.filter((p) => p.deviceId === deviceId);
  const sliced = points.slice(Math.max(0, points.length - limit));

  return res.json({
    ok: true,
    deviceId,
    count: sliced.length,
    points: sliced,
    serverNow: Date.now(),
    serverIso: new Date().toISOString(),
  });
});

// Start
app.listen(PORT, () => {
  console.log(`✅ Express tracker running on http://localhost:${PORT}`);
  console.log(`✅ Map UI: http://localhost:${PORT}/`);
  console.log(`✅ Render URL: (your live) https://shiftmatic-v1.onrender.com/`);
});
