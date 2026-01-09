require("dotenv").config();
const { connectRabbit, getChannel } = require("./rabbit");
const PumpLog = require("../models/PumpLog");

// ======================
// CONFIG LEVEL AIR
// ======================
const TANK_FULL_MIN = 10;
const TANK_FULL_MAX = 20;
const TANK_LOW_CM   = 25;

// ======================
// CONFIG SOIL
// ======================
const SOIL_DRY = 60;
const SOIL_WET = 40;

// ======================
// GLOBAL STATES
// ======================
const globalState = {
  distance_cm: null,
  tds_ppm: null
};

const soilStates = new Map();

// ======================
// LAST STATES
// ======================
let lastPumpSoilState    = null; // SOLENOID
let lastPumpHidroState   = null; // POMPA SANYO
let lastPumpNutrisiState = null;

// ======================
// DECISION: SOIL (SOLENOID)
// OFF = CLOSE = AIR KE SOIL
// ON  = OPEN  = AIR KE BAK
// ======================
function decidePumpSoil() {
  if (soilStates.size === 0)
    return { action: "HOLD", reason: "no_soil_data" };

  const values = [...soilStates.values()];

  // Soil kering → tutup solenoid (air ke soil)
  if (values.some(v => v < SOIL_DRY))
    return { action: "OFF", reason: "soil_dry_direct_to_soil" };

  // Soil lembab / basah → buka solenoid (air ke bak)
  if (values.every(v => v >= SOIL_WET))
    return { action: "ON", reason: "soil_wet_direct_to_tank" };

  return { action: "HOLD", reason: "soil_normal" };
}

// ======================
// DECISION: HIDRO / SANYO
// HANYA BERDASARKAN LEVEL BAK
// ======================
function decidePumpHidroponic() {
  if (globalState.distance_cm === null)
    return { action: "HOLD", reason: "no_ultrasonic_data" };

  const d = globalState.distance_cm;

  // Bak penuh → matikan pompa
  if (d <= TANK_FULL_MAX)
    return { action: "OFF", reason: "tank_full_stop_pump" };

  // Bak belum penuh → nyalakan pompa
  return { action: "ON", reason: "tank_not_full_fill" };
}

// ======================
// DECISION: NUTRISI (TDS)
// ======================
function decidePumpNutrisi() {
  if (globalState.tds_ppm === null)
    return { action: "OFF", reason: "no_tds_data" };

  if (globalState.tds_ppm < 1 || globalState.tds_ppm > 3000)
    return { action: "HOLD", reason: "tds_invalid" };

  if (globalState.tds_ppm < 79)
    return { action: "ON", reason: "tds_low" };

  return { action: "OFF", reason: "tds_ok" };
}

// ======================
// DASHBOARD (LOG)
// ======================
function renderDashboard(soilDecision, hidroDecision, nutrisiDecision) {
  console.log("\n=== HYDROPONIC CONTROL DASHBOARD ===");
  console.log("Time:", new Date().toLocaleString());

  const soilValues = [...soilStates.values()];
  const soilAvg =
    soilValues.length > 0
      ? (soilValues.reduce((a, b) => a + b, 0) / soilValues.length).toFixed(1)
      : "-";

  console.log("\n[ DECISION ]");
  console.table([
    {
      Subsystem: "SOIL (SOLENOID)",
      Action: soilDecision.action,
      Reason: soilDecision.reason,
      Value: soilAvg !== "-" ? `${soilAvg} %` : "-"
    },
    {
      Subsystem: "HIDRO / SANYO",
      Action: hidroDecision.action,
      Reason: hidroDecision.reason,
      Value:
        globalState.distance_cm !== null
          ? `${globalState.distance_cm} cm`
          : "-"
    },
    {
      Subsystem: "NUTRISI",
      Action: nutrisiDecision.action,
      Reason: nutrisiDecision.reason,
      Value:
        globalState.tds_ppm !== null
          ? `${globalState.tds_ppm} ppm`
          : "-"
    }
  ]);

  console.log("[ PUMP STATUS ]");
  console.table([
    { Pump: "pump-soil (solenoid)", Status: lastPumpSoilState ?? "-" },
    { Pump: "pump-hidroponic",      Status: lastPumpHidroState ?? "-" },
    { Pump: "pump-nutrisi",         Status: lastPumpNutrisiState ?? "-" }
  ]);
}

// ======================
// PUBLISH RELAY (UNCHANGED)
// ======================
async function publishRelay(channel, {
  pump,
  relay,
  topic,
  decision,
  lastState
}) {
  const { action, reason } = decision;
  if (action === "HOLD") return;
  if (lastState === action) return;

  const payload = {
    pump,
    relay,
    action,
    reason,
    timestamp: new Date()
  };

  channel.publish(
    "amq.topic",
    topic,
    Buffer.from(JSON.stringify(payload)),
    { persistent: true }
  );

  try {
    await PumpLog.create(payload);
  } catch (_) {}

  return action;
}

// ======================
// EVALUATION ENGINE
// ======================
async function evaluate(channel) {
  const soilDecision    = decidePumpSoil();
  const hidroDecision   = decidePumpHidroponic();
  const nutrisiDecision = decidePumpNutrisi();

  const soilRes = await publishRelay(channel, {
    pump: "soil",
    relay: "pump-soil",
    topic: "control.relay.soil",
    decision: soilDecision,
    lastState: lastPumpSoilState
  });
  if (soilRes) lastPumpSoilState = soilRes;

  const hidroRes = await publishRelay(channel, {
    pump: "hidroponic",
    relay: "pump-hidroponic",
    topic: "control.relay.hidroponic",
    decision: hidroDecision,
    lastState: lastPumpHidroState
  });
  if (hidroRes) lastPumpHidroState = hidroRes;

  const nutrisiRes = await publishRelay(channel, {
    pump: "nutrisi",
    relay: "pump-nutrisi",
    topic: "control.relay.nutrisi",
    decision: nutrisiDecision,
    lastState: lastPumpNutrisiState
  });
  if (nutrisiRes) lastPumpNutrisiState = nutrisiRes;

  renderDashboard(soilDecision, hidroDecision, nutrisiDecision);
}

// ======================
// CONSUMERS
// ======================
async function startConsumers() {
  await connectRabbit();
  const channel = getChannel();

  await channel.assertQueue(process.env.SOIL_QUEUE, { durable: true });
  channel.consume(process.env.SOIL_QUEUE, async msg => {
    const d = JSON.parse(msg.content.toString());
    if (d.ip && d.kelembaban_tanah !== undefined) {
      soilStates.set(d.ip, Number(d.kelembaban_tanah));
      await evaluate(channel);
    }
    channel.ack(msg);
  });

  await channel.assertQueue(process.env.ULTRASONIC_QUEUE, { durable: true });
  channel.consume(process.env.ULTRASONIC_QUEUE, async msg => {
    globalState.distance_cm = JSON.parse(msg.content.toString()).distance_cm;
    await evaluate(channel);
    channel.ack(msg);
  });

  await channel.assertQueue(process.env.TDS_QUEUE, { durable: true });
  channel.consume(process.env.TDS_QUEUE, async msg => {
    globalState.tds_ppm = JSON.parse(msg.content.toString()).tds_ppm;
    await evaluate(channel);
    channel.ack(msg);
  });
}

module.exports = { startConsumers };
