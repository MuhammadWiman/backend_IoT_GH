require("dotenv").config();
const { connectRabbit, getChannel } = require("./rabbit");
const PumpLog = require("../models/PumpLog");

// ======================
// CONFIG LEVEL AIR
// ======================
const TANK_FULL_MAX = 15;

// ======================
// CONFIG SOIL
// ======================
const SOIL_DRY = 40;
const SOIL_WET = 60;

// ======================
// PULSE CONFIG (KHUSUS SOIL KERING + BAK KOSONG)
// ======================
const PULSE_SOIL_MS = 15000;
const PULSE_TANK_MS = 15000;

let pulseTarget = "SOIL";
let lastPulseSwitch = Date.now();

// ======================
// GLOBAL STATES
// ======================
const globalState = {
  distance_cm: null,
  tds_ppm: null
};

const soilStates = new Map();

// ======================
// FSM STATES
// ======================
const STATES = {
  IDLE: "IDLE",
  IRRIGATE_SOIL: "IRRIGATE_SOIL",
  FILL_TANK: "FILL_TANK",
  PULSE_DISTRIBUTION: "PULSE_DISTRIBUTION"
};

let currentState = STATES.IDLE;

// ======================
// LAST RELAY STATES
// ======================
let lastPumpSoilState    = null;
let lastPumpHidroState   = null;
let lastPumpNutrisiState = null;

// ======================
// SENSOR HELPERS
// ======================
function isSoilDry() {
  return soilStates.size > 0 &&
    [...soilStates.values()].some(v => v < SOIL_DRY);
}

function isSoilWet() {
  return soilStates.size > 0 &&
    [...soilStates.values()].every(v => v >= SOIL_WET);
}

function isTankFull() {
  return globalState.distance_cm !== null &&
    globalState.distance_cm <= TANK_FULL_MAX;
}

// ======================
// FSM TRANSITION (FIXED)
// ======================
function updateState() {
  switch (currentState) {

    case STATES.IDLE:
      if (isSoilDry() && !isTankFull()) {
        currentState = STATES.PULSE_DISTRIBUTION;
      } else if (isSoilDry()) {
        currentState = STATES.IRRIGATE_SOIL;
      } else if (!isTankFull()) {
        currentState = STATES.FILL_TANK;
      }
      break;

    case STATES.PULSE_DISTRIBUTION:
      if (isSoilWet()) {
        currentState = isTankFull()
          ? STATES.IDLE
          : STATES.FILL_TANK;
      }
      break;

    case STATES.IRRIGATE_SOIL:
      if (isSoilWet()) {
        currentState = isTankFull()
          ? STATES.IDLE
          : STATES.FILL_TANK;
      }
      break;

    case STATES.FILL_TANK:
      if (isTankFull()) {
        currentState = isSoilDry()
          ? STATES.IRRIGATE_SOIL
          : STATES.IDLE;
      }
      break;
  }
}

// ======================
// FSM ACTIONS
// ======================
function decideByState() {
  const now = Date.now();

  switch (currentState) {

    case STATES.PULSE_DISTRIBUTION: {
      const duration =
        pulseTarget === "SOIL" ? PULSE_SOIL_MS : PULSE_TANK_MS;

      if (now - lastPulseSwitch >= duration) {
        pulseTarget = pulseTarget === "SOIL" ? "TANK" : "SOIL";
        lastPulseSwitch = now;
      }

      return pulseTarget === "SOIL"
        ? {
            soil:  { action: "OFF", reason: "pulse_to_soil" },
            hidro: { action: "ON",  reason: "pulse_to_soil" }
          }
        : {
            soil:  { action: "ON",  reason: "pulse_to_tank" },
            hidro: { action: "ON",  reason: "pulse_to_tank" }
          };
    }

    case STATES.IRRIGATE_SOIL:
      return {
        soil:  { action: "OFF", reason: "fsm_irrigate_soil" },
        hidro: { action: "ON",  reason: "fsm_irrigate_soil" }
      };

    case STATES.FILL_TANK:
      return {
        soil:  { action: "ON",  reason: "fsm_fill_tank" },
        hidro: { action: "ON",  reason: "fsm_fill_tank" }
      };

    case STATES.IDLE:
    default:
      return {
        soil:  { action: "ON",  reason: "fsm_idle" },
        hidro: { action: "OFF", reason: "fsm_idle" }
      };
  }
}

// ======================
// NUTRISI (TIDAK DIUBAH)
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
// DASHBOARD (TIDAK DIUBAH)
// ======================
function renderDashboard(fsmDecision, nutrisiDecision) {
  console.clear();
  console.log("=== HYDROPONIC CONTROL DASHBOARD ===");
  console.log("Time:", new Date().toLocaleString());
  console.log("FSM State:", currentState);

  const soilValues = [...soilStates.values()];
  const soilAvg =
    soilValues.length > 0
      ? (soilValues.reduce((a, b) => a + b, 0) / soilValues.length).toFixed(1)
      : "-";

  console.log("\n[ DECISION ]");
  console.table([
    {
      Subsystem: "SOIL (SOLENOID)",
      Action: fsmDecision.soil.action,
      Reason: fsmDecision.soil.reason,
      Value: soilAvg !== "-" ? `${soilAvg} %` : "-"
    },
    {
      Subsystem: "HIDRO / SANYO",
      Action: fsmDecision.hidro.action,
      Reason: fsmDecision.hidro.reason,
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
  pump, relay, topic, decision, lastState
}) {
  const { action, reason } = decision;
  if (action === "HOLD") return;
  if (lastState === action) return;

  const payload = { pump, relay, action, reason, timestamp: new Date() };

  channel.publish(
    "amq.topic",
    topic,
    Buffer.from(JSON.stringify(payload)),
    { persistent: true }
  );

  try { await PumpLog.create(payload); } catch (_) {}
  return action;
}

// ======================
// EVALUATION ENGINE
// ======================
async function evaluate(channel) {
  updateState();

  const fsmDecision = decideByState();
  const nutrisiDecision = decidePumpNutrisi();

  const soilRes = await publishRelay(channel, {
    pump: "soil",
    relay: "pump-soil",
    topic: "control.relay.soil",
    decision: fsmDecision.soil,
    lastState: lastPumpSoilState
  });
  if (soilRes) lastPumpSoilState = soilRes;

  const hidroRes = await publishRelay(channel, {
    pump: "hidroponic",
    relay: "pump-hidroponic",
    topic: "control.relay.hidroponic",
    decision: fsmDecision.hidro,
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

  renderDashboard(fsmDecision, nutrisiDecision);
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
    globalState.distance_cm =
      JSON.parse(msg.content.toString()).distance_cm;
    await evaluate(channel);
    channel.ack(msg);
  });

  await channel.assertQueue(process.env.TDS_QUEUE, { durable: true });
  channel.consume(process.env.TDS_QUEUE, async msg => {
    globalState.tds_ppm =
      JSON.parse(msg.content.toString()).tds_ppm;
    await evaluate(channel);
    channel.ack(msg);
  });

  console.log("✅ FSM HYDROPONIC CONTROLLER ACTIVE");
}

module.exports = { startConsumers };
