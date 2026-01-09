require("dotenv").config();
const { connectRabbit, getChannel } = require("./rabbit");
const PumpLog = require("../models/PumpLog");

// ======================
// CONFIG LEVEL AIR
// ======================
const TANK_FULL_MAX = 15;   // cm (<= ini dianggap penuh)

// ======================
// CONFIG SOIL
// ======================
const SOIL_DRY = 40;
const SOIL_WET = 60;

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
  FILL_TANK: "FILL_TANK"
};

let currentState = STATES.IDLE;

// ======================
// LAST RELAY STATES
// ======================
let lastPumpSoilState    = null; // SOLENOID
let lastPumpHidroState   = null; // POMPA SANYO
let lastPumpNutrisiState = null;

// ======================
// SENSOR HELPERS
// ======================
function isSoilDry() {
  if (soilStates.size === 0) return false;
  return [...soilStates.values()].some(v => v < SOIL_DRY);
}

function isSoilWet() {
  if (soilStates.size === 0) return false;
  return [...soilStates.values()].every(v => v >= SOIL_WET);
}

function isTankFull() {
  if (globalState.distance_cm === null) return false;
  return globalState.distance_cm <= TANK_FULL_MAX;
}

// ======================
// FSM TRANSITION
// ======================
function updateState() {
  switch (currentState) {

    case STATES.IDLE:
      if (isSoilDry()) {
        currentState = STATES.IRRIGATE_SOIL;
      } else if (!isTankFull()) {
        currentState = STATES.FILL_TANK;
      }
      break;

    case STATES.IRRIGATE_SOIL:
      if (isSoilWet()) {
        if (!isTankFull()) {
          currentState = STATES.FILL_TANK;
        } else {
          currentState = STATES.IDLE;
        }
      }
      break;

    case STATES.FILL_TANK:
      if (isTankFull()) {
        if (isSoilDry()) {
          currentState = STATES.IRRIGATE_SOIL;
        } else {
          currentState = STATES.IDLE;
        }
      }
      break;
  }
}

// ======================
// FSM ACTIONS
// ======================
function decideByState() {
  switch (currentState) {

    // Soil kering → air ke soil
    case STATES.IRRIGATE_SOIL:
      return {
        soil:  { action: "OFF", reason: "fsm_irrigate_soil" }, // solenoid CLOSE
        hidro: { action: "ON",  reason: "fsm_irrigate_soil" }  // pompa ON
      };

    // Isi bak
    case STATES.FILL_TANK:
      return {
        soil:  { action: "ON",  reason: "fsm_fill_tank" }, // solenoid OPEN
        hidro: { action: "ON",  reason: "fsm_fill_tank" }  // pompa ON
      };

    // Aman
    case STATES.IDLE:
    default:
      return {
        soil:  { action: "ON",  reason: "fsm_idle" }, // arah ke bak
        hidro: { action: "OFF", reason: "fsm_idle" }  // pompa mati
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
// DASHBOARD (OVERWRITE)
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
  updateState();

  const fsmDecision      = decideByState();
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

  console.log("✅ FSM HYDROPONIC CONTROLLER ACTIVE");
}

module.exports = { startConsumers };
