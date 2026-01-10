require("dotenv").config();
const readline = require("readline");
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
  MANUAL_SANYO: "MANUAL_SANYO"
};

let currentState = STATES.IDLE;

// ======================
// MANUAL FLAG
// ======================
let manualSanyoActive = false;

// ======================
// LAST RELAY STATES (DIPISAH WAJIB)
// ======================
let lastPumpHidroState      = null;
let lastSolenoidHidroState  = null;
let lastSolenoidSoilState   = null;
let lastPumpNutrisiState    = null;

// ======================
// SENSOR HELPERS
// ======================
function hasTankData() {
  return globalState.distance_cm !== null;
}

function isSoilDry() {
  return soilStates.size > 0 &&
    [...soilStates.values()].some(v => v < SOIL_DRY);
}

function isSoilWet() {
  return soilStates.size > 0 &&
    [...soilStates.values()].every(v => v >= SOIL_WET);
}

function isTankFull() {
  return hasTankData() &&
    globalState.distance_cm <= TANK_FULL_MAX;
}

// ======================
// FSM TRANSITION (TIDAK DIUBAH)
// ======================
function updateState() {
  if (manualSanyoActive) {
    currentState = STATES.MANUAL_SANYO;
    return;
  }

  switch (currentState) {
    case STATES.IDLE:
      if (!hasTankData()) break;
      if (!isTankFull()) currentState = STATES.FILL_TANK;
      else if (isSoilDry()) currentState = STATES.IRRIGATE_SOIL;
      break;

    case STATES.FILL_TANK:
      if (isTankFull())
        currentState = isSoilDry()
          ? STATES.IRRIGATE_SOIL
          : STATES.IDLE;
      break;

    case STATES.IRRIGATE_SOIL:
      if (isSoilWet()) currentState = STATES.IDLE;
      break;
  }
}

// ======================
// ACTUATOR LOGIC (BARU, FSM TETAP)
// ======================
function decideActuators() {
  switch (currentState) {
    case STATES.FILL_TANK:
      return {
        pump:      { action: "ON",  reason: "fsm_fill_tank" },
        soil:      { action: "OFF", reason: "fsm_fill_tank" },
        hidro:     { action: "ON",  reason: "fsm_fill_tank" }
      };

    case STATES.IRRIGATE_SOIL:
      return {
        pump:      { action: "ON",  reason: "fsm_irrigate_soil" },
        soil:      { action: "ON",  reason: "fsm_irrigate_soil" },
        hidro:     { action: "OFF", reason: "fsm_irrigate_soil" }
      };

    case STATES.MANUAL_SANYO:
      return {
        pump:      { action: "ON",  reason: "manual_sanyo" },
        soil:      { action: "OFF", reason: "manual_sanyo" },
        hidro:     { action: "ON",  reason: "manual_sanyo" }
      };

    case STATES.IDLE:
    default:
      return {
        pump:      { action: "OFF", reason: "fsm_idle" },
        soil:      { action: "OFF", reason: "fsm_idle" },
        hidro:     { action: "OFF", reason: "fsm_idle" }
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
    return { action: "OFF", reason: "tds_invalid" };

  if (globalState.tds_ppm < 79)
    return { action: "ON", reason: "tds_low" };

  return { action: "OFF", reason: "tds_ok" };
}

// ======================
// DASHBOARD (TIDAK DIUBAH)
// ======================
function renderDashboard(act, nutrisi) {
  readline.cursorTo(process.stdout, 0, 0);
  readline.clearScreenDown(process.stdout);

  console.log("FSM STATE:", currentState);
  console.table([
    { Actuator: "Pump Hidro",     Status: act.pump.action },
    { Actuator: "Solenoid SOIL",  Status: act.soil.action },
    { Actuator: "Solenoid HIDRO", Status: act.hidro.action },
    { Actuator: "Pump Nutrisi",   Status: nutrisi.action }
  ]);
}

// ======================
// PUBLISH RELAY (TIDAK DIUBAH)
// ======================
async function publishRelay(channel, {
  pump, relay, topic, decision, lastState
}) {
  const { action, reason } = decision;
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
// EVALUATION ENGINE (PATCH SAJA)
// ======================
async function evaluate(channel) {
  updateState();

  const act = decideActuators();
  const nutrisi = decidePumpNutrisi();

  const pumpRes = await publishRelay(channel, {
    pump: "hidroponic",
    relay: "pump-hidroponic",
    topic: "control.relay.hidroponic",
    decision: act.pump,
    lastState: lastPumpHidroState
  });
  if (pumpRes) lastPumpHidroState = pumpRes;

  const soilSolRes = await publishRelay(channel, {
    pump: "soil",
    relay: "selenoid-soil",
    topic: "control.relay.soil",
    decision: act.soil,
    lastState: lastSolenoidSoilState
  });
  if (soilSolRes) lastSolenoidSoilState = soilSolRes;

  const hidroSolRes = await publishRelay(channel, {
    pump: "hidroponic",
    relay: "selenoid-hidroponic",
    topic: "control.relay.hidroponic",
    decision: act.hidro,
    lastState: lastSolenoidHidroState
  });
  if (hidroSolRes) lastSolenoidHidroState = hidroSolRes;

  const nutrisiRes = await publishRelay(channel, {
    pump: "nutrisi",
    relay: "pump-nutrisi",
    topic: "control.relay.nutrisi",
    decision: nutrisi,
    lastState: lastPumpNutrisiState
  });
  if (nutrisiRes) lastPumpNutrisiState = nutrisiRes;

  renderDashboard(act, nutrisi);
}

// ======================
// CONSUMERS (TIDAK DIUBAH)
// ======================
async function startConsumers() {
  await connectRabbit();
  const channel = getChannel();

  channel.consume(process.env.SOIL_QUEUE, async msg => {
    const d = JSON.parse(msg.content.toString());
    soilStates.set(d.ip, Number(d.kelembaban_tanah));
    await evaluate(channel);
    channel.ack(msg);
  });

  channel.consume(process.env.ULTRASONIC_QUEUE, async msg => {
    globalState.distance_cm = JSON.parse(msg.content.toString()).distance_cm;
    await evaluate(channel);
    channel.ack(msg);
  });

  channel.consume(process.env.TDS_QUEUE, async msg => {
    globalState.tds_ppm = JSON.parse(msg.content.toString()).tds_ppm;
    await evaluate(channel);
    channel.ack(msg);
  });

  channel.consume(process.env.APPS_CONTROL_QUEUE, async msg => {
    const d = JSON.parse(msg.content.toString());
    manualSanyoActive = d.pump === 1 && d.status === "ON";
    currentState = manualSanyoActive ? STATES.MANUAL_SANYO : STATES.IDLE;
    await evaluate(channel);
    channel.ack(msg);
  });

  console.log("✅ FSM HYDROPONIC CONTROLLER ACTIVE");
}

module.exports = { startConsumers };
