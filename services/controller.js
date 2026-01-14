require("dotenv").config();
const readline = require("readline");
const { connectRabbit, getChannel } = require("./rabbit");
const ActuatorHistory = require("../models/actuatorHistory");

// ======================
// CONFIG LEVEL AIR
// ======================
const TANK_FULL_MAX = 15;

// ======================
// CONFIG SOIL
// ======================
const SOIL_DRY = 40;
const SOIL_WET = 50;

// ======================
// PUMP SAFETY CONFIG
// ======================
const PUMP_ON_MS       = 30 * 1000;
const PUMP_COOLDOWN_MS = 30 * 1000;

// ======================
// LOG INTERVAL (6 JAM)
// ======================
const LOG_INTERVAL_MS =  1000;

// ======================
// GLOBAL STATES
// ======================
const globalState = {
  distance_cm: null,
  tds_ppm: null
};

const soilStates = new Map();

// ======================
// LAST LOG TIME (PER RELAY)
// ======================
const lastLogTime = new Map();

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
let manualSanyoActive = false;

// ======================
// LAST RELAY STATES
// ======================
let lastPumpHidroState     = null;
let lastSolenoidSoilState  = null;
let lastSolenoidHidroState = null;
let lastPumpNutrisiState   = null;

// ======================
// PUMP RUNTIME STATE
// ======================
const pumpRuntime = {
  isOn: false,
  onSince: 0,
  cooldownUntil: 0
};

// ======================
// SENSOR HELPERS
// ======================
const hasTankData = () => globalState.distance_cm !== null;

const isSoilDry = () =>
  soilStates.size > 0 &&
  [...soilStates.values()].some(v => v < SOIL_DRY);

const isSoilWet = () =>
  soilStates.size > 0 &&
  [...soilStates.values()].every(v => v >= SOIL_WET);

const isTankFull = () =>
  hasTankData() && globalState.distance_cm <= TANK_FULL_MAX;

// ======================
// FSM TRANSITION
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
// FSM ACTUATOR DECISION
// ======================
function decideActuators() {
  switch (currentState) {
    case STATES.FILL_TANK:
      return {
        pump:  { action: "ON",  reason: "fsm_fill_tank" },
        soil:  { action: "OFF", reason: "fsm_fill_tank" },
        hidro: { action: "ON",  reason: "fsm_fill_tank" }
      };

    case STATES.IRRIGATE_SOIL:
      return {
        pump:  { action: "ON",  reason: "fsm_irrigate_soil" },
        soil:  { action: "ON",  reason: "fsm_irrigate_soil" },
        hidro: { action: "OFF", reason: "fsm_irrigate_soil" }
      };

    case STATES.MANUAL_SANYO:
      return {
        pump:  { action: "ON",  reason: "manual_sanyo" },
        soil:  { action: "OFF", reason: "manual_sanyo" },
        hidro: { action: "OFF", reason: "manual_sanyo" }
      };

    default:
      return {
        pump:  { action: "OFF", reason: "fsm_idle" },
        soil:  { action: "OFF", reason: "fsm_idle" },
        hidro: { action: "OFF", reason: "fsm_idle" }
      };
  }
}

// ======================
// PUMP CYCLE CONTROLLER
// ======================
function enforcePumpCycle(requestedAction) {
  const now = Date.now();

  if (now < pumpRuntime.cooldownUntil) return "OFF";

  if (requestedAction === "ON" && !pumpRuntime.isOn) {
    pumpRuntime.isOn = true;
    pumpRuntime.onSince = now;
    return "ON";
  }

  if (pumpRuntime.isOn) {
    const runtime = now - pumpRuntime.onSince;
    if (runtime < PUMP_ON_MS) return "ON";

    pumpRuntime.isOn = false;
    pumpRuntime.cooldownUntil = now + PUMP_COOLDOWN_MS;
    return "OFF";
  }

  return "OFF";
}

// ======================
// NUTRISI
// ======================
function decidePumpNutrisi() {
  if (globalState.tds_ppm === null) return { action: "OFF", reason: "no_tds" };
  if (globalState.tds_ppm < 150) return { action: "ON", reason: "tds_low" };
  return { action: "OFF", reason: "tds_ok" };
}

// ======================
// LOG RATE LIMITER
// ======================
function shouldSaveLog(relay) {
  const now = Date.now();
  const last = lastLogTime.get(relay) || 0;

  if (now - last >= LOG_INTERVAL_MS) {
    lastLogTime.set(relay, now);
    return true;
  }
  return false;
}

// ======================
// DASHBOARD
// ======================
function renderDashboard(act, nutrisi) {
  readline.cursorTo(process.stdout, 0, 0);
  readline.clearScreenDown(process.stdout);

  const now = Date.now();
  const cooldownLeft =
    pumpRuntime.cooldownUntil > now
      ? Math.ceil((pumpRuntime.cooldownUntil - now) / 1000)
      : 0;

  console.log("FSM STATE:", currentState);
  console.table([
    { Actuator: "Pump Sanyo",     Status: act.pump.action },
    { Actuator: "Solenoid SOIL",  Status: act.soil.action },
    { Actuator: "Solenoid HIDRO", Status: act.hidro.action },
    { Actuator: "Pump Nutrisi",   Status: nutrisi.action }
  ]);

  console.log("PUMP CYCLE STATUS:");
  console.table([{
    isOn: pumpRuntime.isOn,
    on_seconds: pumpRuntime.isOn
      ? Math.floor((now - pumpRuntime.onSince) / 1000)
      : 0,
    cooldown_seconds: cooldownLeft
  }]);
}

// ======================
// PUBLISH RELAY (LOG 6 JAM)
// ======================
async function publishRelay(channel, {
  pump, relay, topic, decision, lastState
}) {
  if (lastState === decision.action) return lastState;

  const payload = {
    pump,
    relay,
    action: decision.action,
    reason: decision.reason,
    timestamp: new Date()
  };

  channel.publish(
    "amq.topic",
    topic,
    Buffer.from(JSON.stringify(payload))
  );

  if (shouldSaveLog(relay)) {
    try {
      await ActuatorHistory.create(payload);
    } catch (_) {}
  }

  return decision.action;
}

// ======================
// EVALUATION ENGINE
// ======================
async function evaluate(channel) {
  updateState();

  const act = decideActuators();
  act.pump.action = enforcePumpCycle(act.pump.action);

  const nutrisi = decidePumpNutrisi();

  lastPumpHidroState = await publishRelay(channel, {
    pump: "hidroponic",
    relay: "pump-hidroponic",
    topic: "control.relay.hidroponic",
    decision: act.pump,
    lastState: lastPumpHidroState
  });

  lastSolenoidSoilState = await publishRelay(channel, {
    pump: "soil",
    relay: "pump-soil",
    topic: "control.relay.soil",
    decision: act.soil,
    lastState: lastSolenoidSoilState
  });

  lastSolenoidHidroState = await publishRelay(channel, {
    pump: "hidroponic",
    relay: "selenoid-hidroponic",
    topic: "control.relay.hidroponic",
    decision: act.hidro,
    lastState: lastSolenoidHidroState
  });

  lastPumpNutrisiState = await publishRelay(channel, {
    pump: "nutrisi",
    relay: "pump-nutrisi",
    topic: "control.relay.nutrisi",
    decision: nutrisi,
    lastState: lastPumpNutrisiState
  });

  renderDashboard(act, nutrisi);
}

// ======================
// CONSUMERS
// ======================
async function startConsumers() {
  await connectRabbit();
  const channel = getChannel();

  channel.consume(process.env.SOIL_QUEUE, msg => {
    const d = JSON.parse(msg.content.toString());
    soilStates.set(d.ip, d.kelembaban_tanah);
    evaluate(channel);
    channel.ack(msg);
  });

  channel.consume(process.env.ULTRASONIC_QUEUE, msg => {
    globalState.distance_cm = JSON.parse(msg.content.toString()).distance_cm;
    evaluate(channel);
    channel.ack(msg);
  });

  channel.consume(process.env.TDS_QUEUE, msg => {
    globalState.tds_ppm = JSON.parse(msg.content.toString()).tds_ppm;
    evaluate(channel);
    channel.ack(msg);
  });

  channel.consume(process.env.APPS_CONTROL_QUEUE, msg => {
    const d = JSON.parse(msg.content.toString());
    manualSanyoActive = d.pump === 1 && d.status === "ON";
    currentState = manualSanyoActive ? STATES.MANUAL_SANYO : STATES.IDLE;
    evaluate(channel);
    channel.ack(msg);
  });

  console.log("✅ FSM HYDROPONIC CONTROLLER ACTIVE");
}

module.exports = { startConsumers };
