const mongoose = require("mongoose");

const PumpLogSchema = new mongoose.Schema({
  action: {
    type: String,
    enum: ["ON", "OFF"],
    required: true
  },
  source: {
    type: String,
    default: "controller"
  },
  reason: {
    type: String,
    default: "-"
  },
  timestamp: {
    type: Date,
    default: Date.now
  }
});

module.exports = mongoose.model("PumpLog", PumpLogSchema);
