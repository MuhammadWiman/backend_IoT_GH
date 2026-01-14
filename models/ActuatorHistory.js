const mongoose = require("mongoose");

const ActuatorHistorySchema = new mongoose.Schema({
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
    required: true
  },
  timestamp: {
    type: Date,
    default: Date.now
  }
});

module.exports = mongoose.model("ActuatorHistory", ActuatorHistorySchema);
