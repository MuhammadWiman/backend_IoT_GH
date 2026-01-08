require("dotenv").config();
const express = require("express");
const cors = require("cors");

const connectDB = require("./database/db");

// ⬇️ controller/consumer
const { startConsumers } = require("./services/controller");

const app = express();
connectDB();

app.use(cors());
app.use(express.json());


const PORT = process.env.PORT || 3000;

app.listen(PORT, async () => {
  console.log(` Server running on port ${PORT}`);

  // 🔥 start IoT background worker
  await startConsumers();
});
