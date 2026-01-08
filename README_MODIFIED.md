
# Pemro-3 Modified: Sensor Microservice Integration

Project dimodifikasi untuk menambahkan:
- RabbitMQ consumer (collector) yang membaca pesan JSON dan menyimpan ke MongoDB
- Scheduled flush setiap 10 menit
- Real-time dashboard via Socket.IO (public/index.html)
- Pesan format: { nama, npm, tgl_lahir, timestamp }

## Arsitektur (Mermaid)
```mermaid
flowchart LR
  A[Sensor / Simulator] -->|send JSON to AMQP| B(RabbitMQ)
  B --> C[Collector Service (AMQP Consumer)]
  C --> D[In-memory Buffer]
  C -->|every 10 min| E[MongoDB (sensor_records)]
  C -->|emit| F[Frontend (Socket.IO Dashboard)]
```
