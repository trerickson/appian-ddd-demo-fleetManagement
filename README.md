# appian-ddd-demo-fleetManagement

Fleet Management Sync Service
This service acts as the middleware bridge between the Fleet Fabric database and the Appian Sandbox. It ensures that when data is modified via the Python API, Appian’s Record Types are notified and synchronized in real-time.

🚀 Purpose
In a service-backed or externally synced Record architecture, Appian isn't always aware of changes made directly to the source database. This service solves that by:

Processing incoming vehicle and maintenance data via FastAPI.

Persisting changes to the PostgreSQL database.

Triggering a Webhook Sync in Appian to ensure the UI stays updated without requiring a manual full-sync.

🛠 Tech Stack
Framework: FastAPI (Python)

ORM: SQLAlchemy (PostgreSQL)

Infrastructure: Hosted on Railway

Appian Integration: Web API + Process Model Webhook