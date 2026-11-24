# EventPulse — Full-stack starter

A runnable local EventPulse implementation with:
- React dashboard
- Node.js/Express producer API
- Google Cloud Pub/Sub (local emulator)
- Google Cloud Firestore (local emulator)
- Consumer with schema validation, atomic idempotency, bounded retries and failure records
- Structured JSON logs (Cloud Run automatically exports container stdout/stderr to Cloud Logging when deployed there)

## Requirements
- Docker Desktop with Docker Compose
- Node.js 20+ only if you want to run frontend/backend outside Docker

## Run locally
From this folder:

```bash
docker compose up --build
```

Open:
- Frontend: http://localhost:5173
- Backend health: http://localhost:8081/health
- API events: http://localhost:8081/api/events

The first startup can take a minute while the Google Cloud emulators initialize. Backend startup retries emulator setup.

## Try it
1. Open the dashboard.
2. Click **Send sample event**.
3. Watch the event status and student activity update.
4. Click **Replay last event** to test duplicate detection.
5. Click **Send malformed event** to test schema rejection.
6. Click **Simulate temporary failure** to test bounded retries.
7. Refresh the dashboard; Firestore data remains available while the emulator container volume exists.

## What is real and what is emulated?
| Component | Local mode |
|---|---|
| React frontend | Real local application |
| Express producer/consumer | Real local application |
| Pub/Sub | Google Pub/Sub emulator |
| Firestore | Firestore emulator |
| Cloud Logging | Structured stdout JSON locally; Cloud Logging receives container logs on Cloud Run |
| Cloud Run | Not used locally; deploy the backend container to Cloud Run for a real deployment |
| Eventarc | Not enabled in this local stack; configure and verify separately in Google Cloud if required |

Do not describe local emulator tests as proof of a live Cloud Run/Eventarc deployment. In your report, identify each service as native cloud, emulator, or simulated.

## Environment and architecture
- `POST /api/events` is the producer endpoint.
- Producer publishes JSON to Pub/Sub topic `eventpulse-events`.
- Subscriber receives the message and validates its schema.
- Consumer uses a Firestore transaction to atomically check `processed_events/{event_id}`, update `student_activities/{student_id}_{activity_id}`, and record the processed event.
- Duplicate event IDs are acknowledged without repeating database side effects.
- Invalid messages are recorded in `failed_events` and acknowledged (not retried forever).
- Temporary failures retry up to 3 attempts with short exponential backoff. Exhausted events are written to `failed_events`.

## Useful API routes
- `GET /health`
- `POST /api/events`
- `POST /api/test/malformed`
- `POST /api/test/temporary-failure`
- `GET /api/activities`
- `GET /api/events`
- `GET /api/stats`

## Sample event contract
```json
{
  "event_id": "EVT-unique-id",
  "event_type": "assignment_submitted",
  "schema_version": "1.0",
  "timestamp": "2026-11-24T10:30:00.000Z",
  "student_id": "STU101",
  "activity_id": "ASSIGN101",
  "data": { "status": "submitted" }
}
```

## Deploying to Google Cloud
This repository is a local-first starter, not a one-click production deployment. For a real deployment:
1. Create a Google Cloud project and enable Cloud Run, Pub/Sub, Firestore and Cloud Logging APIs.
2. Create a Firestore database.
3. Create a Pub/Sub topic and subscription, or allow the application service account to create them.
4. Build and deploy the backend container to Cloud Run.
5. Set `GOOGLE_CLOUD_PROJECT` and `PUBSUB_TOPIC` in Cloud Run; remove emulator environment variables.
6. Grant the Cloud Run service account only the required Pub/Sub and Firestore IAM permissions.
7. Configure a Pub/Sub push subscription targeting the Cloud Run consumer endpoint, or use a separately deployed pull consumer. Do not expose an unauthenticated consumer endpoint.
8. If your assignment requires Eventarc, configure an Eventarc trigger supported by the chosen event source and destination, then capture evidence proving the route. This local stack does not claim Eventarc integration.
9. Verify Cloud Logging entries and capture screenshots of real cloud resources.

For production, use Secret Manager for secrets, authenticated service-to-service calls, least-privilege IAM, and appropriate retry/dead-letter policies.
