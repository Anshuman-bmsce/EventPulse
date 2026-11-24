const express = require("express");
const cors = require("cors");
const { Firestore, FieldValue } = require("@google-cloud/firestore");
const { PubSub } = require("@google-cloud/pubsub");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || "eventpulse-demo";
const TOPIC_NAME = process.env.PUBSUB_TOPIC || "eventpulse-events";
const SUB_NAME = process.env.PUBSUB_SUBSCRIPTION || "eventpulse-consumer";
const PORT = Number(process.env.PORT || 8081);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);

const firestore = new Firestore({ projectId: PROJECT_ID });
const pubsub = new PubSub({ projectId: PROJECT_ID });

let topic;
let subscription;
let subscriberStarted = false;
const temporaryFailures = new Map();

function log(severity, message, fields = {}) {
  // Cloud Run captures structured JSON written to stdout/stderr in Cloud Logging.
  console.log(JSON.stringify({
    severity, message, service: "eventpulse", timestamp: new Date().toISOString(), ...fields
  }));
}

function validateEvent(e) {
  const required = ["event_id", "event_type", "schema_version", "timestamp", "student_id", "activity_id"];
  const missing = required.filter(k => !e || typeof e[k] !== "string" || !e[k].trim());
  if (missing.length) return `Missing or invalid fields: ${missing.join(", ")}`;
  if (e.schema_version !== "1.0") return "Unsupported schema_version";
  if (Number.isNaN(Date.parse(e.timestamp))) return "timestamp must be a valid ISO date";
  if (!["assignment_submitted", "attendance_recorded", "quiz_completed"].includes(e.event_type))
    return "Unsupported event_type";
  return null;
}

async function ensureMessaging() {
  for (let attempt = 1; attempt <= 30; attempt++) {
    try {
      const [exists] = await pubsub.topic(TOPIC_NAME).exists();
      topic = pubsub.topic(TOPIC_NAME);
      if (!exists) await topic.create();
      const sub = pubsub.subscription(SUB_NAME);
      const [subExists] = await sub.exists();
      if (!subExists) await topic.createSubscription(SUB_NAME);
      subscription = pubsub.subscription(SUB_NAME);
      log("INFO", "Messaging resources ready", { topic: TOPIC_NAME, subscription: SUB_NAME });
      return;
    } catch (err) {
      if (attempt === 30) throw err;
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

async function processEvent(event) {
  const validationError = validateEvent(event);
  if (validationError) {
    await firestore.collection("failed_events").doc(event?.event_id || uuidv4()).set({
      event_id: event?.event_id || null,
      status: "rejected",
      reason: validationError,
      received_at: FieldValue.serverTimestamp()
    });
    log("WARNING", "Malformed event rejected", { event_id: event?.event_id || null, reason: validationError });
    return { status: "rejected", reason: validationError };
  }

  // Demonstration hook used by the dashboard to exercise retry behaviour.
  if (event.data?.simulate_temporary_failure === true) {
    const key = event.event_id;
    const count = (temporaryFailures.get(key) || 0) + 1;
    temporaryFailures.set(key, count);
    if (count <= 2) throw new Error(`Simulated temporary failure ${count}`);
  }

  const processedRef = firestore.collection("processed_events").doc(event.event_id);
  const activityRef = firestore.collection("student_activities")
    .doc(`${event.student_id}_${event.activity_id}`);

  return firestore.runTransaction(async tx => {
    const existing = await tx.get(processedRef);
    if (existing.exists) {
      log("INFO", "Duplicate event ignored", { event_id: event.event_id });
      return { status: "duplicate", event_id: event.event_id };
    }

    tx.set(activityRef, {
      student_id: event.student_id,
      activity_id: event.activity_id,
      status: event.data?.status || "completed",
      last_event_id: event.event_id,
      event_type: event.event_type,
      updated_at: FieldValue.serverTimestamp()
    }, { merge: true });

    tx.create(processedRef, {
      event_id: event.event_id,
      event_type: event.event_type,
      student_id: event.student_id,
      activity_id: event.activity_id,
      status: "processed",
      processed_at: FieldValue.serverTimestamp()
    });
    log("INFO", "Event processed", { event_id: event.event_id, student_id: event.student_id });
    return { status: "processed", event_id: event.event_id };
  });
}

async function consumeMessage(message) {
  let event;
  try {
    event = JSON.parse(message.data.toString());
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await processEvent(event);
        message.ack();
        return;
      } catch (err) {
        log("ERROR", "Event processing attempt failed", {
          event_id: event?.event_id || null, attempt, error: err.message
        });
        if (attempt === MAX_RETRIES) {
          await firestore.collection("failed_events").doc(event?.event_id || uuidv4()).set({
            event_id: event?.event_id || null,
            status: "failed",
            retry_count: attempt,
            reason: err.message,
            failed_at: FieldValue.serverTimestamp()
          });
          log("ERROR", "Event reached terminal failure", { event_id: event?.event_id || null, retry_count: attempt });
          message.ack();
          return;
        }
        await new Promise(r => setTimeout(r, 500 * (2 ** (attempt - 1))));
      }
    }
  } catch (err) {
    log("ERROR", "Message could not be decoded or recorded", { error: err.message });
    // A poison message is acknowledged to avoid an infinite redelivery loop.
    message.ack();
  }
}

async function startSubscriber() {
  if (subscriberStarted) return;
  await ensureMessaging();
  subscription.on("message", consumeMessage);
  subscription.on("error", err => log("ERROR", "Pub/Sub subscriber error", { error: err.message }));
  subscriberStarted = true;
}

app.get("/health", (_req, res) => res.json({ ok: true, service: "eventpulse-backend" }));

app.post("/api/events", async (req, res) => {
  try {
    const event = {
      event_id: req.body.event_id || `EVT-${uuidv4()}`,
      event_type: req.body.event_type || "assignment_submitted",
      schema_version: req.body.schema_version || "1.0",
      timestamp: req.body.timestamp || new Date().toISOString(),
      student_id: req.body.student_id || "STU101",
      activity_id: req.body.activity_id || "ASSIGN101",
      data: req.body.data || { status: "submitted" }
    };
    const problem = validateEvent(event);
    if (problem) return res.status(400).json({ error: problem });
    await topic.publishMessage({ json: event });
    log("INFO", "Event published", { event_id: event.event_id });
    res.status(202).json({ message: "Event published", event });
  } catch (err) {
    log("ERROR", "Publish failed", { error: err.message });
    res.status(503).json({ error: "Unable to publish event", detail: err.message });
  }
});

app.post("/api/test/malformed", async (_req, res) => {
  try {
    const malformed = { event_id: `BAD-${uuidv4()}`, event_type: "assignment_submitted" };
    await topic.publishMessage({ json: malformed });
    res.status(202).json({ message: "Malformed test message published", event: malformed });
  } catch (err) { res.status(503).json({ error: err.message }); }
});

app.post("/api/test/temporary-failure", async (_req, res) => {
  try {
    const event = {
      event_id: `RETRY-${uuidv4()}`,
      event_type: "assignment_submitted",
      schema_version: "1.0",
      timestamp: new Date().toISOString(),
      student_id: "STU102",
      activity_id: "ASSIGN-RETRY",
      data: { status: "submitted", simulate_temporary_failure: true }
    };
    await topic.publishMessage({ json: event });
    res.status(202).json({ message: "Temporary failure test published; consumer will retry twice then succeed", event });
  } catch (err) { res.status(503).json({ error: err.message }); }
});

app.get("/api/activities", async (_req, res) => {
  try {
    const snap = await firestore.collection("student_activities").orderBy("updated_at", "desc").limit(100).get();
    res.json(snap.docs.map(d => ({ id: d.id, ...d.data(), updated_at: d.data().updated_at?.toDate?.() || null })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/events", async (_req, res) => {
  try {
    const [processed, failed] = await Promise.all([
      firestore.collection("processed_events").orderBy("processed_at", "desc").limit(100).get(),
      firestore.collection("failed_events").limit(100).get()
    ]);
    res.json({
      processed: processed.docs.map(d => ({ id: d.id, ...d.data() })),
      failed: failed.docs.map(d => ({ id: d.id, ...d.data() }))
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/stats", async (_req, res) => {
  try {
    const [a,p,f] = await Promise.all([
      firestore.collection("student_activities").get(),
      firestore.collection("processed_events").get(),
      firestore.collection("failed_events").get()
    ]);
    res.json({ activities: a.size, processed: p.size, failed: f.size });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

(async () => {
  try {
    await ensureMessaging();
    await startSubscriber();
    app.listen(PORT, "0.0.0.0", () => log("INFO", "EventPulse API listening", { port: PORT }));
  } catch (err) {
    log("FATAL", "Startup failed", { error: err.message });
    process.exit(1);
  }
})();
