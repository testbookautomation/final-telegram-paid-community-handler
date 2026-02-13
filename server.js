"use strict";

const express = require("express");
const crypto = require("crypto");
const { Firestore, FieldValue } = require("@google-cloud/firestore");
const { CloudTasksClient } = require("@google-cloud/tasks");

const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

const app = express();
app.use(express.json({ limit: "2mb" }));

/* =========================
   ENV
   ========================= */

const {
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHANNEL_ID,
  STORE_API_KEY,
  WEBENGAGE_LICENSE_CODE,
  WEBENGAGE_API_KEY,
  FIRE_JOIN_EVENT,
  GCP_PROJECT_ID,
  TASKS_LOCATION,
  TASKS_QUEUE,
  WORKER_URL,
  PORT = 8080,
} = process.env;

/* =========================
   FIRESTORE (SEPARATE DB)
   ========================= */

const db = new Firestore({
  projectId: GCP_PROJECT_ID,
  databaseId: "larger_requests_db",
});

const COL_TXN = "large_txn_invites";
const COL_INV = "large_invite_lookup";

/* =========================
   CLOUD TASKS
   ========================= */

const tasksClient = new CloudTasksClient();

/* =========================
   UTILS
   ========================= */

const trace = (tag, msg, data = null) => {
  console.log(
    `[${tag}] ${msg}${data ? " | DATA: " + JSON.stringify(data) : ""}`
  );
};

function generateTxn() {
  return `txn_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
}

/* =========================
   PUSH CLOUD TASK
   ========================= */

async function pushTask(transactionId) {
  const parent = tasksClient.queuePath(
    GCP_PROJECT_ID,
    TASKS_LOCATION,
    TASKS_QUEUE
  );

  const task = {
    httpRequest: {
      httpMethod: "POST",
      url: `${WORKER_URL}/process-invite`,
      headers: {
        "Content-Type": "application/json",
      },
      body: Buffer.from(
        JSON.stringify({ transactionId })
      ).toString("base64"),
    },
  };

  await tasksClient.createTask({ parent, task });

  trace("TASK", "Task pushed", { transactionId });
}

/* =========================
   TELEGRAM LINK CREATION
   ========================= */

async function createTelegramLink(transactionId) {
  const expireDate =
    Math.floor(Date.now() / 1000) + 60 * 60 * 48;

  const res = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/createChatInviteLink`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHANNEL_ID,
        member_limit: 1,
        expire_date: expireDate,
        name: `TXN:${transactionId}`.slice(0, 255),
      }),
    }
  );

  const data = await res.json();

  if (!data.ok) {
    trace("TELEGRAM_ERROR", data);
    throw new Error(data.description);
  }

  return data.result.invite_link;
}

/* =========================
   WEBENGAGE
   ========================= */

async function fireWebEngage(userId, eventName, eventData) {
  const url = `https://api.webengage.com/v1/accounts/${WEBENGAGE_LICENSE_CODE}/events`;

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${WEBENGAGE_API_KEY}`,
      },
      body: JSON.stringify({
        userId: String(userId),
        eventName,
        eventData,
      }),
    });

    trace("WEBENGAGE", `Status ${res.status}`);
  } catch (err) {
    trace("WEBENGAGE_ERROR", err.message);
  }
}

/* ====================================================
   API: CREATE INVITE (FAST)
   ==================================================== */

app.post("/create-invite", async (req, res) => {
  const apiKey = req.header("x-api-key");
  if (apiKey !== STORE_API_KEY) return res.sendStatus(401);

  const { userId, telegramUserId } = req.body;
  if (!userId) return res.sendStatus(400);

  const transactionId =
    req.body.transactionId?.trim() !== ""
      ? req.body.transactionId
      : generateTxn();

  try {
    const docRef = db.collection(COL_TXN).doc(transactionId);
    const snap = await docRef.get();

    if (!snap.exists) {
      await docRef.set({
        userId,
        telegramUserId: telegramUserId || null,
        status: "PENDING",
        createdAt: FieldValue.serverTimestamp(),
      });

      await pushTask(transactionId);
    }

    return res.json({ ok: true, transactionId });
  } catch (err) {
    trace("CREATE_ERROR", err.message);
    return res.status(500).json({ ok: false });
  }
});

/* ====================================================
   WORKER: PROCESS INVITE (QUEUE CONTROLLED)
   ==================================================== */

app.post("/process-invite", async (req, res) => {
  const { transactionId } = req.body;

  if (!transactionId) return res.sendStatus(400);

  const txnRef = db.collection(COL_TXN).doc(transactionId);

  try {
    const snap = await txnRef.get();
    if (!snap.exists) return res.send("not_found");

    const data = snap.data();

    if (data.status === "SUCCESS") {
      return res.send("already_done");
    }

    const inviteLink = await createTelegramLink(transactionId);

    const inviteHash = crypto
      .createHash("sha256")
      .update(inviteLink)
      .digest("hex");

    const batch = db.batch();

    batch.update(txnRef, {
      status: "SUCCESS",
      inviteLink,
      inviteHash,
      updatedAt: FieldValue.serverTimestamp(),
    });

    batch.set(db.collection(COL_INV).doc(inviteHash), {
      userId: data.userId,
      transactionId,
      inviteLink,
      createdAt: FieldValue.serverTimestamp(),
    });

    await batch.commit();

    await fireWebEngage(
      data.userId,
      "pass_paid_community_telegram_link_created",
      { transactionId }
    );

    res.send("ok");
  } catch (err) {
    trace("PROCESS_ERROR", err.message);
    res.status(500).send("error");
  }
});

/* ====================================================
   TELEGRAM WEBHOOK (JOIN TRACK)
   ==================================================== */

app.post("/telegram-webhook", async (req, res) => {
  if (FIRE_JOIN_EVENT !== "true") return res.send("ignored");

  const cm = req.body.chat_member || req.body.my_chat_member;
  if (!cm) return res.send("ignored");

  const inviteLink = cm?.invite_link?.invite_link;
  const status = cm?.new_chat_member?.status;

  if (!inviteLink || !["member", "administrator", "creator"].includes(status))
    return res.send("ignored");

  const inviteHash = crypto
    .createHash("sha256")
    .update(inviteLink)
    .digest("hex");

  const inviteSnap = await db
    .collection(COL_INV)
    .doc(inviteHash)
    .get();

  if (!inviteSnap.exists) return res.send("not_found");

  const { transactionId, userId } = inviteSnap.data();

  await fireWebEngage(
    userId,
    "pass_paid_community_telegram_joined",
    { transactionId }
  );

  res.send("ok");
});

/* =========================
   HEALTH
   ========================= */

app.get("/healthz", (_, res) => res.send("ok"));

/* =========================
   START
   ========================= */

app.listen(PORT, "0.0.0.0", () =>
  trace("SYSTEM", `Listening on ${PORT}`)
);
