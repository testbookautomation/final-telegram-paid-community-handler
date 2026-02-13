"use strict";

const express = require("express");
const crypto = require("crypto");
const { Firestore, FieldValue } = require("@google-cloud/firestore");
const { CloudTasksClient } = require("@google-cloud/tasks");

const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

/* =========================
   ENV CONFIG
   ========================= */

const {
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHANNEL_ID,
  STORE_API_KEY,
  WEBENGAGE_LICENSE_CODE,
  WEBENGAGE_API_KEY,
  FIRE_JOIN_EVENT,
  GCP_PROJECT,
  GCP_LOCATION = "asia-south1",
  TASKS_QUEUE = "telegram-invite-queue",
  BASE_URL,
  PORT = 8080,
} = process.env;

/* =========================
   INIT
   ========================= */

const app = express();
app.use(express.json({ limit: "1mb" }));

const db = new Firestore();
const tasksClient = new CloudTasksClient();

const COL_TXN = "txn_invites";
const COL_INV = "invite_lookup";

const SAFE_RPS_SLEEP_MS = 220;
const MAX_RETRY = 10;

/* =========================
   LOGGING
   ========================= */

const trace = (tag, msg, data = null) => {
  console.log(
    `[${tag}] ${msg}${data ? " | DATA: " + JSON.stringify(data) : ""}`
  );
};

/* =========================
   HELPERS
   ========================= */

async function enqueueInviteTask(transactionId, delaySeconds = 0) {
  if (!GCP_PROJECT || !BASE_URL) {
    throw new Error("Missing GCP_PROJECT or BASE_URL");
  }

  const parent = tasksClient.queuePath(
    GCP_PROJECT,
    GCP_LOCATION,
    TASKS_QUEUE
  );

  const task = {
    httpRequest: {
      httpMethod: "POST",
      url: `${BASE_URL}/create-invite-worker`,
      headers: { "Content-Type": "application/json" },
      body: Buffer.from(
        JSON.stringify({ transactionId })
      ).toString("base64"),
    },
  };

  if (delaySeconds > 0) {
    task.scheduleTime = {
      seconds: Math.floor(Date.now() / 1000) + delaySeconds,
    };
  }

  await tasksClient.createTask({ parent, task });
}

async function fireWebEngage(userId, eventName, eventData) {
  trace("WEBENGAGE", "Firing event", { userId, eventName });

  try {
    const res = await fetch(
      `https://api.webengage.com/v1/accounts/${WEBENGAGE_LICENSE_CODE}/events`,
      {
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
      }
    );

    const body = await res.text();
    trace("WEBENGAGE", `Status ${res.status}`, body);
    return res.ok;
  } catch (err) {
    trace("WEBENGAGE", "ERROR", err.message);
    return false;
  }
}

async function createTelegramLink(transactionId) {
  trace("TELEGRAM", "Creating invite link", { transactionId });

  const res = await fetch(
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/createChatInviteLink`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHANNEL_ID,
        member_limit: 1,
        expire_date: Math.floor(Date.now() / 1000) + 172800,
        name: `TXN:${transactionId}`.slice(0, 255),
      }),
    }
  );

  const data = await res.json();

  if (!res.ok || !data.ok) {
    throw new Error(
      data?.description ||
        `Telegram error status ${res.status}`
    );
  }

  trace("TELEGRAM", "Invite created");
  return data.result.invite_link;
}

/* =========================
   HEALTH CHECK
   ========================= */

app.get("/healthz", (_, res) => res.send("ok"));

/* =========================
   CREATE INVITE (Queued)
   ========================= */

app.post("/create-invite", async (req, res) => {
  trace("API", "create-invite called", req.body);

  const apiKey = req.header("x-api-key");
  if (apiKey !== STORE_API_KEY) {
    trace("AUTH", "Invalid API key");
    return res.sendStatus(401);
  }

  const { userId, telegramUserId } = req.body;

  const transactionId =
    req.body.transactionId && req.body.transactionId.trim() !== ""
      ? req.body.transactionId
      : `txn_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;

  if (!userId) return res.sendStatus(400);

  try {
    await db.collection(COL_TXN).doc(transactionId).set({
      userId,
      telegramUserId: telegramUserId || null,
      transactionId,
      joined: false,
      status: "QUEUED",
      attempts: 0,
      createdAt: FieldValue.serverTimestamp(),
    });

    await enqueueInviteTask(transactionId);

    res.json({ ok: true, status: "queued" });
  } catch (err) {
    trace("ERROR", "Queue failed", err.message);
    res.status(500).json({ ok: false });
  }
});

/* =========================
   WORKER
   ========================= */

app.post("/create-invite-worker", async (req, res) => {
  try {
    const { transactionId } = req.body;
    if (!transactionId) return res.send("ok");

    const txnRef = db.collection(COL_TXN).doc(transactionId);
    const txnSnap = await txnRef.get();
    if (!txnSnap.exists) return res.send("ok");

    const txnData = txnSnap.data();
    if (txnData.status === "DONE") return res.send("ok");

    const attempts = (txnData.attempts || 0) + 1;

    if (attempts > MAX_RETRY) {
      await txnRef.update({
        status: "FAILED",
        lastError: "Max retries exceeded",
      });
      return res.send("failed");
    }

    await txnRef.update({
      status: "PROCESSING",
      attempts,
    });

    await new Promise((r) => setTimeout(r, SAFE_RPS_SLEEP_MS));

    let inviteLink;

    try {
      inviteLink = await createTelegramLink(transactionId);
    } catch (err) {
      trace("WORKER", "Telegram error", err.message);

      const delay = Math.min(3600, 5 * Math.pow(2, attempts));

      await txnRef.update({
        status: "QUEUED",
        lastError: err.message,
      });

      await enqueueInviteTask(transactionId, delay);
      return res.send("retry_scheduled");
    }

    const inviteHash = crypto
      .createHash("sha256")
      .update(inviteLink)
      .digest("hex");

    await db.collection(COL_INV).doc(inviteHash).set({
      userId: txnData.userId,
      telegramUserId: txnData.telegramUserId || null,
      transactionId,
      inviteLink,
      createdAt: FieldValue.serverTimestamp(),
    });

    await txnRef.update({
      inviteHash,
      inviteLink,
      status: "DONE",
      updatedAt: FieldValue.serverTimestamp(),
    });

    await fireWebEngage(
      txnData.userId,
      "pass_paid_community_telegram_link_created",
      { transactionId, inviteLink }
    );

    res.send("ok");
  } catch (err) {
    trace("WORKER", "Fatal", err.message);
    res.send("ok");
  }
});

/* =========================
   TELEGRAM WEBHOOK
   ========================= */

app.post("/telegram-webhook", async (req, res) => {
  trace("WEBHOOK", "Received Telegram webhook");

  if (FIRE_JOIN_EVENT !== "true") return res.send("ignored");

  const cm = req.body.chat_member || req.body.my_chat_member;
  if (!cm) return res.send("ignored");

  const inviteLink = cm?.invite_link?.invite_link;
  const status = cm?.new_chat_member?.status;
  const joinedTelegramUserId = cm?.new_chat_member?.user?.id;

  if (
    !inviteLink ||
    !joinedTelegramUserId ||
    !["member", "administrator", "creator"].includes(status)
  ) {
    return res.send("ignored");
  }

  const inviteHash = crypto
    .createHash("sha256")
    .update(inviteLink)
    .digest("hex");

  const inviteRef = db.collection(COL_INV).doc(inviteHash);
  const inviteSnap = await inviteRef.get();
  if (!inviteSnap.exists) return res.send("not_found");

  const { transactionId, userId } = inviteSnap.data();
  const txnRef = db.collection(COL_TXN).doc(transactionId);

  let finalTelegramUserId = null;
  let fire = false;

  await db.runTransaction(async (t) => {
    const txnSnap = await t.get(txnRef);
    if (!txnSnap.exists) return;

    const txnData = txnSnap.data();

    if (!txnData.joined) {
      finalTelegramUserId =
        txnData.telegramUserId || joinedTelegramUserId;

      t.update(txnRef, {
        joined: true,
        joinedAt: FieldValue.serverTimestamp(),
        telegramUserId: finalTelegramUserId,
      });

      t.update(inviteRef, {
        telegramUserId: finalTelegramUserId,
      });

      fire = true;
    }
  });

  if (fire) {
    await fireWebEngage(
      userId,
      "pass_paid_community_telegram_joined",
      {
        transactionId,
        inviteLink,
        joined: true,
        telegramUserId: String(finalTelegramUserId),
      }
    );
  }

  res.send("ok");
});

/* ========================= */

app.listen(PORT, "0.0.0.0", () => {
  trace("SYSTEM", `Listening on ${PORT}`);
});
