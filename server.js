"use strict";

const express = require("express");
const crypto = require("crypto");
const { Firestore, FieldValue } = require("@google-cloud/firestore");

const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

const app = express();
app.use(express.json({ limit: "1mb" }));

const {
  TELEGRAM_BOT_TOKEN,
  TELEGRAM_CHANNEL_ID,
  STORE_API_KEY,
  WEBENGAGE_LICENSE_CODE,
  WEBENGAGE_API_KEY,
  FIRE_JOIN_EVENT,
  PORT = 8080,
} = process.env;

const db = new Firestore();
const COL_TXN = "txn_invites";
const COL_INV = "invite_lookup";

const trace = (tag, msg, data = null) => {
  console.log(
    `[${tag}] ${msg}${data ? " | DATA: " + JSON.stringify(data) : ""}`
  );
};

async function fireWebEngage(userId, eventName, eventData) {
  trace("WEBENGAGE", "Firing event", { userId, eventName });

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
  if (!data.ok) {
    trace("TELEGRAM", "Invite creation failed", data);
    throw new Error(data.description);
  }

  trace("TELEGRAM", "Invite created");
  return data.result.invite_link;
}

app.get("/healthz", (_, res) => res.send("ok"));

/* =========================
   CREATE INVITE
   ========================= */
app.post("/create-invite", async (req, res) => {
  trace("API", "create-invite called", req.body);

  const apiKey = req.header("x-api-key");

  if (apiKey !== STORE_API_KEY) {
    trace("AUTH", "Invalid API key");
    return res.sendStatus(401);
  }

  const { userId, telegramUserId } = req.body;

  // AUTO-GENERATE transactionId if missing or empty
  const transactionId =
    req.body.transactionId && req.body.transactionId.trim() !== ""
      ? req.body.transactionId
      : `txn_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;

  if (!userId) {
    trace("API", "Missing userId");
    return res.sendStatus(400);
  }

  try {
    const inviteLink = await createTelegramLink(transactionId);

    const inviteHash = crypto
      .createHash("sha256")
      .update(inviteLink)
      .digest("hex");

    const batch = db.batch();

    batch.set(db.collection(COL_TXN).doc(transactionId), {
      userId,
      telegramUserId: telegramUserId || null,
      transactionId,
      inviteHash,
      inviteLink,
      joined: false,
      createdAt: FieldValue.serverTimestamp(),
    });

    batch.set(db.collection(COL_INV).doc(inviteHash), {
      userId,
      telegramUserId: telegramUserId || null,
      transactionId,
      inviteLink,
      createdAt: FieldValue.serverTimestamp(),
    });

    await batch.commit();
    trace("DB", "Invite stored", { transactionId });

    await fireWebEngage(
      userId,
      "pass_paid_community_telegram_link_created",
      { transactionId, inviteLink }
    );

    res.json({ ok: true, inviteLink });
  } catch (err) {
    trace("ERROR", "create-invite failed", err.message);
    res.status(500).json({ ok: false });
  }
});

/* =========================
   TELEGRAM WEBHOOK
   ========================= */
app.post("/telegram-webhook", async (req, res) => {
  trace("WEBHOOK", "Received Telegram webhook");

  if (FIRE_JOIN_EVENT !== "true") {
    return res.send("ignored");
  }

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

  const inviteHash = crypto.createHash("sha256").update(inviteLink).digest("hex");
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

app.listen(PORT, "0.0.0.0", () =>
  trace("SYSTEM", `Listening on ${PORT}`)
);
