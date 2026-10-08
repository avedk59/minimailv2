/* N2 Mesh — serverless P2P chat.
 *
 * How it works:
 *  1. Every peer in a room subscribes to a per-room topic on a public MQTT
 *     broker (WSS). The broker is used ONLY for signaling and as a message
 *     fallback — there is no chat server anywhere.
 *  2. On join, each peer announces its presence on the room topic. When two
 *     peers see each other, they exchange WebRTC offer/answer/ICE candidates
 *     through the same topic (the classic "signaling server" pattern used by
 *     PeerJS & co.), then open a WebRTC data channel directly between them.
 *  3. Once two peers are connected, chat messages travel over the WebRTC
 *     data channel — real P2P, payloads never touch any server.
 *
 * Why this design (and why WebTorrent trackers were dropped):
 *  - The public WebTorrent WebSocket trackers (tracker.webtorrent.dev,
 *    tracker.openwebtorrent.com) accept announces and see the swarm, but they
 *    no longer relay WebRTC offers between peers — verified live: peers were
 *    registered (complete=2) yet zero offers ever came back. The browser
 *    build of WebTorrent can only use WebSocket trackers (no UDP/DHT in the
 *    browser), so peers could never find each other and P2P was dead.
 *  - Signaling over the MQTT relay keeps the app fully serverless (GitHub
 *    Pages friendly), works today, and matches how real messengers do it.
 *  - Pure browser WebRTC still cannot punch through mobile carrier CGNAT,
 *    so every message is ALSO published to the room topic as a fallback.
 *    Recipients deduplicate by message id, so P2P peers get messages over
 *    the data channel while devices that cannot connect directly still
 *    exchange messages through the relay. P2P first, relay as fallback.
 *
 * Notes:
 *  - Same-browser tabs cannot WebRTC-connect to each other (browsers block
 *    loopback WebRTC), so a BroadcastChannel bridge connects local tabs.
 *  - Two peers may both see each other's presence at once. To avoid the
 *    "glare" race, the peer with the lexicographically smaller id sends the
 *    offer; the other side waits for it.
 */

'use strict';

/* Public MQTT brokers over WSS — the signaling channel + message fallback.
 * broker.hivemq.com verified working; broker.emqx.io is the backup. */
const RELAY_BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt',
];
const RELAY_TOPIC = 'n2mesh/'; // + room name

/* Presence / signaling sent to the room topic (JSON). */
const MSG_PRESENCE = 'presence';
const MSG_SIGNAL = 'signal';

/* ICE servers for WebRTC. The browser probes all of them and picks
 * whichever works; on networks where none do, the relay takes over. */
const ICE_SERVERS = [
  { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
  { urls: ['stun:global.stun.twilio.com:3478'] },
  {
    urls: [
      'turn:openrelay.metered.ca:80',
      'turn:openrelay.metered.ca:80?transport=tcp',
      'turns:openrelay.metered.ca:443?transport=tcp',
    ],
    username: 'openrelayproject',
    credential: 'openrelayproject',
  },
  {
    urls: [
      'turn:staticauth.openrelay.metered.ca:80',
      'turn:staticauth.openrelay.metered.ca:80?transport=tcp',
      'turns:staticauth.openrelay.metered.ca:443?transport=tcp',
    ],
    username: 'openrelayproject',
    credential: 'openrelayprojectsecret',
  },
];

/* BroadcastChannel name — local bridge between tabs of the same browser. */
const CHANNEL = 'n2mesh';
/* Presence re-announce interval — new peers discover you within one tick. */
const PRESENCE_MS = 5000;

const $ = (id) => document.getElementById(id);

const state = {
  /* Per-session peer id — used for glare-free offer selection. */
  pid: Math.random().toString(36).slice(2, 8) + Math.random().toString(36).slice(2, 6),
  /* pid -> { pc, dc } — active WebRTC connections. */
  pcs: new Map(),
  /* Open data channels (also counted as "peers"). */
  peers: new Set(),
  local: new Set(), // BroadcastChannel peers (same browser)
  channel: null,
  room: NV2MeshCore.parseRoom(location.hash),
  nick: localStorage.getItem('n2mesh:nick') || '',
  /* Increments on every room switch; stale async callbacks bail on mismatch. */
  session: 0,
  /* Relay (MQTT) state. */
  relay: { ws: null, connected: false, brokerIdx: 0, retry: 0, queue: [], presenceTimer: null },
  /* Message ids already seen — dedup across P2P / relay / BroadcastChannel. */
  seen: new Map(),
  /* Messages rendered this session, keyed per room — persisted to
   * localStorage so scrollback survives reloads. */
  history: {},
};

/* Per-room scrollback cap. */
const HISTORY_MAX = 100;

/* Persist the current room's history (best-effort; quota errors ignored). */
function saveHistory() {
  try {
    state.history[state.room] = C.trimHistory(state.history[state.room], HISTORY_MAX);
    localStorage.setItem('n2mesh:history', JSON.stringify(state.history));
  } catch (_) {}
}

/* Load persisted scrollback for `room` and render it. */
function loadHistory(room) {
  let saved = [];
  try {
    saved = C.trimHistory(JSON.parse(localStorage.getItem('n2mesh:history') || '{}')[room], HISTORY_MAX);
  } catch (_) {}
  state.history[room] = saved;
  for (const m of saved) addMessage(m.u, m.t, m.ts, false);
  if (saved.length > 0) {
    addSystem(`Restored ${saved.length} message${saved.length === 1 ? '' : 's'} from history.`);
  }
}

/* Append a message to the persisted room history. */
function recordHistory(msg) {
  if (!Array.isArray(state.history[state.room])) state.history[state.room] = [];
  state.history[state.room].push(msg);
  saveHistory();
}

/* ── Tiny helpers ───────────────────────────────────────────
 * Pure logic (bytes, ids, dedup, MQTT packets, room parsing) lives in
 * core.js so it can be unit-tested headlessly — see tests/core.test.js. */
const C = NV2MeshCore;
function fmtTime(ts) {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

/* ── UI ───────────────────────────────────────────────────── */
function setStatus(text, kind) {
  $('statusText').textContent = text;
  $('statusDot').className = 'dot' + (kind ? ' ' + kind : '');
}
function setPeers(n) {
  $('peerCount').textContent = n;
}
function addMessage(nick, text, ts, self) {
  const chat = $('chat');
  const welcome = $('welcome');
  if (welcome) welcome.remove();
  if (!self) recordHistory({ u: nick, t: text, ts: ts || Date.now() });

  const el = document.createElement('div');
  el.className = 'msg' + (self ? ' self' : '');
  const meta = document.createElement('div');
  meta.className = 'meta';
  const n = document.createElement('span');
  n.className = 'nick';
  n.textContent = self ? 'you' : nick;
  n.style.color = self ? '#7dd3fc' : C.nickColor(nick);
  const t = document.createElement('span');
  t.className = 'time';
  t.textContent = fmtTime(ts);
  meta.append(n, t);
  const b = document.createElement('div');
  b.className = 'body';
  b.textContent = text;
  el.append(meta, b);
  chat.appendChild(el);
  chat.scrollTop = chat.scrollHeight;
}
function addSystem(text) {
  const chat = $('chat');
  const el = document.createElement('div');
  el.className = 'msg system';
  el.textContent = text;
  chat.appendChild(el);
  chat.scrollTop = chat.scrollHeight;
}
/* Reflect current connectivity: P2P peers, local tabs, relay. */
function refreshStatus() {
  const p2p = state.peers.size + state.local.size;
  if (p2p > 0 && state.relay.connected) {
    setStatus(`connected · P2P + relay · room #${state.room}`, 'ok');
  } else if (p2p > 0) {
    setStatus(`connected · P2P · room #${state.room}`, 'ok');
  } else if (state.relay.connected) {
    setStatus(`connected · relay mode · room #${state.room}`, 'ok');
  } else {
    setStatus('connecting…', 'busy');
  }
}

/* ── Relay (MQTT 3.1.1 over WSS) ────────────────────────────
 * A tiny self-contained MQTT client (no dependency). Every message is
 * published to RELAY_TOPIC + room; each tab subscribes only to its own
 * room topic (rooms are isolated — no cross-room traffic). Works from
 * any static page on any network. */
function relaySubscribe() {
  const ws = state.relay.ws;
  if (!ws || ws.readyState !== 1) return;
  try { ws.send(C.mqttSubPkt(RELAY_TOPIC + state.room)); } catch (_) {}
}
function relayConnect() {
  if (!state.relay || state.relay.connected) return;
  const url = RELAY_BROKERS[state.relay.brokerIdx % RELAY_BROKERS.length];
  let ws;
  try {
    ws = new WebSocket(url);
  } catch (e) {
    relayRetry();
    return;
  }
  state.relay.ws = ws;
  ws.binaryType = 'arraybuffer';
  ws.onopen = () => {
    ws.send(C.mqttConnectPkt('n2-' + Math.random().toString(36).slice(2, 10)));
  };
  ws.onmessage = (ev) => {
    const d = new Uint8Array(ev.data);
    if (!d.length) return;
    const type = d[0] >> 4;
    if (type === 0x2) { // CONNACK
      if (d.length < 4 || d[3] !== 0) return;
      state.relay.connected = true;
      state.relay.retry = 0;
      state.relay.brokerIdx = 0; // reset — current broker works
      relaySubscribe();
    } else if (type === 0x9) { // SUBACK — subscription live, flush queue now
      /* flush anything queued while offline (each entry remembers its room) */
      const q = state.relay.queue;
      state.relay.queue = [];
      for (const item of q) relayPublish(item.payload, item.room);
      /* Announce our presence so other peers in this room can dial us. */
      publishPresence();
      if (!state.relay.presenceTimer) {
        state.relay.presenceTimer = setInterval(publishPresence, PRESENCE_MS);
      }
      refreshStatus();
      addSystem('Relay connected — chat works on any network.');
    } else if (type === 0x3) { // PUBLISH
      const m = C.mqttParsePublish(d);
      if (m && m.topic === RELAY_TOPIC + state.room) {
        handlePayload(m.payload);
      }
    } else if (type === 0xb) { // UNSUBACK — ignore
    } else if (type === 0xd) { // PINGRESP
      /* keepalive confirmed */
    }
  };
  ws.onclose = () => {
    if (state.relay.ws === ws) {
      state.relay.connected = false;
      state.relay.ws = null;
      refreshStatus();
      relayRetry();
    }
  };
  ws.onerror = () => {
    try { ws.close(); } catch (_) {}
  };
  /* keepalive every 30s (broker keepalive is 60s) */
  const ping = setInterval(() => {
    if (ws.readyState === 1) {
      try { ws.send(C.mqttPingPkt()); } catch (_) {}
    }
  }, 30000);
  ws.addEventListener('close', () => clearInterval(ping));
}
function relayRetry() {
  if (!state.relay) return;
  const delay = Math.min(1000 * Math.pow(2, state.relay.retry++), 15000);
  state.relay.retryTimer = setTimeout(() => {
    if (state.relay.retry >= 3) state.relay.brokerIdx = (state.relay.brokerIdx + 1) % RELAY_BROKERS.length;
    relayConnect();
  }, delay);
}
function relayPublish(payload, room) {
  room = room || state.room;
  if (state.relay.connected && state.relay.ws && state.relay.ws.readyState === 1) {
    try { state.relay.ws.send(C.mqttPubPkt(RELAY_TOPIC + room, payload)); } catch (_) {}
  } else {
    state.relay.queue.push({ payload, room });
    if (state.relay.queue.length > 100) state.relay.queue.shift();
  }
}

/* ── P2P (WebRTC over relay signaling) ────────────────────── */

/** Announce our presence on the room topic (JSON string). */
function publishPresence() {
  relayPublish(JSON.stringify({
    type: MSG_PRESENCE,
    pid: state.pid,
    nick: currentNick(),
    ts: Date.now(),
  }));
}

/** Send a signaling message addressed to a specific peer. */
function sendSignal(to, data) {
  relayPublish(JSON.stringify({
    type: MSG_SIGNAL,
    from: state.pid,
    to: to,
    data: data,
  }));
}

/** Handle an incoming presence announcement from another peer. */
function onPresence(p) {
  if (!p || !p.pid || p.pid === state.pid) return;
  if (state.pcs.has(p.pid)) return; // already connecting/connected
  /* Glare avoidance: the lexicographically smaller peer id sends the offer,
   * the larger one waits for it. Only one side ever dials. */
  if (state.pid < p.pid) {
    dial(p.pid);
  }
}

/** Initiate a WebRTC connection to another peer (offerer side). */
function dial(remotePid) {
  if (state.pcs.has(remotePid)) return;
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  const entry = { pc, dc: null };
  state.pcs.set(remotePid, entry);

  const dc = pc.createDataChannel('n2');
  entry.dc = dc;
  setupDataChannel(remotePid, entry, dc);

  pc.onicecandidate = (ev) => {
    if (ev.candidate) sendSignal(remotePid, { candidate: ev.candidate.toJSON ? ev.candidate.toJSON() : ev.candidate });
  };
  pc.oniceconnectionstatechange = () => {
    if (['failed', 'disconnected', 'closed'].includes(pc.iceConnectionState)) {
      teardown(remotePid, entry);
    }
  };

  pc.createOffer()
    .then((offer) => pc.setLocalDescription(offer))
    .then(() => sendSignal(remotePid, { sdp: pc.localDescription }))
    .catch(() => teardown(remotePid, entry));
}

/** Handle an incoming signaling message (offer / answer / candidate). */
function onSignal(sig) {
  if (!sig || sig.to !== state.pid) return;
  const data = sig.data || {};
  const remotePid = sig.from;

  if (data.sdp) {
    if (data.sdp.type === 'offer') {
      /* Answerer side. If we somehow already have a pc (glare edge case),
       * discard ours — the incoming offer wins. */
      if (state.pcs.has(remotePid)) teardown(remotePid, state.pcs.get(remotePid));
      const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
      const entry = { pc, dc: null };
      state.pcs.set(remotePid, entry);

      pc.ondatachannel = (ev) => {
        entry.dc = ev.channel;
        setupDataChannel(remotePid, entry, ev.channel);
      };
      pc.onicecandidate = (ev) => {
        if (ev.candidate) sendSignal(remotePid, { candidate: ev.candidate.toJSON ? ev.candidate.toJSON() : ev.candidate });
      };
      pc.oniceconnectionstatechange = () => {
        if (['failed', 'disconnected', 'closed'].includes(pc.iceConnectionState)) {
          teardown(remotePid, entry);
        }
      };

      pc.setRemoteDescription(data.sdp)
        .then(() => pc.createAnswer())
        .then((answer) => pc.setLocalDescription(answer))
        .then(() => sendSignal(remotePid, { sdp: pc.localDescription }))
        .catch(() => teardown(remotePid, entry));
    } else if (data.sdp.type === 'answer') {
      const entry = state.pcs.get(remotePid);
      if (entry && entry.pc && entry.pc.signalingState === 'have-local-offer') {
        entry.pc.setRemoteDescription(data.sdp).catch(() => teardown(remotePid, entry));
      }
    }
  } else if (data.candidate) {
    const entry = state.pcs.get(remotePid);
    if (entry && entry.pc && entry.pc.remoteDescription) {
      entry.pc.addIceCandidate(data.candidate).catch(() => {});
    }
  }
}

/** Wire up a data channel: messages in, peer counting, cleanup. */
function setupDataChannel(remotePid, entry, dc) {
  dc.onmessage = (ev) => {
    handlePayload(ev.data);
  };
  dc.onopen = () => {
    if (!state.peers.has(dc)) {
      state.peers.add(dc);
      updatePeers();
      addSystem('A peer connected — you are now talking P2P.');
    }
  };
  dc.onclose = () => {
    state.peers.delete(dc);
    state.pcs.delete(remotePid);
    updatePeers();
  };
}

/** Close and forget a connection (on failure or room switch). */
function teardown(remotePid, entry) {
  if (!entry) return;
  if (state.pcs.get(remotePid) === entry) state.pcs.delete(remotePid);
  if (entry.dc && state.peers.has(entry.dc)) {
    state.peers.delete(entry.dc);
    updatePeers();
  }
  try { entry.pc.close(); } catch (_) {}
}

/** Handle an incoming payload (chat, presence or signal) from any transport. */
function handlePayload(data) {
  let parsed;
  try {
    parsed = JSON.parse(typeof data === 'string' ? data : C.fromBytes(data));
  } catch (_) {
    return;
  }
  if (!parsed || typeof parsed !== 'object') return;
  if (parsed.type === MSG_PRESENCE) {
    onPresence(parsed);
    return;
  }
  if (parsed.type === MSG_SIGNAL) {
    onSignal(parsed);
    return;
  }
  if (typeof parsed.t === 'string') {
    if (!C.isNewMid(state.seen, parsed.mid, 30000, 500)) return; // already seen via another path
    addMessage(String(parsed.u || '?'), parsed.t, parsed.ts || Date.now(), false);
  }
}

/* BroadcastChannel bridge — tabs of the SAME browser can't do WebRTC
 * loopback, so they relay through a same-origin channel. This makes the
 * chat work out of the box when testing in two tabs. */
function initChannel() {
  try {
    state.channel = new BroadcastChannel(CHANNEL);
  } catch (_) {
    return; // not supported — P2P/relay only
  }
  state.channel.onmessage = (ev) => {
    if (!ev.data || ev.data.room !== state.room) return;
    if (ev.data.type === 'hello') {
      // A new same-browser peer appeared — greet it back (only once per peer,
      // otherwise tabs reply to each other's hellos forever).
      const uid = ev.data.uid;
      if (uid !== state.localUid && !state.local.has(uid)) {
        state.local.add(uid);
        updatePeers();
        state.channel.postMessage({
          type: 'hello', room: state.room, uid: state.localUid,
        });
        addSystem('Same-browser peer connected (local bridge).');
      }
      return;
    }
    if (ev.data.type === 'msg') handlePayload(ev.data.payload);
  };
  state.localUid = Math.random().toString(36).slice(2, 10);
  state.channel.postMessage({ type: 'hello', room: state.room, uid: state.localUid });
}

function updatePeers() {
  setPeers(state.peers.size + state.local.size);
  refreshStatus();
}

function initClient() {
  /* P2P needs no library — WebRTC is native. Presence announcements over the
   * relay are what let peers discover and dial each other. */
  addSystem(`Joined room “${state.room}”. Waiting for P2P peers — sharing the link works even over relay.`);
  refreshStatus();
}

function currentNick() {
  const v = $('nickInput').value.trim().slice(0, 24);
  return v || state.nick;
}

function sendMessage(text) {
  const msg = text.trim();
  if (!msg) return;
  const nick = currentNick();
  const payload = JSON.stringify({ u: nick, t: msg, ts: Date.now(), mid: C.newMid() });

  /* P2P — messages travel on WebRTC data channels when possible. */
  if (state.peers.size > 0) {
    for (const dc of state.peers) {
      try { dc.send(payload); } catch (_) {}
    }
  }
  /* Local tabs (same browser). */
  if (state.local.size > 0 && state.channel) {
    state.channel.postMessage({ type: 'msg', room: state.room, payload });
  }
  /* Relay — guaranteed delivery on any network (mobile CGNAT included). */
  relayPublish(payload);

  if (state.peers.size === 0 && state.local.size === 0 && !state.relay.connected) {
    addSystem('Not connected yet — message queued for the relay.');
  }
  addMessage(nick, msg, Date.now(), true);
}

/* ── Room switching ───────────────────────────────────────── */
function joinRoom(room) {
  room = (room || 'lobby').toLowerCase().slice(0, 48);
  if (room === state.room && state.pcs.size > 0) return;
  const oldRoom = state.room;
  state.room = room;
  location.hash = '/' + room;
  /* Fresh room → clear the DOM chat, then restore this room's scrollback. */
  const chat = $('chat');
  chat.replaceChildren();
  loadHistory(room);
  addSystem(`Switching to room “${room}” — reloading swarm…`);
  /* Invalidate everything async from the old room. */
  state.session++;
  /* Close all WebRTC connections and clear peer state. */
  for (const entry of state.pcs.values()) {
    try { entry.pc.close(); } catch (_) {}
  }
  state.pcs.clear();
  state.peers.clear();
  state.local.clear();
  setPeers(0);
  /* Move the relay subscription: drop the old room, pick up the new one. */
  if (state.relay.ws && state.relay.ws.readyState === 1) {
    try { state.relay.ws.send(C.mqttUnsubPkt(RELAY_TOPIC + oldRoom)); } catch (_) {}
  }
  relaySubscribe();
  /* Announce ourselves in the new room right away. */
  publishPresence();
  refreshStatus();
}

/* ── Boot ─────────────────────────────────────────────────── */
function boot() {
  if (!state.nick) {
    state.nick = 'guest-' + Math.random().toString(36).slice(2, 7);
  }
  $('nickInput').value = state.nick;
  $('roomInput').value = state.room;

  initChannel();
  relayConnect(); // relay is the signaling channel — always on

  $('sendBtn').addEventListener('click', () => {
    sendMessage($('msgInput').value);
    $('msgInput').value = '';
  });
  $('msgInput').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      sendMessage($('msgInput').value);
      $('msgInput').value = '';
    }
  });
  $('nickInput').addEventListener('change', (e) => {
    state.nick = e.target.value.trim().slice(0, 24) || state.nick;
    localStorage.setItem('n2mesh:nick', state.nick);
  });
  $('joinBtn').addEventListener('click', () => joinRoom($('roomInput').value));
  $('roomInput').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') joinRoom($('roomInput').value);
  });
  $('copyBtn').addEventListener('click', () => {
    const link = location.origin + location.pathname + '#/' + state.room;
    navigator.clipboard
      .writeText(link)
      .then(() => { addSystem('Room link copied to clipboard.'); })
      .catch(() => { addSystem('Could not copy — link: ' + link); });
  });

  window.addEventListener('hashchange', () => {
    const room = (location.hash.replace(/^#\/?/, '') || 'lobby').toLowerCase().slice(0, 48);
    if (room !== state.room) joinRoom(room);
  });

  initClient();
  loadHistory(state.room);
}

boot();
