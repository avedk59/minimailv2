/* n2-mesh core — pure logic shared by the browser app and the Node test
 * suite. No DOM, no WebSocket, no global state: everything here is a pure
 * function of its inputs, so it can be unit-tested headlessly. */
(function (root, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = factory();
  } else {
    root.NV2MeshCore = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /* ── bytes ─────────────────────────────────────────────────── */
  function toBytes(str) {
    if (typeof Buffer !== 'undefined') return Buffer.from(str, 'utf8');
    return new TextEncoder().encode(str);
  }
  function fromBytes(data) {
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    return new TextDecoder().decode(bytes);
  }

  /* ── nicks & ids ───────────────────────────────────────────── */
  function nickColor(name) {
    let h = 0;
    for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
    return `hsl(${h % 360} 70% 62%)`;
  }
  function newMid() {
    return Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10);
  }

  /* Dedup: returns true if the id is new (should be displayed).
   * `seen` is the caller's Map (persisted across calls); expired entries
   * are pruned and the map is capped so memory stays bounded. */
  function isNewMid(seen, mid, ttlMs, max) {
    if (!mid) return true;
    const now = Date.now();
    for (const [k, t] of seen) {
      if (now - t > ttlMs) seen.delete(k);
    }
    if (seen.has(mid)) return false;
    if (seen.size > max) seen.clear();
    seen.set(mid, now);
    return true;
  }

  /* Trim a chat-history array to the last `max` entries, dropping
   * everything non-object. Returns a NEW array; the input is untouched.
   * Used to cap per-room scrollback persisted to localStorage. */
  function trimHistory(messages, max) {
    if (!Array.isArray(messages)) return [];
    const clean = messages.filter(function (m) {
      return m && typeof m === 'object';
    });
    return max >= 0 ? clean.slice(-max) : clean;
  }

  /* Normalise a location hash into a room name: strip `#/`, lowercase,
   * cap at 48 chars, default to 'lobby'. The cap is applied per Unicode
   * code point (not UTF-16 code unit) so astral chars like 🚀 are never
   * split into a lone surrogate. */
  function parseRoom(hash, fallback) {
    const room = (hash == null ? '' : hash).replace(/^#\/?/, '') || fallback || 'lobby';
    return Array.from(room.toLowerCase()).slice(0, 48).join('') || 'lobby';
  }

  /* ── MQTT 3.1.1 remaining length ───────────────────────────── */
  /* Encode a remaining length into its 1-4 byte LSB-first form
   * (7 bits per byte, continuation bit 0x80 on all but the last).
   * The spec's maximum is 268 435 455 (0xFF 0xFF 0xFF 0x7F). */
  function mqttRemainingLengthBytes(len) {
    if (!Number.isInteger(len) || len < 0 || len > 268435455) {
      throw new RangeError('MQTT remaining length out of range: ' + len);
    }
    const rem = [];
    do {
      let d = len % 128;
      len = Math.floor(len / 128);
      if (len > 0) d |= 0x80;
      rem.push(d);
    } while (len > 0);
    return rem;
  }

  /* Decode a remaining length starting at `d[start]`. Returns
   * { length, consumed } or null when the packet is malformed
   * (truncated, continuation bit set past 4 bytes, > 268 435 455). */
  function mqttRemainingLength(d, start) {
    let i = start;
    let multiplier = 1;
    let value = 0;
    let count = 0;
    for (;;) {
      if (i >= d.length || count >= 4) return null;
      const b = d[i];
      value += (b & 0x7f) * multiplier;
      multiplier *= 128;
      count++;
      i++;
      if (!(b & 0x80)) break;
    }
    return { length: value, consumed: count };
  }

  /* ── MQTT 3.1.1 packet layer ───────────────────────────────── */
  function mqttEncode(type, body) {
    const b = toBytes(body);
    const rem = mqttRemainingLengthBytes(b.length);
    return Uint8Array.from([type, ...rem, ...b]);
  }
  function mqttConnectPkt(clientId) {
    const cid = toBytes(clientId);
    /* protocol name "MQTT", level 4, clean session, keepalive 60 */
    const vh = [0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x3c];
    const pl = [0x00, cid.length, ...cid];
    return mqttEncode(0x10, [...vh, ...pl]);
  }
  function mqttSubPkt(topic) {
    const t = toBytes(topic);
    return mqttEncode(0x82, [0x00, 0x01, 0x00, t.length, ...t, 0x00]); // qos 0
  }
  function mqttPubPkt(topic, payload) {
    const t = toBytes(topic);
    return mqttEncode(0x30, [0x00, t.length, ...t, ...payload]);
  }
  function mqttPingPkt() {
    return Uint8Array.from([0xc0, 0x00]);
  }
  function mqttUnsubPkt(topic) {
    const t = toBytes(topic);
    return mqttEncode(0xa2, [0x00, 0x01, 0x00, t.length, ...t]);
  }
  /* Parse a PUBLISH packet. Returns { topic, payload } or null when the
   * packet is malformed (bad remaining length, truncated topic, missing
   * packet id). Payload is always bounded by the decoded remaining
   * length, so trailing bytes can never leak into a message. */
  function mqttParsePublish(d) {
    const rl = mqttRemainingLength(d, 1);
    if (!rl) return null;
    const remaining = rl.length;
    let i = 1 + rl.consumed;
    if (remaining > d.length - i) return null; // body shorter than declared
    if (remaining < 2) return null; // need at least the topic length field
    const tlen = (d[i] << 8) | d[i + 1];
    i += 2;
    if (tlen > remaining - 2 || i + tlen > d.length) return null;
    const topic = fromBytes(d.slice(i, i + tlen));
    i += tlen;
    if (d[0] & 0x06) { // packet id (QoS level > 0)
      if (i + 2 > d.length) return null;
      i += 2;
    }
    return { topic, payload: d.slice(i, 1 + rl.consumed + remaining) };
  }

  return {
    toBytes,
    fromBytes,
    nickColor,
    newMid,
    isNewMid,
    parseRoom,
    trimHistory,
    mqttRemainingLengthBytes,
    mqttRemainingLength,
    mqttEncode,
    mqttConnectPkt,
    mqttSubPkt,
    mqttPubPkt,
    mqttPingPkt,
    mqttUnsubPkt,
    mqttParsePublish,
  };
});
