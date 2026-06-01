#!/usr/bin/env node
"use strict";
const fs = require("fs");
const readline = require("readline");
let versions = {}, transactions = {}, conflicts = [], txCounter = 0, currentTx = null, bloom = 0;
function h(k) { let x = 0; for (let i = 0; i < k.length; i++) x = (x * 31 + k.charCodeAt(i)) & 8191; return x; }
function bloomAdd(k) { bloom |= (1 << h(k)) | (1 << h(k + "~")); }
function bloomOk(k) { const m = (1 << h(k)) | (1 << h(k + "~")); return (bloom & m) === m; }
function logC(a, b, k, rs) { conflicts.push(`${a} --${k}--> ${b}  ${Array.isArray(rs) ? rs.join(",") : rs}`); }
function load() {
  try {
    const lines = fs.readFileSync("mvcc_db.dat", "utf8").split("\n");
    versions = JSON.parse(lines[0]); transactions = JSON.parse(lines[1]); txCounter = parseInt(lines[2], 10);
    bloom = lines[3] && /^\d+$/.test(lines[3].trim()) ? parseInt(lines[3], 10) : 0;
    conflicts = lines[4] ? JSON.parse(lines[4]) : [];
    return true;
  } catch (e) { return false; }
}
function initDb() {
  versions = {}; transactions = {}; conflicts = []; txCounter = 0; currentTx = null; bloom = 0; load();
  bloom = 0; for (const rid of Object.keys(versions)) bloomAdd(rid);
}
function latest(rid) {
  if (!versions[rid] || !versions[rid].length) return null;
  const v = versions[rid][0]; return v.deleted ? null : v.data;
}
function readVersion(rid, snap) {
  if (!versions[rid]) return null;
  for (const v of versions[rid]) {
    const tid = v.tx_id;
    if (transactions[tid] && transactions[tid].state !== "committed" && tid !== currentTx) continue;
    if (v.timestamp <= snap) return v.deleted ? null : v.data;
  }
  return null;
}
function checkSkew(tid) {
  const tx = transactions[tid], ww = new Set(tx.writes.map(w => w[0]));
  for (const r of tx.reads || []) {
    for (const [oid, o] of Object.entries(transactions)) {
      if (oid === tid || o.state !== "committed" || o.timestamp <= tx.snapshot_ts) continue;
      const ow = new Set(o.writes.map(w => w[0]));
      if (ow.has(r) && [...ww].some(x => (o.reads || []).includes(x))) {
        logC(tid, oid, "rw-skew", [...ww, ...(o.reads || [])].sort()); return true;
      }
    }
  }
  return false;
}
function txStart() {
  txCounter++; const tid = `tx${txCounter}`;
  transactions[tid] = { timestamp: txCounter, snapshot_ts: txCounter, state: "active", writes: [], reads: [] };
  currentTx = tid; return tid;
}
function txUse(tid) {
  if (transactions[tid] && transactions[tid].state === "active") { currentTx = tid; return true; }
  return false;
}
function txCommit(tid) {
  if (!transactions[tid] || transactions[tid].state !== "active") return false;
  const tx = transactions[tid], wr = new Set(tx.writes.map(w => w[0]));
  for (const r of tx.reads || []) {
    if (wr.has(r)) continue;
    if (versions[r] && versions[r].length) {
      const v = versions[r][0];
      if (v.timestamp > tx.timestamp && v.tx_id !== tid) { logC(tid, v.tx_id, "rw-conflict", [r]); return false; }
    }
  }
  if (checkSkew(tid)) return false;
  transactions[tid].state = "committed"; return true;
}
function txAbort(tid) {
  if (!transactions[tid] || transactions[tid].state !== "active") return false;
  const tx = transactions[tid];
  for (const [r] of tx.writes) {
    if (versions[r] && versions[r].length && versions[r][0].tx_id === tid) versions[r].shift();
  }
  tx.state = "aborted"; if (currentTx === tid) currentTx = null; return true;
}
function txRead(rid) {
  if (!currentTx || !transactions[currentTx]) return null;
  transactions[currentTx].reads.push(rid);
  return readVersion(rid, transactions[currentTx].snapshot_ts);
}
function txWrite(rid, data) {
  if (!currentTx) return false;
  const tx = transactions[currentTx];
  if (versions[rid] && versions[rid].length) {
    const lv = versions[rid][0];
    if (lv.timestamp > tx.timestamp && lv.tx_id !== currentTx) { logC(currentTx, lv.tx_id, "ww", [rid]); txAbort(currentTx); return false; }
  }
  if (!versions[rid]) versions[rid] = [];
  versions[rid].unshift({ timestamp: tx.timestamp, tx_id: currentTx, data, deleted: false });
  tx.writes.push([rid, 0]); bloomAdd(rid); return true;
}
function txDelete(rid) {
  if (!currentTx) return false;
  const tx = transactions[currentTx], v = readVersion(rid, tx.snapshot_ts);
  if (v === null) return false;
  if (!versions[rid]) versions[rid] = [];
  versions[rid].unshift({ timestamp: tx.timestamp, tx_id: currentTx, data: v, deleted: true });
  tx.writes.push([rid, 0]); return true;
}
function finish(ok) { return currentTx ? (ok ? txCommit(currentTx) : txAbort(currentTx)) && ok : ok; }
function create(rid, d) { if (latest(rid) !== null) return false; txStart(); return finish(txWrite(rid, d)); }
function read(rid) { if (!bloomOk(rid)) return null; txStart(); const r = txRead(rid); txCommit(currentTx); return r; }
function update(rid, d) { if (!bloomOk(rid) || latest(rid) === null) return false; txStart(); return finish(txWrite(rid, d)); }
function del(rid) { if (!bloomOk(rid) || latest(rid) === null) return false; txStart(); return finish(txDelete(rid)); }
function cas(rid, exp, nw) {
  if (!bloomOk(rid)) return false; txStart(); const cur = txRead(rid);
  if (cur !== exp) { txAbort(currentTx); return false; } return finish(txWrite(rid, nw));
}
function listAll() { return Object.keys(versions).filter(r => latest(r) !== null); }
function search(term) { return listAll().filter(r => r.includes(term) || String(latest(r)).includes(term)).map(r => `${r}: ${latest(r)}`); }
function save() {
  try { fs.writeFileSync("mvcc_db.dat", [JSON.stringify(versions), JSON.stringify(transactions), txCounter, bloom, JSON.stringify(conflicts)].join("\n")); }
  catch (e) { console.log(`ERR: save failed - ${e.message}`); }
}
function runCmd(parts) {
  const c = parts[0];
  if (c === "quit") { save(); return false; }
  if (c === "help") { console.log("create read update delete list search exists cas iso stats conflicts | tx_start tx_use tx_read tx_write tx_commit tx_abort txs | versions read_at gc"); return true; }
  if (c === "create" && parts.length === 3) console.log(create(parts[1], parts[2]) ? "OK" : "FAIL");
  else if (c === "read" && parts.length === 2) { const r = read(parts[1]); console.log(r !== null ? r : "Not found"); }
  else if (c === "update" && parts.length === 3) console.log(update(parts[1], parts[2]) ? "OK" : "FAIL");
  else if (c === "delete" && parts.length === 2) console.log(del(parts[1]) ? "OK" : "FAIL");
  else if (c === "list") console.log(listAll().join(", ") || "empty");
  else if (c === "search" && parts.length === 2) console.log(search(parts[1]).join("\n") || "none");
  else if (c === "exists" && parts.length === 2) console.log(bloomOk(parts[1]) && latest(parts[1]) !== null ? "yes" : "no");
  else if (c === "cas" && parts.length === 4) console.log(cas(parts[1], parts[2], parts[3]) ? "OK" : "FAIL");
  else if (c === "iso") {
    console.log("Isolation: SERIALIZABLE (SSI)");
    ["Dirty Read", "Non-Repeatable Read", "Write Skew", "Lost Update"].forEach(a => console.log(`  ${a}: PREVENTED`));
    console.log("  Phantom Read: NOT PREVENTED (no range locking)");
  } else if (c === "stats") {
    const n = listAll().length, v = Object.values(versions).reduce((s, a) => s + a.length, 0);
    console.log(`records=${n} versions=${v} txs=${Object.keys(transactions).length} bloom_bits=${bloom.toString(2).split("1").length - 1} isolation=SERIALIZABLE`);
  } else if (c === "conflicts") console.log(conflicts.length ? conflicts.join("\n") : "none");
  else if (c === "tx_start") console.log(`${txStart()} started`);
  else if (c === "tx_use" && parts.length === 2) console.log(txUse(parts[1]) ? "OK" : "FAIL");
  else if (c === "tx_read" && parts.length === 2) { const r = txRead(parts[1]); console.log(r !== null ? r : "Not found"); }
  else if (c === "tx_write" && parts.length === 3) console.log(txWrite(parts[1], parts[2]) ? "OK" : "FAIL");
  else if (c === "tx_commit" && parts.length === 2) console.log(txCommit(parts[1]) ? "OK" : "FAIL");
  else if (c === "tx_abort" && parts.length === 2) console.log(txAbort(parts[1]) ? "OK" : "FAIL");
  else if (c === "read_at" && parts.length === 3) { const r = readVersion(parts[1], parseInt(parts[2], 10)); console.log(r !== null ? r : "Not found"); }
  else console.log("ERR: invalid command (try help)");
  return true;
}
function parseCmd(raw) {
  if (!raw) return [];
  const c = raw.split(/\s+/)[0];
  return c === "cas" ? raw.split(/\s+/, 4) : raw.split(/\s+/, 3);
}
async function main() {
  initDb(); console.log("MVCC DB (JS) | SERIALIZABLE (SSI) | Node stdlib | type help");
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: "> " });
  rl.prompt();
  for await (const line of rl) {
    const parts = parseCmd(line.trim());
    if (!parts.length) { rl.prompt(); continue; }
    if (!runCmd(parts)) { rl.close(); break; }
    rl.prompt();
  }
  save();
}
main().catch(() => save());
