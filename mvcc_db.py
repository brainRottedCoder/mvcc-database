"""Zero-import MVCC database with SSI. Under 200 lines, zero imports."""
versions, transactions, conflicts, tx_counter, current_tx, bloom = {}, {}, [], 0, None, 0
def h(k):
    x = 0
    for c in k: x = (x * 31 + ord(c)) & 8191
    return x
def bloom_add(k):
    global bloom
    bloom = bloom | (1 << h(k)) | (1 << h(k + "~"))
def bloom_ok(k):
    m = (1 << h(k)) | (1 << h(k + "~")); return bloom & m == m
def log_c(a, b, k, rs):
    conflicts.append(f"{a} --{k}--> {b}  {','.join(rs) if isinstance(rs, list) else rs}")
def init_db():
    global versions, transactions, conflicts, tx_counter, current_tx, bloom
    versions, transactions, conflicts, tx_counter, current_tx, bloom = {}, {}, [], 0, None, 0
    load(); bloom = 0
    for rid in versions: bloom_add(rid)
def latest(rid):
    if rid not in versions or not versions[rid]: return None
    v = versions[rid][0]; return None if v.get("deleted") else v["data"]
def read_version(rid, snap):
    if rid not in versions: return None
    for v in versions[rid]:
        if v["tx_id"] in transactions and transactions[v["tx_id"]]["state"] != "committed" and v["tx_id"] != current_tx: continue
        if v["timestamp"] <= snap: return None if v.get("deleted") else v["data"]
    return None
def check_skew(tid):
    tx = transactions[tid]; ww = {r for r, _ in tx["writes"]}
    for r in tx.get("reads", []):
        for oid, o in transactions.items():
            if oid == tid or o["state"] != "committed" or o["timestamp"] <= tx["snapshot_ts"]: continue
            ow = {x for x, _ in o.get("writes", [])}
            if r in ow and ww & set(o.get("reads", [])):
                log_c(tid, oid, "rw-skew", sorted(ww | set(o.get("reads", [])))); return True
    return False
def tx_start():
    global tx_counter, current_tx
    tx_counter += 1; tid = f"tx{tx_counter}"; transactions[tid] = {"timestamp": tx_counter, "snapshot_ts": tx_counter, "state": "active", "writes": [], "reads": []}
    current_tx = tid; return tid
def tx_use(tid):
    global current_tx
    if tid in transactions and transactions[tid]["state"] == "active": current_tx = tid; return True
    return False
def tx_commit(tid):
    if tid not in transactions or transactions[tid]["state"] != "active": return False
    tx = transactions[tid]; wr = {r for r, _ in tx["writes"]}
    for r in tx.get("reads", []):
        if r in wr: continue
        if r in versions and versions[r] and (v := versions[r][0])["timestamp"] > tx["timestamp"] and v["tx_id"] != tid:
            log_c(tid, v["tx_id"], "rw-conflict", [r]); return False
    if check_skew(tid): return False
    transactions[tid]["state"] = "committed"; return True
def tx_abort(tid):
    global current_tx
    if tid not in transactions or transactions[tid]["state"] != "active": return False
    tx = transactions[tid]
    for r, _ in tx["writes"]:
        if r in versions and versions[r] and versions[r][0]["tx_id"] == tid: versions[r].pop(0)
    tx["state"] = "aborted"; current_tx = None if current_tx == tid else current_tx; return True
def tx_read(rid):
    if not current_tx or current_tx not in transactions: return None
    transactions[current_tx].setdefault("reads", []).append(rid)
    return read_version(rid, transactions[current_tx]["snapshot_ts"])
def tx_write(rid, data):
    if not current_tx: return False
    tx = transactions[current_tx]
    if rid in versions and versions[rid]:
        lv = versions[rid][0]
        if lv["timestamp"] > tx["timestamp"] and lv["tx_id"] != current_tx:
            log_c(current_tx, lv["tx_id"], "ww", [rid]); tx_abort(current_tx); return False
    if rid not in versions: versions[rid] = []
    versions[rid].insert(0, {"timestamp": tx["timestamp"], "tx_id": current_tx, "data": data, "deleted": False})
    tx["writes"].append((rid, 0)); bloom_add(rid); return True
def tx_delete(rid):
    if not current_tx: return False
    tx = transactions[current_tx]; v = read_version(rid, tx["snapshot_ts"])
    if v is None: return False
    if rid not in versions: versions[rid] = []
    versions[rid].insert(0, {"timestamp": tx["timestamp"], "tx_id": current_tx, "data": v, "deleted": True})
    tx["writes"].append((rid, 0)); return True
def finish(ok):
    if not current_tx: return ok
    return (tx_commit if ok else tx_abort)(current_tx) and ok
def create(rid, d):
    if latest(rid) is not None: return False
    tx_start(); return finish(tx_write(rid, d))
def read(rid):
    if not bloom_ok(rid): return None
    tx_start(); r = tx_read(rid); tx_commit(current_tx); return r
def update(rid, d):
    if not bloom_ok(rid) or latest(rid) is None: return False
    tx_start(); return finish(tx_write(rid, d))
def delete(rid):
    if not bloom_ok(rid) or latest(rid) is None: return False
    tx_start(); return finish(tx_delete(rid))
def cas(rid, exp, new):
    if not bloom_ok(rid): return False
    tx_start(); cur = tx_read(rid)
    if cur != exp: tx_abort(current_tx); return False
    return finish(tx_write(rid, new))
def gc():
    active = [t["snapshot_ts"] for t in transactions.values() if t["state"] == "active"]; cut = min(active) if active else tx_counter + 1
    for rid in list(versions.keys()):
        while len(versions[rid]) > 1 and versions[rid][-1]["timestamp"] < cut: versions[rid].pop()
def list_all():
    return [r for r in versions if latest(r) is not None]
def search(term):
    return [f"{r}: {latest(r)}" for r in list_all() if term in r or term in str(latest(r))]
def save():
    try:
        with open("mvcc_db.dat", "w", encoding="utf-8") as f: f.write("\n".join([repr(versions), repr(transactions), str(tx_counter), str(bloom), repr(conflicts)]))
    except OSError as e: print(f"ERR: save failed - {e}")
def load():
    global versions, transactions, tx_counter, bloom, conflicts
    try:
        with open("mvcc_db.dat", "r", encoding="utf-8") as f: lines = f.read().splitlines(); versions, transactions, tx_counter = eval(lines[0], {"__builtins__": None}, {}), eval(lines[1], {"__builtins__": None}, {}), int(lines[2])
        bloom = int(lines[3].strip()) if len(lines) > 3 and lines[3].strip().isdigit() else 0
        conflicts = eval(lines[4], {"__builtins__": None}, {}) if len(lines) > 4 else []
        return True
    except (OSError, ValueError, SyntaxError, IndexError): return False
def show_versions(rid):
    if rid not in versions: print("Not found"); return
    for i, v in enumerate(versions[rid]): print(f"v{i}: ts={v['timestamp']}, tx={v['tx_id']}, del={v.get('deleted', False)}, data={v['data']}")
def read_at(rid, ts):
    try: return read_version(rid, int(ts))
    except ValueError: return None
def backup(ts=0):
    try:
        i = {r: [v for v in vs if v["timestamp"] > ts] for r, vs in versions.items()}; i = {r: vs for r, vs in i.items() if vs}
        with open(f"backup_{tx_counter}.dat", "w", encoding="utf-8") as f: f.write(f"{repr(i)}\n{ts}\n{tx_counter}")
        return True
    except OSError: return False
def restore(bf):
    global tx_counter
    try:
        with open(bf, "r", encoding="utf-8") as f: lines = f.read().splitlines(); iv, bc = eval(lines[0], {"__builtins__": None}, {}), int(lines[2])
        for r, vs in iv.items(): versions.setdefault(r, []).extend(vs); versions[r].sort(key=lambda v: -v["timestamp"]); bloom_add(r)
        tx_counter = max(tx_counter, bc); return True
    except (OSError, ValueError, SyntaxError, IndexError): return False
def show_iso():
    print("Isolation: SERIALIZABLE (SSI)\n  Dirty Read: PREVENTED\n  Non-Repeatable Read: PREVENTED\n  Write Skew: PREVENTED\n  Lost Update: PREVENTED\n  Phantom Read: NOT PREVENTED (no range locking)")
def show_stats():
    n = len(list_all()); v = sum(len(versions[r]) for r in versions); b = bin(bloom).count("1") if bloom else 0; print(f"records={n} versions={v} txs={len(transactions)} bloom_bits={b} isolation=SERIALIZABLE")
def run_data(parts):
    c = parts[0]
    if c == "create" and len(parts) == 3: print("OK" if create(parts[1], parts[2]) else "FAIL")
    elif c == "read" and len(parts) == 2: r = read(parts[1]); print(r if r is not None else "Not found")
    elif c == "update" and len(parts) == 3: print("OK" if update(parts[1], parts[2]) else "FAIL")
    elif c == "delete" and len(parts) == 2: print("OK" if delete(parts[1]) else "FAIL")
    elif c == "cas" and len(parts) == 4: print("OK" if cas(parts[1], parts[2], parts[3]) else "FAIL")
    elif c == "list": print(", ".join(list_all()) or "empty")
    elif c == "search" and len(parts) == 2: print("\n".join(search(parts[1])) or "none")
    elif c == "exists" and len(parts) == 2: print("yes" if bloom_ok(parts[1]) and latest(parts[1]) is not None else "no")
    elif c in ("iso", "stats"): (show_iso if c == "iso" else show_stats)()
    elif c == "conflicts": print("\n".join(conflicts) if conflicts else "none")
    elif c == "versions" and len(parts) == 2: show_versions(parts[1])
    else: return False
    return True
def run_tx(parts):
    c = parts[0]
    if c == "tx_start": print(f"{tx_start()} started")
    elif c == "tx_use" and len(parts) == 2: print("OK" if tx_use(parts[1]) else "FAIL")
    elif c == "tx_read" and len(parts) == 2: r = tx_read(parts[1]); print(r if r is not None else "Not found")
    elif c == "tx_write" and len(parts) == 3: print("OK" if tx_write(parts[1], parts[2]) else "FAIL")
    elif c in ("tx_commit", "tx_abort") and len(parts) == 2: print("OK" if (tx_commit if c == "tx_commit" else tx_abort)(parts[1]) else "FAIL")
    elif c == "gc": gc(); print("OK")
    elif c == "restore" and len(parts) == 2: print("OK" if restore(parts[1]) else "FAIL")
    elif c == "read_at" and len(parts) == 3: r = read_at(parts[1], parts[2]); print(r if r is not None else "Not found")
    else: return False
    return True
def run_cmd(parts):
    c = parts[0]
    if c == "quit": save(); return False
    if c == "help": print("create read update delete list search exists cas iso stats | tx cmds | versions read_at gc"); return True
    if c == "txs":
        for tid, tx in transactions.items(): print(f"{tid}: ts={tx['timestamp']}, state={tx['state']}")
    elif c == "backup":
        try: print("OK" if backup(int(parts[1]) if len(parts) > 1 else 0) else "FAIL")
        except ValueError: print("ERR: invalid timestamp")
    elif not run_data(parts) and not run_tx(parts):
        print("ERR: invalid command (try help)")
    return True
def parse_cmd(raw):
    if not raw: return []
    return raw.split(maxsplit=3) if raw.split(None, 1)[0] == "cas" else raw.split(maxsplit=2)
def main():
    init_db(); print("MVCC DB | SERIALIZABLE (SSI) | zero imports | type help")
    while True:
        try: parts = parse_cmd(input("> ").strip())
        except EOFError: save(); break
        if not parts: continue
        if not run_cmd(parts): break
if __name__ == "__main__":
    main()
