from app.db import connect


def _migrate(conn) -> None:
    """对已存在的旧库补列/补种，保持可重复执行。"""
    cols = {
        r["name"] for r in conn.execute("PRAGMA table_info(ledger_cleanup_items)").fetchall()
    }
    if "detail" not in cols:
        conn.execute("ALTER TABLE ledger_cleanup_items ADD COLUMN detail TEXT")
    batch_cols = {
        r["name"]
        for r in conn.execute("PRAGMA table_info(ledger_cleanup_batches)").fetchall()
    }
    if "newly_protected_count" not in batch_cols:
        conn.execute(
            "ALTER TABLE ledger_cleanup_batches ADD COLUMN newly_protected_count INTEGER NOT NULL DEFAULT 0"
        )


def _seed_streams_and_settings(conn) -> None:
    if conn.execute("SELECT COUNT(*) c FROM streams").fetchone()["c"] == 0:
        conn.executemany(
            """
            INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    "Orders CDC",
                    "orders_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "clean：seq 1→5 连续；含一次 delete",
                ),
                (
                    "Inventory CDC",
                    "inventory_cdc.jsonl",
                    "inventory",
                    0,
                    1,
                    "clean：seq 1→3 连续库存变更",
                ),
                (
                    "Orders Archive CDC",
                    "orders_archive_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "历史夹具：event_ts 2025-09，用于账本过期清理演示",
                ),
            ],
        )
        conn.execute("INSERT INTO settings(key, value) VALUES ('project_label', 'Changeflow')")
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('gap_policy', 'stop_quarantine')"
        )
        conn.execute("INSERT INTO settings(key, value) VALUES ('retention_days', '30')")
        _seed_demo_holds(conn)
    else:
        # 旧库：补「历史账本」流与保留窗默认值。
        exists = conn.execute(
            "SELECT COUNT(*) c FROM streams WHERE name='Orders Archive CDC'"
        ).fetchone()["c"]
        if not exists:
            conn.execute(
                """
                INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    "Orders Archive CDC",
                    "orders_archive_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "历史夹具：event_ts 2025-09，用于账本过期清理演示",
                ),
            )
        if conn.execute(
            "SELECT COUNT(*) c FROM settings WHERE key='retention_days'"
        ).fetchone()["c"] == 0:
            conn.execute("INSERT INTO settings(key, value) VALUES ('retention_days', '30')")
        _seed_demo_holds(conn)


def _seed_demo_holds(conn) -> None:
    """演示冻结名单：仅当一条都没有时补种，避免覆盖用户登记的冻结。"""
    if conn.execute("SELECT COUNT(*) c FROM legal_holds").fetchone()["c"] > 0:
        return
    conn.execute(
        """
        INSERT INTO legal_holds(scope, stream_id, entity_key, start_ts, end_ts, reason, active, created_at)
        VALUES ('entity_key', NULL, 'ORD-0801', NULL, NULL, '演示：订单实体证据保全（实体键冻结）', 1, '2026-11-12T00:00:00Z')
        """
    )
    conn.execute(
        """
        INSERT INTO legal_holds(scope, stream_id, entity_key, start_ts, end_ts, reason, active, created_at)
        VALUES ('time_range', NULL, NULL, '2025-09-03T00:00:00Z', '2025-09-03T23:59:59Z', '演示：2025-09-03 时间窗证据保全', 1, '2026-11-12T00:00:00Z')
        """
    )


def init_db():
    conn = connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS streams(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            fixture_path TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            apply_cursor INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS apply_runs(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            loaded_count INTEGER NOT NULL,
            applied_count INTEGER NOT NULL,
            quarantined_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            cursor_before INTEGER NOT NULL,
            cursor_after INTEGER NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS change_ledger(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER,
            seq INTEGER NOT NULL,
            op TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            disposition TEXT NOT NULL,
            UNIQUE(stream_id, seq, run_id)
        );
        CREATE TABLE IF NOT EXISTS sink_records(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL,
            last_event_ts TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(stream_id, entity_key)
        );
        CREATE TABLE IF NOT EXISTS quarantine(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS applied_seqs(
            stream_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            applied_at TEXT NOT NULL,
            PRIMARY KEY(stream_id, seq)
        );
        CREATE TABLE IF NOT EXISTS legal_holds(
            id INTEGER PRIMARY KEY,
            scope TEXT NOT NULL CHECK(scope IN ('entity_key','time_range')),
            stream_id INTEGER,
            entity_key TEXT,
            start_ts TEXT,
            end_ts TEXT,
            reason TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            released_at TEXT
        );
        CREATE TABLE IF NOT EXISTS ledger_cleanup_batches(
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            retention_days INTEGER NOT NULL,
            cutoff_ts TEXT NOT NULL,
            scanned_count INTEGER NOT NULL DEFAULT 0,
            deletable_count INTEGER NOT NULL DEFAULT 0,
            frozen_retained_count INTEGER NOT NULL DEFAULT 0,
            cursor_protected_count INTEGER NOT NULL DEFAULT 0,
            quarantine_linked_count INTEGER NOT NULL DEFAULT 0,
            deleted_count INTEGER NOT NULL DEFAULT 0,
            newly_frozen_count INTEGER NOT NULL DEFAULT 0,
            newly_protected_count INTEGER NOT NULL DEFAULT 0,
            missing_at_execute_count INTEGER NOT NULL DEFAULT 0,
            ledger_count_at_preview INTEGER NOT NULL DEFAULT 0,
            ledger_count_before INTEGER,
            ledger_count_after INTEGER,
            active_hold_count INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            report_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            executed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS ledger_cleanup_items(
            id INTEGER PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            ledger_id INTEGER NOT NULL,
            stream_id INTEGER NOT NULL,
            run_id INTEGER,
            seq INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            disposition TEXT NOT NULL,
            decision TEXT NOT NULL,
            hold_id INTEGER,
            hold_reason TEXT,
            detail TEXT,
            execute_state TEXT,
            deleted INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_change_ledger_event_ts ON change_ledger(event_ts);
        CREATE INDEX IF NOT EXISTS idx_cleanup_items_batch ON ledger_cleanup_items(batch_id);
        CREATE TRIGGER IF NOT EXISTS trg_ledger_block_frozen_delete
        BEFORE DELETE ON change_ledger
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM legal_holds h
            WHERE h.active=1
              AND (h.stream_id IS NULL OR h.stream_id=OLD.stream_id)
              AND (
                    (h.scope='entity_key' AND h.entity_key=OLD.entity_key)
                 OR (h.scope='time_range'
                     AND (h.start_ts IS NULL OR OLD.event_ts >= h.start_ts)
                     AND (h.end_ts IS NULL OR OLD.event_ts <= h.end_ts))
              )
        )
        BEGIN
            SELECT RAISE(ABORT, 'ledger row is under active legal hold');
        END;
        """
    )
    _migrate(conn)
    _seed_streams_and_settings(conn)
    conn.commit()
    conn.close()
