//
//  DatabaseManager.swift
//  ScoutProcess
//

import Foundation
import GRDB

final class DatabaseManager {
    static let shared = DatabaseManager()

    let databaseURL: URL
    let dbQueue: DatabaseQueue?

    private init(fileManager: FileManager = .default) {
        let documentsDirectory = fileManager.homeDirectoryForCurrentUser
            .appendingPathComponent("Documents", isDirectory: true)
        let databaseDirectory = documentsDirectory
            .appendingPathComponent("ScoutProcess", isDirectory: true)
            .appendingPathComponent("Database", isDirectory: true)

        databaseURL = databaseDirectory.appendingPathComponent("scoutdatabase.sqlite", isDirectory: false)

        do {
            try fileManager.createDirectory(at: databaseDirectory, withIntermediateDirectories: true, attributes: nil)
            var configuration = Configuration()
            configuration.prepareDatabase { db in
                try db.execute(sql: "PRAGMA foreign_keys = ON")
            }

            dbQueue = try DatabaseQueue(path: databaseURL.path, configuration: configuration)
        } catch {
            dbQueue = nil
            log("Database bootstrap failed for \(databaseURL.path): \(error.localizedDescription)")
        }
    }

    func migrateIfNeeded() {
        guard let dbQueue else {
            log("Database migration skipped because the database queue is unavailable.")
            return
        }

        var migrator = DatabaseMigrator()

        migrator.registerMigration("createAppMeta") { db in
            try db.create(table: "app_meta", ifNotExists: true) { table in
                table.column("key", .text).primaryKey()
                table.column("value", .text).notNull()
            }

            let timestamp = ISO8601DateFormatter().string(from: Date())
            try db.execute(
                sql: """
                INSERT OR REPLACE INTO app_meta (key, value)
                VALUES (?, ?)
                """,
                arguments: ["db_initialized", timestamp]
            )
        }

        migrator.registerMigration("createProductionSchema") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    property_id TEXT NOT NULL,
                    org_id TEXT,
                    org_name TEXT,
                    folder_id TEXT,
                    property_name TEXT,
                    property_address TEXT,
                    propertyStreet TEXT,
                    propertyCity TEXT,
                    propertyState TEXT,
                    propertyZip TEXT,
                    primary_contact_name TEXT,
                    primary_contact_phone TEXT,
                    primary_contact_email TEXT,
                    started_at_utc TEXT NOT NULL,
                    ended_at_utc TEXT,
                    is_baseline INTEGER NOT NULL DEFAULT 0,
                    status TEXT,
                    schema_version TEXT,
                    app_version TEXT,
                    time_zone TEXT,
                    capture_profile TEXT,
                    imported_at TEXT NOT NULL,
                    zip_name TEXT
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_sessions_property_started
                ON sessions (property_id, started_at_utc)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_sessions_org
                ON sessions (org_id)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS shots (
                    shot_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    property_id TEXT NOT NULL,
                    propertyStreet TEXT,
                    propertyCity TEXT,
                    propertyState TEXT,
                    propertyZip TEXT,
                    building TEXT,
                    elevation TEXT,
                    detail_type TEXT,
                    angle_index INTEGER,
                    shot_key TEXT,
                    logical_shot_identity TEXT,
                    capture_kind TEXT,
                    is_flagged INTEGER NOT NULL DEFAULT 0,
                    is_guided INTEGER NOT NULL DEFAULT 0,
                    issue_id TEXT,
                    captured_at_utc TEXT,
                    latitude REAL,
                    longitude REAL,
                    lens TEXT,
                    original_filename TEXT NOT NULL,
                    original_byte_size INTEGER,
                    capture_profile TEXT,
                    trade TEXT,
                    priority TEXT
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_session
                ON shots (session_id)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_property
                ON shots (property_id)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_shot_key
                ON shots (shot_key)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_logical_identity
                ON shots (logical_shot_identity)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_flagged
                ON shots (is_flagged)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_issue
                ON shots (issue_id)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS issues (
                    issue_id TEXT PRIMARY KEY,
                    property_id TEXT NOT NULL,
                    propertyStreet TEXT,
                    propertyCity TEXT,
                    propertyState TEXT,
                    propertyZip TEXT,
                    first_seen_session_id TEXT,
                    last_capture_session_id TEXT,
                    current_status TEXT,
                    current_reason TEXT,
                    previous_reason TEXT,
                    first_seen_at_utc TEXT,
                    last_seen_at_utc TEXT,
                    resolved_at_utc TEXT,
                    shot_key TEXT
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_issues_property_status
                ON issues (property_id, current_status)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_issues_shot_key
                ON issues (shot_key)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS issue_history (
                    event_id TEXT PRIMARY KEY,
                    issue_id TEXT NOT NULL REFERENCES issues(issue_id) ON DELETE CASCADE,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    event_type TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    field_changed TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    shot_id TEXT
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_issue_history_issue_time
                ON issue_history (issue_id, timestamp_utc)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_issue_history_session
                ON issue_history (session_id)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS guided_rows (
                    guided_row_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    property_id TEXT NOT NULL,
                    propertyStreet TEXT,
                    propertyCity TEXT,
                    propertyState TEXT,
                    propertyZip TEXT,
                    building TEXT,
                    elevation TEXT,
                    detail_type TEXT,
                    angle_index INTEGER,
                    status TEXT,
                    is_retired INTEGER NOT NULL DEFAULT 0,
                    retired_at TEXT,
                    skip_reason TEXT,
                    skip_session_id TEXT,
                    trade TEXT,
                    priority TEXT
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_guided_rows_property
                ON guided_rows (property_id)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_guided_rows_session
                ON guided_rows (session_id)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_guided_rows_status
                ON guided_rows (status)
                """)
        }

        migrator.registerMigration("createImportRuns") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS import_runs (
                    import_run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    session_id TEXT,
                    folder_path TEXT NOT NULL,
                    zip_name TEXT,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    rows_sessions INTEGER NOT NULL DEFAULT 0,
                    rows_shots INTEGER NOT NULL DEFAULT 0,
                    rows_guided_rows INTEGER NOT NULL DEFAULT 0,
                    rows_issues INTEGER NOT NULL DEFAULT 0,
                    rows_issue_history INTEGER NOT NULL DEFAULT 0
                )
                """)
        }

        migrator.registerMigration("addZipImportsAndShotMetadata") { db in
            let shotColumnNames = try Set(db.columns(in: "shots").map(\.name))
            if shotColumnNames.contains("stamped_jpeg_filename") == false {
                try db.execute(sql: """
                    ALTER TABLE shots ADD COLUMN stamped_jpeg_filename TEXT
                    """)
            }
            if shotColumnNames.contains("flagged_reason") == false {
                try db.execute(sql: """
                    ALTER TABLE shots ADD COLUMN flagged_reason TEXT
                    """)
            }

            try db.execute(sql: """
                UPDATE shots
                SET shot_key = shot_id
                WHERE shot_key IS NULL OR TRIM(shot_key) = ''
                """)

            try db.execute(sql: """
                DELETE FROM shots
                WHERE rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM shots
                    GROUP BY session_id, shot_key
                )
                """)

            try db.execute(sql: """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_shots_session_shot_key_unique
                ON shots (session_id, shot_key)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS zip_imports (
                    id INTEGER PRIMARY KEY,
                    zipFingerprint TEXT NOT NULL UNIQUE,
                    zipFilename TEXT NOT NULL,
                    importedAt DATETIME NOT NULL,
                    sessionID TEXT NOT NULL
                )
                """)
        }

        migrator.registerMigration("addLogicalShotIdentity") { db in
            let shotColumnNames = try Set(db.columns(in: "shots").map(\.name))
            if shotColumnNames.contains("logical_shot_identity") == false {
                try db.execute(sql: """
                    ALTER TABLE shots ADD COLUMN logical_shot_identity TEXT
                    """)
            }

            try db.execute(sql: """
                UPDATE shots
                SET logical_shot_identity = COALESCE(NULLIF(TRIM(logical_shot_identity), ''), NULLIF(TRIM(shot_key), ''), shot_id)
                """)

            try db.execute(sql: """
                DELETE FROM shots
                WHERE rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM shots
                    GROUP BY session_id, logical_shot_identity
                )
                """)

            try db.execute(sql: "DROP INDEX IF EXISTS idx_shots_session_shot_key_unique")

            try db.execute(sql: """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_shots_session_logical_identity_unique
                ON shots (session_id, logical_shot_identity)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_logical_identity
                ON shots (logical_shot_identity)
                """)
        }

        migrator.registerMigration("repairReplacedDatabaseSchema") { db in
            let sessionColumnNames = try Set(db.columns(in: "sessions").map(\.name))
            if sessionColumnNames.contains("imported_at") == false {
                try db.execute(sql: "ALTER TABLE sessions ADD COLUMN imported_at TEXT")
            }
            if sessionColumnNames.contains("zip_name") == false {
                try db.execute(sql: "ALTER TABLE sessions ADD COLUMN zip_name TEXT")
            }

            let shotColumnNames = try Set(db.columns(in: "shots").map(\.name))
            if shotColumnNames.contains("propertyStreet") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyStreet TEXT")
            }
            if shotColumnNames.contains("propertyCity") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyCity TEXT")
            }
            if shotColumnNames.contains("propertyState") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyState TEXT")
            }
            if shotColumnNames.contains("propertyZip") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyZip TEXT")
            }
            if shotColumnNames.contains("stamped_jpeg_filename") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN stamped_jpeg_filename TEXT")
            }
            if shotColumnNames.contains("flagged_reason") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN flagged_reason TEXT")
            }
            if shotColumnNames.contains("logical_shot_identity") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN logical_shot_identity TEXT")
            }

            try db.execute(sql: """
                UPDATE shots
                SET logical_shot_identity = COALESCE(NULLIF(TRIM(logical_shot_identity), ''), NULLIF(TRIM(shot_key), ''), shot_id)
                WHERE logical_shot_identity IS NULL OR TRIM(logical_shot_identity) = ''
                """)

            try db.execute(sql: """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_shots_session_logical_identity_unique
                ON shots (session_id, logical_shot_identity)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_shots_logical_identity
                ON shots (logical_shot_identity)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS zip_imports (
                    id INTEGER PRIMARY KEY,
                    zipFingerprint TEXT NOT NULL UNIQUE,
                    zipFilename TEXT NOT NULL,
                    importedAt DATETIME NOT NULL,
                    sessionID TEXT NOT NULL
                )
                """)
        }

        migrator.registerMigration("addShotAddressColumnsIfMissing") { db in
            let shotColumnNames = try Set(db.columns(in: "shots").map(\.name))
            if shotColumnNames.contains("propertyStreet") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyStreet TEXT")
            }
            if shotColumnNames.contains("propertyCity") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyCity TEXT")
            }
            if shotColumnNames.contains("propertyState") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyState TEXT")
            }
            if shotColumnNames.contains("propertyZip") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN propertyZip TEXT")
            }
        }

        migrator.registerMigration("addTradePriorityToCaptureTablesV1") { db in
            let shotColumnNames = try Set(db.columns(in: "shots").map(\.name))
            if shotColumnNames.contains("trade") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN trade TEXT")
            }
            if shotColumnNames.contains("priority") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN priority TEXT")
            }

            let guidedRowColumnNames = try Set(db.columns(in: "guided_rows").map(\.name))
            if guidedRowColumnNames.contains("trade") == false {
                try db.execute(sql: "ALTER TABLE guided_rows ADD COLUMN trade TEXT")
            }
            if guidedRowColumnNames.contains("priority") == false {
                try db.execute(sql: "ALTER TABLE guided_rows ADD COLUMN priority TEXT")
            }
        }

        migrator.registerMigration("addCaptureProfileToCaptureTablesV1") { db in
            let sessionColumnNames = try Set(db.columns(in: "sessions").map(\.name))
            if sessionColumnNames.contains("capture_profile") == false {
                try db.execute(sql: "ALTER TABLE sessions ADD COLUMN capture_profile TEXT")
            }

            let shotColumnNames = try Set(db.columns(in: "shots").map(\.name))
            if shotColumnNames.contains("capture_profile") == false {
                try db.execute(sql: "ALTER TABLE shots ADD COLUMN capture_profile TEXT")
            }
        }

        migrator.registerMigration("addPrimaryContactEmailToSessionsV1") { db in
            let sessionColumnNames = try Set(db.columns(in: "sessions").map(\.name))
            if sessionColumnNames.contains("primary_contact_email") == false {
                try db.execute(sql: "ALTER TABLE sessions ADD COLUMN primary_contact_email TEXT")
            }
        }

        migrator.registerMigration("addPunchListTablesV1") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS punch_list_items (
                    id INTEGER PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                    shot_id TEXT,
                    issue_id TEXT,
                    logical_shot_identity TEXT NOT NULL,
                    property_id TEXT,
                    property_name TEXT,
                    org_name TEXT,
                    building TEXT,
                    elevation TEXT,
                    detail_type TEXT,
                    angle_index INTEGER,
                    shot_key TEXT,
                    captured_at_utc TEXT,
                    flagged_reason TEXT,
                    stamped_jpeg_filename TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    priority TEXT NOT NULL DEFAULT 'medium',
                    assigned_to TEXT,
                    due_date TEXT,
                    resolution_note TEXT,
                    resolved_at_utc TEXT,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """)

            try db.execute(sql: """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_punch_list_items_session_identity
                ON punch_list_items (session_id, logical_shot_identity)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_punch_list_items_status
                ON punch_list_items (status)
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_punch_list_items_property
                ON punch_list_items (property_id, property_name)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS punch_list_evidence (
                    id INTEGER PRIMARY KEY,
                    punch_list_item_id INTEGER NOT NULL REFERENCES punch_list_items(id) ON DELETE CASCADE,
                    file_path TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    captured_at_utc TEXT,
                    uploaded_at_utc TEXT NOT NULL,
                    uploader TEXT,
                    file_hash TEXT
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_punch_list_evidence_item
                ON punch_list_evidence (punch_list_item_id)
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS punch_list_history (
                    id INTEGER PRIMARY KEY,
                    punch_list_item_id INTEGER NOT NULL REFERENCES punch_list_items(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    from_value TEXT,
                    to_value TEXT,
                    actor TEXT,
                    created_at_utc TEXT NOT NULL
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_punch_list_history_item_time
                ON punch_list_history (punch_list_item_id, created_at_utc)
                """)
        }

        migrator.registerMigration("addPunchListPriorityV1") { db in
            let columnNames = try Set(db.columns(in: "punch_list_items").map(\.name))
            if columnNames.contains("priority") == false {
                try db.execute(sql: "ALTER TABLE punch_list_items ADD COLUMN priority TEXT NOT NULL DEFAULT 'medium'")
            }
        }

        migrator.registerMigration("addPunchListTradeV1") { db in
            let columnNames = try Set(db.columns(in: "punch_list_items").map(\.name))
            if columnNames.contains("trade") == false {
                try db.execute(sql: "ALTER TABLE punch_list_items ADD COLUMN trade TEXT NOT NULL DEFAULT 'general'")
            }
        }

        do {
            try migrator.migrate(dbQueue)
        } catch {
            log("Database migration failed: \(error.localizedDescription)")
        }
    }

    private func log(_ message: String) {
        NSLog("[DatabaseManager] %@", message)
    }
}
