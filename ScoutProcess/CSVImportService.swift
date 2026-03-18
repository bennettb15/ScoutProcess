//
//  CSVImportService.swift
//  ScoutProcess
//

import Foundation
import GRDB

struct CSVImportSessionResult {
    let sessionID: String?
    let duplicateSessionID: String?
    let qualitySummary: CSVImportQualitySummary?
}

struct CSVImportQualitySummary {
    let rowsSessions: Int
    let rowsShots: Int
    let rowsGuidedRows: Int
    let rowsIssues: Int
    let rowsIssueHistory: Int
    let issueHistoryParsedRows: Int
    let issueHistorySkippedMalformedRows: Int
    let issueHistorySkippedOrphanRows: Int
}

final class CSVImportService {
    static let shared = CSVImportService()

    private let fileManager: FileManager

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
    }

    func importSessionFolder(at folderURL: URL) throws -> CSVImportSessionResult {
        try importSessionFolder(at: folderURL, zipName: nil, zipFingerprint: nil)
    }

    func importSessionFolder(at folderURL: URL, zipName: String?, zipFingerprint: String?) throws -> CSVImportSessionResult {
        guard let dbQueue = DatabaseManager.shared.dbQueue else {
            throw CSVImportError.databaseUnavailable
        }

        let importRunID = UUID().uuidString
        let startedAt = Self.iso8601Formatter.string(from: Date())
        let folderPath = folderURL.path

        var importedSessionID: String?
        var duplicateSessionID: String?
        var importQualitySummary: CSVImportQualitySummary?

        try dbQueue.inDatabase { db in
            try db.inTransaction {
                try insertImportRun(
                    db: db,
                    importRunID: importRunID,
                    startedAt: startedAt,
                    folderPath: folderPath
                )

                log("ImportRun \(importRunID) started folder=\(folderPath)")

                let result: ImportResult
                do {
                    if let zipFingerprint,
                       let existingSessionID = try String.fetchOne(
                        db,
                        sql: "SELECT sessionID FROM zip_imports WHERE zipFingerprint = ?",
                        arguments: [zipFingerprint]
                       ) {
                        let duplicateResult = ImportResult(
                            status: .skipped,
                            sessionID: existingSessionID,
                            zipName: zipName ?? detectSourceZipName(for: folderURL),
                            errorMessage: "Duplicate ZIP detected. Already imported as session \(existingSessionID).",
                            rowCounts: .zero,
                            qualitySummary: nil
                        )
                        try finalizeImportRun(
                            db: db,
                            importRunID: importRunID,
                            sessionID: duplicateResult.sessionID,
                            zipName: duplicateResult.zipName,
                            status: "duplicate",
                            errorMessage: duplicateResult.errorMessage,
                            rowCounts: duplicateResult.rowCounts
                        )
                        log("ImportRun \(importRunID) finished status=duplicate session_id=\(existingSessionID)")
                        importedSessionID = nil
                        duplicateSessionID = existingSessionID
                        importQualitySummary = nil
                        return .commit
                    }

                    result = try executeImport(
                        db: db,
                        importRunID: importRunID,
                        folderURL: folderURL,
                        zipName: zipName
                    )
                } catch {
                    let failedResult = ImportResult(
                        status: .failed,
                        sessionID: nil,
                        zipName: zipName ?? detectSourceZipName(for: folderURL),
                        errorMessage: error.localizedDescription,
                        rowCounts: .zero,
                        qualitySummary: nil
                    )

                    try finalizeImportRun(
                        db: db,
                        importRunID: importRunID,
                        sessionID: failedResult.sessionID,
                        zipName: failedResult.zipName,
                        status: failedResult.status.rawValue,
                        errorMessage: failedResult.errorMessage,
                        rowCounts: failedResult.rowCounts
                    )

                    log("ImportRun \(importRunID) finished status=failed session_id=nil")
                    importedSessionID = nil
                    return .commit
                }

                try finalizeImportRun(
                    db: db,
                    importRunID: importRunID,
                    sessionID: result.sessionID,
                    zipName: result.zipName,
                    status: result.status.rawValue,
                    errorMessage: result.errorMessage,
                    rowCounts: result.rowCounts
                )

                if let zipFingerprint, let sessionID = result.sessionID, let zipName = result.zipName {
                    try db.execute(
                        sql: """
                        INSERT INTO zip_imports (
                            zipFingerprint,
                            zipFilename,
                            importedAt,
                            sessionID
                        ) VALUES (?, ?, ?, ?)
                        """,
                        arguments: [
                            zipFingerprint,
                            zipName,
                            Self.iso8601Formatter.string(from: Date()),
                            sessionID,
                        ]
                    )
                }

                log("ImportRun \(importRunID) finished status=\(result.status.rawValue) session_id=\(result.sessionID ?? "nil")")
                importedSessionID = result.sessionID
                duplicateSessionID = nil
                importQualitySummary = result.qualitySummary
                return .commit
            }
        }

        return CSVImportSessionResult(
            sessionID: importedSessionID,
            duplicateSessionID: duplicateSessionID,
            qualitySummary: importQualitySummary
        )
    }

    private func executeImport(
        db: Database,
        importRunID: String,
        folderURL: URL,
        zipName: String?
    ) throws -> ImportResult {
        let csvFiles = findCSVFiles(in: folderURL)
        let detectedZipName = zipName ?? detectSourceZipName(for: folderURL)

        guard let sessionsCSV = csvFiles["sessions"] else {
            return ImportResult(
                status: .skipped,
                sessionID: nil,
                zipName: detectedZipName,
                errorMessage: "sessions.csv not found.",
                rowCounts: .zero,
                qualitySummary: nil
            )
        }

        let sessionsFile = try parseCSVFile(at: sessionsCSV)

        let firstUsableSessionRow = sessionsFile.rows.first { row in
            value(for: "session_id", in: row, aliases: Self.sessionAliases) != nil
                && value(for: "property_id", in: row, aliases: Self.sessionAliases) != nil
        }

        guard let sessionID = firstUsableSessionRow.flatMap({
            value(for: "session_id", in: $0, aliases: Self.sessionAliases)
        }) else {
            return ImportResult(
                status: .skipped,
                sessionID: nil,
                zipName: detectedZipName,
                errorMessage: "sessions.csv has no usable session_id in any data row.",
                rowCounts: .zero,
                qualitySummary: nil
            )
        }

        guard let sessionPropertyID = firstUsableSessionRow.flatMap({
            value(for: "property_id", in: $0, aliases: Self.sessionAliases)
        }) else {
            return ImportResult(
                status: .skipped,
                sessionID: sessionID,
                zipName: detectedZipName,
                errorMessage: "sessions.csv has no usable property_id in any data row.",
                rowCounts: .zero,
                qualitySummary: nil
            )
        }

        let distinctSessionIDs = Set(sessionsFile.rows.compactMap {
            value(for: "session_id", in: $0, aliases: Self.sessionAliases)
        })
        if distinctSessionIDs.count > 1 {
            throw CSVImportError.multipleSessionIDs(distinctSessionIDs.sorted())
        }

        let guidedRowsFile = try parseOptionalCSV(named: "guided_rows", files: csvFiles)
        let issuesFile = try parseOptionalCSV(named: "issues", files: csvFiles)
        let shotsFile = try parseOptionalCSV(named: "shots", files: csvFiles)
        let issueHistoryFile = try parseOptionalCSV(named: "issue_history", files: csvFiles)

        let importedAtNow = Self.iso8601Formatter.string(from: Date())
        var rowCounts = ImportRowCounts.zero

        try db.inSavepoint {
            try clearSessionScopedRowsForReimport(db: db, sessionID: sessionID)

            rowCounts.rowsSessions = try upsertSessions(
                db: db,
                file: sessionsFile,
                importRunID: importRunID,
                zipName: detectedZipName,
                importedAtNow: importedAtNow
            )
            rowCounts.rowsGuidedRows = try upsertGuidedRows(
                db: db,
                file: guidedRowsFile,
                importRunID: importRunID,
                sessionID: sessionID
            )
            rowCounts.rowsIssues = try upsertIssues(
                db: db,
                file: issuesFile,
                importRunID: importRunID
            )
            rowCounts.rowsShots = try upsertShots(
                db: db,
                file: shotsFile,
                importRunID: importRunID,
                sessionID: sessionID,
                defaultPropertyID: sessionPropertyID
            )
            let issueHistoryStats = try upsertIssueHistory(
                db: db,
                file: issueHistoryFile,
                importRunID: importRunID,
                sessionID: sessionID
            )
            rowCounts.rowsIssueHistory = issueHistoryStats.upsertedRows
            rowCounts.issueHistoryParsedRows = issueHistoryStats.parsedRows
            rowCounts.issueHistorySkippedMalformedRows = issueHistoryStats.skippedMalformedRows
            rowCounts.issueHistorySkippedOrphanRows = issueHistoryStats.skippedOrphanRows
            return .commit
        }

        return ImportResult(
            status: .success,
            sessionID: sessionID,
            zipName: detectedZipName,
            errorMessage: nil,
            rowCounts: rowCounts,
            qualitySummary: CSVImportQualitySummary(
                rowsSessions: rowCounts.rowsSessions,
                rowsShots: rowCounts.rowsShots,
                rowsGuidedRows: rowCounts.rowsGuidedRows,
                rowsIssues: rowCounts.rowsIssues,
                rowsIssueHistory: rowCounts.rowsIssueHistory,
                issueHistoryParsedRows: rowCounts.issueHistoryParsedRows,
                issueHistorySkippedMalformedRows: rowCounts.issueHistorySkippedMalformedRows,
                issueHistorySkippedOrphanRows: rowCounts.issueHistorySkippedOrphanRows
            )
        )
    }

    private func clearSessionScopedRowsForReimport(db: Database, sessionID: String) throws {
        // Replace existing session payload instead of leaving stale rows behind on re-import.
        try db.execute(
            sql: """
            WITH session_issue_ids AS (
                SELECT DISTINCT issue_id
                FROM shots
                WHERE session_id = ?
                  AND issue_id IS NOT NULL
                  AND TRIM(issue_id) <> ''
            )
            DELETE FROM issues
            WHERE issue_id IN (SELECT issue_id FROM session_issue_ids)
              AND issue_id NOT IN (
                  SELECT DISTINCT issue_id
                  FROM shots
                  WHERE issue_id IS NOT NULL
                    AND TRIM(issue_id) <> ''
                    AND session_id <> ?
              )
            """,
            arguments: [sessionID, sessionID]
        )

        try db.execute(
            sql: "DELETE FROM issue_history WHERE session_id = ?",
            arguments: [sessionID]
        )
        try db.execute(
            sql: "DELETE FROM guided_rows WHERE session_id = ?",
            arguments: [sessionID]
        )
        try db.execute(
            sql: "DELETE FROM shots WHERE session_id = ?",
            arguments: [sessionID]
        )
        try db.execute(
            sql: "DELETE FROM punch_list_items WHERE session_id = ?",
            arguments: [sessionID]
        )
    }

    private func upsertSessions(
        db: Database,
        file: CSVFileResult,
        importRunID: String,
        zipName: String?,
        importedAtNow: String
    ) throws -> Int {
        var upsertedCount = 0

        for row in file.rows {
            guard let sessionID = value(for: "session_id", in: row, aliases: Self.sessionAliases),
                  let propertyID = value(for: "property_id", in: row, aliases: Self.sessionAliases),
                  let startedAtUTC = value(for: "started_at_utc", in: row, aliases: Self.sessionAliases) else {
                continue
            }

            try db.execute(
                sql: """
                INSERT INTO sessions (
                    session_id,
                    property_id,
                    org_id,
                    org_name,
                    folder_id,
                    property_name,
                    property_address,
                    propertyStreet,
                    propertyCity,
                    propertyState,
                    propertyZip,
                    primary_contact_name,
                    primary_contact_phone,
                    started_at_utc,
                    ended_at_utc,
                    is_baseline,
                    status,
                    schema_version,
                    app_version,
                    time_zone,
                    capture_profile,
                    imported_at,
                    zip_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    property_id = excluded.property_id,
                    org_id = excluded.org_id,
                    org_name = excluded.org_name,
                    folder_id = excluded.folder_id,
                    property_name = excluded.property_name,
                    property_address = excluded.property_address,
                    propertyStreet = excluded.propertyStreet,
                    propertyCity = excluded.propertyCity,
                    propertyState = excluded.propertyState,
                    propertyZip = excluded.propertyZip,
                    primary_contact_name = excluded.primary_contact_name,
                    primary_contact_phone = excluded.primary_contact_phone,
                    started_at_utc = excluded.started_at_utc,
                    ended_at_utc = excluded.ended_at_utc,
                    is_baseline = excluded.is_baseline,
                    status = excluded.status,
                    schema_version = excluded.schema_version,
                    app_version = excluded.app_version,
                    time_zone = excluded.time_zone,
                    capture_profile = excluded.capture_profile,
                    imported_at = excluded.imported_at,
                    zip_name = excluded.zip_name
                """,
                arguments: [
                    sessionID,
                    propertyID,
                    optionalValue(for: "org_id", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "org_name", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "folder_id", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "property_name", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "property_address", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "propertyStreet", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "propertyCity", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "propertyState", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "propertyZip", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "primary_contact_name", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "primary_contact_phone", in: row, aliases: Self.sessionAliases),
                    startedAtUTC,
                    optionalValue(for: "ended_at_utc", in: row, aliases: Self.sessionAliases),
                    boolIntegerValue(for: "is_baseline", in: row, aliases: Self.sessionAliases, defaultValue: 0),
                    optionalValue(for: "status", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "schema_version", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "app_version", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "time_zone", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "capture_profile", in: row, aliases: Self.sessionAliases),
                    optionalValue(for: "imported_at", in: row, aliases: Self.sessionAliases) ?? importedAtNow,
                    zipName,
                ]
            )
            upsertedCount += 1
        }

        log("ImportRun \(importRunID) sessions parsed=\(file.stats.parsedRows) skippedMalformed=\(file.stats.skippedMalformedRows) upserted=\(upsertedCount)")
        return upsertedCount
    }

    private func upsertGuidedRows(
        db: Database,
        file: CSVFileResult,
        importRunID: String,
        sessionID: String
    ) throws -> Int {
        var upsertedCount = 0

        for row in file.rows {
            guard let guidedRowID = value(for: "guided_row_id", in: row, aliases: Self.guidedRowAliases),
                  let propertyID = value(for: "property_id", in: row, aliases: Self.guidedRowAliases),
                  let detailType = optionalValue(for: "detail_type", in: row, aliases: Self.guidedRowAliases)
                    ?? optionalValue(for: "guided_key", in: row, aliases: Self.guidedRowAliases) else {
                continue
            }

            let rowSessionID = value(for: "session_id", in: row, aliases: Self.guidedRowAliases) ?? sessionID

            try db.execute(
                sql: """
                INSERT INTO guided_rows (
                    guided_row_id,
                    session_id,
                    property_id,
                    propertyStreet,
                    propertyCity,
                    propertyState,
                    propertyZip,
                    building,
                    elevation,
                    detail_type,
                    angle_index,
                    status,
                    is_retired,
                    retired_at,
                    skip_reason,
                    skip_session_id,
                    trade,
                    priority
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guided_row_id) DO UPDATE SET
                    session_id = excluded.session_id,
                    property_id = excluded.property_id,
                    propertyStreet = excluded.propertyStreet,
                    propertyCity = excluded.propertyCity,
                    propertyState = excluded.propertyState,
                    propertyZip = excluded.propertyZip,
                    building = excluded.building,
                    elevation = excluded.elevation,
                    detail_type = excluded.detail_type,
                    angle_index = excluded.angle_index,
                    status = excluded.status,
                    is_retired = excluded.is_retired,
                    retired_at = excluded.retired_at,
                    skip_reason = excluded.skip_reason,
                    skip_session_id = excluded.skip_session_id,
                    trade = excluded.trade,
                    priority = excluded.priority
                """,
                arguments: [
                    guidedRowID,
                    rowSessionID,
                    propertyID,
                    optionalValue(for: "propertyStreet", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "propertyCity", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "propertyState", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "propertyZip", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "building", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "elevation", in: row, aliases: Self.guidedRowAliases),
                    detailType,
                    integerValue(for: "angle_index", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "status", in: row, aliases: Self.guidedRowAliases),
                    boolIntegerValue(for: "is_retired", in: row, aliases: Self.guidedRowAliases, defaultValue: 0),
                    optionalValue(for: "retired_at", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "skip_reason", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "skip_session_id", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "trade", in: row, aliases: Self.guidedRowAliases),
                    optionalValue(for: "priority", in: row, aliases: Self.guidedRowAliases),
                ]
            )
            upsertedCount += 1
        }

        log("ImportRun \(importRunID) guided_rows parsed=\(file.stats.parsedRows) skippedMalformed=\(file.stats.skippedMalformedRows) upserted=\(upsertedCount)")
        return upsertedCount
    }

    private func upsertIssues(
        db: Database,
        file: CSVFileResult,
        importRunID: String
    ) throws -> Int {
        var upsertedCount = 0

        for row in file.rows {
            guard let issueID = value(for: "issue_id", in: row, aliases: Self.issueAliases),
                  let propertyID = value(for: "property_id", in: row, aliases: Self.issueAliases) else {
                continue
            }

            try db.execute(
                sql: """
                INSERT INTO issues (
                    issue_id,
                    property_id,
                    propertyStreet,
                    propertyCity,
                    propertyState,
                    propertyZip,
                    first_seen_session_id,
                    last_capture_session_id,
                    current_status,
                    current_reason,
                    previous_reason,
                    first_seen_at_utc,
                    last_seen_at_utc,
                    resolved_at_utc,
                    shot_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(issue_id) DO UPDATE SET
                    property_id = excluded.property_id,
                    propertyStreet = excluded.propertyStreet,
                    propertyCity = excluded.propertyCity,
                    propertyState = excluded.propertyState,
                    propertyZip = excluded.propertyZip,
                    first_seen_session_id = excluded.first_seen_session_id,
                    last_capture_session_id = excluded.last_capture_session_id,
                    current_status = excluded.current_status,
                    current_reason = excluded.current_reason,
                    previous_reason = excluded.previous_reason,
                    first_seen_at_utc = excluded.first_seen_at_utc,
                    last_seen_at_utc = excluded.last_seen_at_utc,
                    resolved_at_utc = excluded.resolved_at_utc,
                    shot_key = excluded.shot_key
                """,
                arguments: [
                    issueID,
                    propertyID,
                    optionalValue(for: "propertyStreet", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "propertyCity", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "propertyState", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "propertyZip", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "first_seen_session_id", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "last_capture_session_id", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "current_status", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "current_reason", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "previous_reason", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "first_seen_at_utc", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "last_seen_at_utc", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "resolved_at_utc", in: row, aliases: Self.issueAliases),
                    optionalValue(for: "shot_key", in: row, aliases: Self.issueAliases),
                ]
            )
            upsertedCount += 1
        }

        log("ImportRun \(importRunID) issues parsed=\(file.stats.parsedRows) skippedMalformed=\(file.stats.skippedMalformedRows) upserted=\(upsertedCount)")
        return upsertedCount
    }

    private func upsertShots(
        db: Database,
        file: CSVFileResult,
        importRunID: String,
        sessionID: String,
        defaultPropertyID: String
    ) throws -> Int {
        var upsertedCount = 0
        var skippedShotIDConflictCount = 0

        for row in file.rows {
            guard let shotID = value(for: "shot_id", in: row, aliases: Self.shotAliases),
                  let fileName = value(for: "original_filename", in: row, aliases: Self.shotAliases) else {
                continue
            }

            let rowSessionID = value(for: "session_id", in: row, aliases: Self.shotAliases) ?? sessionID
            let propertyID = value(for: "property_id", in: row, aliases: Self.shotAliases) ?? defaultPropertyID
            let shotKey = value(for: "shot_key", in: row, aliases: Self.shotAliases) ?? shotID
            let logicalShotIdentity = value(for: "logical_shot_identity", in: row, aliases: Self.shotAliases) ?? shotKey
            if rowSessionID != sessionID {
                throw CSVImportError.mismatchedShotSessionID(shotID: shotID, sessionID: rowSessionID, expectedSessionID: sessionID)
            }

            do {
                try db.execute(
                    sql: """
                    INSERT INTO shots (
                        shot_id,
                        session_id,
                        property_id,
                        propertyStreet,
                        propertyCity,
                        propertyState,
                        propertyZip,
                        building,
                        elevation,
                        detail_type,
                        angle_index,
                        shot_key,
                        logical_shot_identity,
                        capture_kind,
                        is_flagged,
                        is_guided,
                        issue_id,
                        captured_at_utc,
                        latitude,
                        longitude,
                        lens,
                        original_filename,
                        original_byte_size,
                        capture_profile,
                        stamped_jpeg_filename,
                        flagged_reason,
                        trade,
                        priority
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, logical_shot_identity) DO UPDATE SET
                        shot_id = excluded.shot_id,
                        session_id = excluded.session_id,
                        property_id = excluded.property_id,
                        propertyStreet = excluded.propertyStreet,
                        propertyCity = excluded.propertyCity,
                        propertyState = excluded.propertyState,
                        propertyZip = excluded.propertyZip,
                        building = excluded.building,
                        elevation = excluded.elevation,
                        detail_type = excluded.detail_type,
                        angle_index = excluded.angle_index,
                        shot_key = excluded.shot_key,
                        logical_shot_identity = excluded.logical_shot_identity,
                        capture_kind = excluded.capture_kind,
                        is_flagged = excluded.is_flagged,
                        is_guided = excluded.is_guided,
                        issue_id = excluded.issue_id,
                        captured_at_utc = excluded.captured_at_utc,
                        latitude = excluded.latitude,
                        longitude = excluded.longitude,
                        lens = excluded.lens,
                        original_filename = excluded.original_filename,
                        original_byte_size = excluded.original_byte_size,
                        capture_profile = excluded.capture_profile,
                        stamped_jpeg_filename = excluded.stamped_jpeg_filename,
                        flagged_reason = excluded.flagged_reason,
                        trade = excluded.trade,
                        priority = excluded.priority
                    """,
                    arguments: [
                        shotID,
                        rowSessionID,
                        propertyID,
                        optionalValue(for: "propertyStreet", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "propertyCity", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "propertyState", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "propertyZip", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "building", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "elevation", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "detail_type", in: row, aliases: Self.shotAliases),
                        integerValue(for: "angle_index", in: row, aliases: Self.shotAliases),
                        shotKey,
                        logicalShotIdentity,
                        optionalValue(for: "capture_kind", in: row, aliases: Self.shotAliases),
                        boolIntegerValue(for: "is_flagged", in: row, aliases: Self.shotAliases, defaultValue: 0),
                        boolIntegerValue(for: "is_guided", in: row, aliases: Self.shotAliases, defaultValue: 0),
                        optionalValue(for: "issue_id", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "captured_at_utc", in: row, aliases: Self.shotAliases),
                        doubleValue(for: "latitude", in: row, aliases: Self.shotAliases),
                        doubleValue(for: "longitude", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "lens", in: row, aliases: Self.shotAliases),
                        fileName,
                        integerValue(for: "original_byte_size", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "capture_profile", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "stamped_jpeg_filename", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "flagged_reason", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "trade", in: row, aliases: Self.shotAliases),
                        optionalValue(for: "priority", in: row, aliases: Self.shotAliases),
                    ]
                )
                upsertedCount += 1
            } catch let error as DatabaseError
                where error.resultCode == .SQLITE_CONSTRAINT
                && (error.message?.contains("shots.shot_id") == true || error.description.contains("shots.shot_id")) {
                skippedShotIDConflictCount += 1
                log("ImportRun \(importRunID) shots skip shot_id=\(shotID) reason=duplicate_shot_id_across_sessions")
                continue
            }
        }

        log("ImportRun \(importRunID) shots parsed=\(file.stats.parsedRows) skippedMalformed=\(file.stats.skippedMalformedRows) skippedShotIDConflict=\(skippedShotIDConflictCount) upserted=\(upsertedCount)")
        return upsertedCount
    }

    private func upsertIssueHistory(
        db: Database,
        file: CSVFileResult,
        importRunID: String,
        sessionID: String
    ) throws -> IssueHistoryImportStats {
        var upsertedCount = 0
        var skippedOrphanCount = 0
        let knownIssueIDs = Set(try String.fetchAll(db, sql: "SELECT issue_id FROM issues"))
        let knownSessionIDs = Set(try String.fetchAll(db, sql: "SELECT session_id FROM sessions"))
        let knownShotIDs = Set(try String.fetchAll(db, sql: "SELECT shot_id FROM shots"))

        for row in file.rows {
            guard let issueHistoryID = value(for: "event_id", in: row, aliases: Self.issueHistoryAliases),
                  let issueID = value(for: "issue_id", in: row, aliases: Self.issueHistoryAliases),
                  let eventType = value(for: "event_type", in: row, aliases: Self.issueHistoryAliases),
                  let eventAt = value(for: "timestamp_utc", in: row, aliases: Self.issueHistoryAliases) else {
                continue
            }

            let rowSessionID = value(for: "session_id", in: row, aliases: Self.issueHistoryAliases) ?? sessionID
            guard knownIssueIDs.contains(issueID) else {
                skippedOrphanCount += 1
                log("ImportRun \(importRunID) issue_history skip event_id=\(issueHistoryID) reason=missing_issue issue_id=\(issueID)")
                continue
            }

            guard knownSessionIDs.contains(rowSessionID) else {
                skippedOrphanCount += 1
                log("ImportRun \(importRunID) issue_history skip event_id=\(issueHistoryID) reason=missing_session session_id=\(rowSessionID)")
                continue
            }

            let rawShotID = optionalValue(for: "shot_id", in: row, aliases: Self.issueHistoryAliases)
            let resolvedShotID: String?
            if let rawShotID, rawShotID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
                let trimmedShotID = rawShotID.trimmingCharacters(in: .whitespacesAndNewlines)
                if knownShotIDs.contains(trimmedShotID) {
                    resolvedShotID = trimmedShotID
                } else {
                    resolvedShotID = nil
                    log("ImportRun \(importRunID) issue_history event_id=\(issueHistoryID) warning=missing_shot shot_id=\(trimmedShotID) storing_null")
                }
            } else {
                resolvedShotID = nil
            }

            try db.execute(
                sql: """
                INSERT INTO issue_history (
                    event_id,
                    issue_id,
                    session_id,
                    event_type,
                    timestamp_utc,
                    field_changed,
                    old_value,
                    new_value,
                    shot_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    issue_id = excluded.issue_id,
                    session_id = excluded.session_id,
                    event_type = excluded.event_type,
                    timestamp_utc = excluded.timestamp_utc,
                    field_changed = excluded.field_changed,
                    old_value = excluded.old_value,
                    new_value = excluded.new_value,
                    shot_id = excluded.shot_id
                """,
                arguments: [
                    issueHistoryID,
                    issueID,
                    rowSessionID,
                    eventType,
                    eventAt,
                    optionalValue(for: "field_changed", in: row, aliases: Self.issueHistoryAliases),
                    optionalValue(for: "old_value", in: row, aliases: Self.issueHistoryAliases),
                    optionalValue(for: "new_value", in: row, aliases: Self.issueHistoryAliases),
                    resolvedShotID,
                ]
            )
            upsertedCount += 1
        }

        log("ImportRun \(importRunID) issue_history parsed=\(file.stats.parsedRows) skippedMalformed=\(file.stats.skippedMalformedRows) skippedOrphan=\(skippedOrphanCount) upserted=\(upsertedCount)")
        return IssueHistoryImportStats(
            parsedRows: file.stats.parsedRows,
            skippedMalformedRows: file.stats.skippedMalformedRows,
            skippedOrphanRows: skippedOrphanCount,
            upsertedRows: upsertedCount
        )
    }

    private func insertImportRun(
        db: Database,
        importRunID: String,
        startedAt: String,
        folderPath: String
    ) throws {
        try db.execute(
            sql: """
            INSERT INTO import_runs (
                import_run_id,
                started_at,
                folder_path,
                status
            ) VALUES (?, ?, ?, ?)
            """,
            arguments: [importRunID, startedAt, folderPath, "started"]
        )
    }

    private func finalizeImportRun(
        db: Database,
        importRunID: String,
        sessionID: String?,
        zipName: String?,
        status: String,
        errorMessage: String?,
        rowCounts: ImportRowCounts
    ) throws {
        try db.execute(
            sql: """
            UPDATE import_runs
            SET finished_at = ?,
                session_id = ?,
                zip_name = ?,
                status = ?,
                error_message = ?,
                rows_sessions = ?,
                rows_shots = ?,
                rows_guided_rows = ?,
                rows_issues = ?,
                rows_issue_history = ?
            WHERE import_run_id = ?
            """,
            arguments: [
                Self.iso8601Formatter.string(from: Date()),
                sessionID,
                zipName,
                status,
                errorMessage,
                rowCounts.rowsSessions,
                rowCounts.rowsShots,
                rowCounts.rowsGuidedRows,
                rowCounts.rowsIssues,
                rowCounts.rowsIssueHistory,
                importRunID,
            ]
        )
    }

    private func parseOptionalCSV(named name: String, files: [String: URL]) throws -> CSVFileResult {
        guard let url = files[name] else {
            return CSVFileResult(rows: [], stats: FileStats(parsedRows: 0, skippedMalformedRows: 0))
        }

        return try parseCSVFile(at: url)
    }

    private func parseCSVFile(at url: URL) throws -> CSVFileResult {
        let content = try String(contentsOf: url, encoding: .utf8)
        let parsed = parseCSV(content, sourceName: url.lastPathComponent)
        guard let header = parsed.rows.first else {
            return CSVFileResult(rows: [], stats: FileStats(parsedRows: 0, skippedMalformedRows: parsed.skippedMalformedRows))
        }

        let normalizedHeader = header.map { Self.normalizeHeader($0 ?? "") }
        var rows: [[String: String?]] = []
        var skippedMalformedRows = parsed.skippedMalformedRows

        for fields in parsed.rows.dropFirst() {
            guard fields.count <= normalizedHeader.count else {
                skippedMalformedRows += 1
                continue
            }

            var row: [String: String?] = [:]
            for (index, key) in normalizedHeader.enumerated() {
                let rawValue = index < fields.count ? fields[index] : nil
                row[key] = sanitized(rawValue)
            }
            rows.append(row)
        }

        return CSVFileResult(
            rows: rows,
            stats: FileStats(parsedRows: rows.count, skippedMalformedRows: skippedMalformedRows)
        )
    }

    private func parseCSV(_ content: String, sourceName: String) -> ParsedCSV {
        var rows: [[String?]] = []
        var row: [String?] = []
        var field = ""
        var isQuoted = false
        var sawQuoteInField = false
        var malformedRow = false
        var justFinishedField = false
        var skippedMalformedRows = 0
        var index = content.startIndex

        func finishField() {
            row.append(field)
            field = ""
            sawQuoteInField = false
            justFinishedField = true
        }

        func finishRow() {
            if malformedRow {
                skippedMalformedRows += 1
            } else if !(row.count == 1 && (row[0] ?? "").isEmpty && rows.isEmpty) {
                rows.append(row)
            }
            row = []
            field = ""
            isQuoted = false
            sawQuoteInField = false
            malformedRow = false
            justFinishedField = false
        }

        while index < content.endIndex {
            let character = content[index]

            if isQuoted {
                if character == "\"" {
                    let nextIndex = content.index(after: index)
                    if nextIndex < content.endIndex, content[nextIndex] == "\"" {
                        field.append("\"")
                        index = nextIndex
                        justFinishedField = false
                    } else {
                        isQuoted = false
                        sawQuoteInField = true
                    }
                } else {
                    field.append(character)
                    justFinishedField = false
                }
            } else {
                switch character {
                case "\"":
                    if field.isEmpty {
                        isQuoted = true
                    } else {
                        malformedRow = true
                        field.append(character)
                        justFinishedField = false
                    }
                case ",":
                    finishField()
                case "\n":
                    finishField()
                    finishRow()
                case "\r":
                    break
                default:
                    if sawQuoteInField, !character.isWhitespace {
                        malformedRow = true
                    }
                    field.append(character)
                    justFinishedField = false
                }
            }

            index = content.index(after: index)
        }

        if isQuoted {
            malformedRow = true
        }

        if !justFinishedField, (!field.isEmpty || !row.isEmpty) {
            finishField()
        }

        if !row.isEmpty {
            finishRow()
        }

        return ParsedCSV(rows: rows, skippedMalformedRows: skippedMalformedRows)
    }

    private func findCSVFiles(in folderURL: URL) -> [String: URL] {
        guard let enumerator = fileManager.enumerator(
            at: folderURL,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return [:]
        }

        var files: [String: URL] = [:]
        for case let url as URL in enumerator {
            guard url.pathExtension.lowercased() == "csv" else { continue }
            let fileName = url.deletingPathExtension().lastPathComponent
            let normalized = Self.normalizeHeader(fileName).replacingOccurrences(of: "_", with: "")

            switch normalized {
            case "session", "sessions":
                files["sessions"] = files["sessions"] ?? url
            case "shot", "shots":
                files["shots"] = files["shots"] ?? url
            case "guidedrows", "guidedrow":
                files["guided_rows"] = files["guided_rows"] ?? url
            case "issues", "issue":
                files["issues"] = files["issues"] ?? url
            case "issuehistory":
                files["issue_history"] = files["issue_history"] ?? url
            default:
                break
            }
        }

        return files
    }

    private func detectSourceZipName(for folderURL: URL) -> String? {
        let parentURL = folderURL.deletingLastPathComponent()
        let sessionBaseName = folderURL.lastPathComponent
            .replacingOccurrences(of: "_\\d{8}_\\d{6}$", with: "", options: .regularExpression)

        guard let siblings = try? fileManager.contentsOfDirectory(
            at: parentURL,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return nil
        }

        let zipSiblings = siblings.filter { $0.pathExtension.lowercased() == "zip" }
        if let match = zipSiblings.first(where: { $0.deletingPathExtension().lastPathComponent == sessionBaseName }) {
            return match.lastPathComponent
        }

        if zipSiblings.count == 1 {
            return zipSiblings[0].lastPathComponent
        }

        return nil
    }

    private func value(for key: String, in row: [String: String?], aliases: [String: [String]]) -> String? {
        for alias in aliases[key] ?? [key] {
            if let value = sanitized(row[Self.normalizeHeader(alias)] ?? nil) {
                return value
            }
        }
        return nil
    }

    private func optionalValue(for key: String, in row: [String: String?], aliases: [String: [String]]) -> String? {
        value(for: key, in: row, aliases: aliases)
    }

    private func integerValue(for key: String, in row: [String: String?], aliases: [String: [String]]) -> Int? {
        guard let value = optionalValue(for: key, in: row, aliases: aliases) else { return nil }
        return Int(value)
    }

    private func doubleValue(for key: String, in row: [String: String?], aliases: [String: [String]]) -> Double? {
        guard let value = optionalValue(for: key, in: row, aliases: aliases) else { return nil }
        return Double(value)
    }

    private func boolIntegerValue(for key: String, in row: [String: String?], aliases: [String: [String]], defaultValue: Int) -> Int {
        guard let value = optionalValue(for: key, in: row, aliases: aliases)?.lowercased() else { return defaultValue }
        switch value {
        case "1", "true", "yes", "y":
            return 1
        case "0", "false", "no", "n":
            return 0
        default:
            return Int(value) ?? defaultValue
        }
    }

    private func sanitized(_ value: String??) -> String? {
        guard let value, let unwrapped = value?.trimmingCharacters(in: .whitespacesAndNewlines), !unwrapped.isEmpty else {
            return nil
        }
        return unwrapped
    }

    private func log(_ message: String) {
        NSLog("[CSVImportService] %@", message)
    }

    private static func normalizeHeader(_ value: String) -> String {
        value
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .replacingOccurrences(of: "[^a-z0-9]+", with: "_", options: .regularExpression)
            .trimmingCharacters(in: CharacterSet(charactersIn: "_"))
    }

    private static let iso8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()

    private static let sessionAliases: [String: [String]] = [
        "session_id": ["session_id", "sessionid", "session_id_text"],
        "property_id": ["property_id", "propertyid", "folder_id", "folderid"],
        "org_id": ["org_id", "orgid"],
        "org_name": ["org_name", "orgname"],
        "folder_id": ["folder_id", "folderid"],
        "property_name": ["property_name", "propertyname"],
        "property_address": ["property_address", "propertyaddress"],
        "propertyStreet": ["propertyStreet", "property_street"],
        "propertyCity": ["propertyCity", "property_city"],
        "propertyState": ["propertyState", "property_state"],
        "propertyZip": ["propertyZip", "property_zip"],
        "primary_contact_name": ["primary_contact_name", "primarycontactname"],
        "primary_contact_phone": ["primary_contact_phone", "primarycontactphone"],
        "started_at_utc": ["started_at_utc", "startedatutc", "created_at", "createdat", "started_at"],
        "ended_at_utc": ["ended_at_utc", "endedatutc"],
        "is_baseline": ["is_baseline", "isbaseline"],
        "status": ["status"],
        "schema_version": ["schema_version", "schemaversion"],
        "app_version": ["app_version", "appversion"],
        "time_zone": ["time_zone", "timezone"],
        "capture_profile": ["capture_profile", "captureprofile"],
        "imported_at": ["imported_at", "importedat"],
    ]

    private static let shotAliases: [String: [String]] = [
        "shot_id": ["shot_id", "shotid", "id"],
        "session_id": ["session_id", "sessionid"],
        "property_id": ["property_id", "propertyid", "folder_id", "folderid"],
        "propertyStreet": ["propertyStreet", "property_street"],
        "propertyCity": ["propertyCity", "property_city"],
        "propertyState": ["propertyState", "property_state"],
        "propertyZip": ["propertyZip", "property_zip"],
        "building": ["building"],
        "elevation": ["elevation"],
        "detail_type": ["detail_type", "detailtype", "guided_key", "guidedkey"],
        "angle_index": ["angle_index", "angleindex"],
        "shot_key": ["shot_key", "shotkey"],
        "logical_shot_identity": ["logical_shot_identity", "logicalshotidentity"],
        "capture_kind": ["capture_kind", "capturekind"],
        "is_flagged": ["is_flagged", "isflagged", "flagged"],
        "is_guided": ["is_guided", "isguided"],
        "issue_id": ["issue_id", "issueid"],
        "captured_at_utc": ["captured_at_utc", "capturedatutc", "captured_at", "capturedat", "captured_at_local", "capturedatlocal"],
        "latitude": ["latitude", "lat"],
        "longitude": ["longitude", "lng", "lon"],
        "lens": ["lens"],
        "original_filename": ["original_filename", "originalfilename", "file_name", "filename", "file"],
        "original_byte_size": ["original_byte_size", "originalbytesize"],
        "capture_profile": ["capture_profile", "captureprofile"],
        "stamped_jpeg_filename": ["stamped_jpeg_filename", "stampedjpegfilename"],
        "flagged_reason": ["flagged_reason", "flaggedreason", "current_reason", "currentreason", "reason"],
        "trade": ["trade"],
        "priority": ["priority"],
    ]

    private static let guidedRowAliases: [String: [String]] = [
        "guided_row_id": ["guided_row_id", "guidedrowid", "id"],
        "session_id": ["session_id", "sessionid"],
        "property_id": ["property_id", "propertyid", "folder_id", "folderid"],
        "propertyStreet": ["propertyStreet", "property_street"],
        "propertyCity": ["propertyCity", "property_city"],
        "propertyState": ["propertyState", "property_state"],
        "propertyZip": ["propertyZip", "property_zip"],
        "building": ["building"],
        "elevation": ["elevation"],
        "detail_type": ["detail_type", "detailtype"],
        "angle_index": ["angle_index", "angleindex"],
        "status": ["status"],
        "is_retired": ["is_retired", "isretired"],
        "retired_at": ["retired_at", "retiredat"],
        "skip_reason": ["skip_reason", "skipreason"],
        "skip_session_id": ["skip_session_id", "skipsessionid"],
        "trade": ["trade"],
        "priority": ["priority"],
        "guided_key": ["guided_key", "guidedkey", "key"],
    ]

    private static let issueAliases: [String: [String]] = [
        "issue_id": ["issue_id", "issueid", "id"],
        "property_id": ["property_id", "propertyid", "folder_id", "folderid"],
        "propertyStreet": ["propertyStreet", "property_street"],
        "propertyCity": ["propertyCity", "property_city"],
        "propertyState": ["propertyState", "property_state"],
        "propertyZip": ["propertyZip", "property_zip"],
        "first_seen_session_id": ["first_seen_session_id", "firstseensessionid"],
        "last_capture_session_id": ["last_capture_session_id", "lastcapturesessionid", "last_updated_session_id", "lastupdatedsessionid"],
        "current_status": ["current_status", "currentstatus", "status", "issue_status"],
        "current_reason": ["current_reason", "currentreason", "title", "name", "reason"],
        "previous_reason": ["previous_reason", "previousreason"],
        "first_seen_at_utc": ["first_seen_at_utc", "firstseenatutc", "first_seen_at", "firstseenat", "created_at"],
        "last_seen_at_utc": ["last_seen_at_utc", "lastseenatutc", "last_seen_at", "lastseenat", "updated_at"],
        "resolved_at_utc": ["resolved_at_utc", "resolvedatutc"],
        "shot_key": ["shot_key", "shotkey"],
    ]

    private static let issueHistoryAliases: [String: [String]] = [
        "event_id": ["event_id", "eventid", "issue_history_id", "issuehistoryid", "id"],
        "issue_id": ["issue_id", "issueid"],
        "session_id": ["session_id", "sessionid"],
        "event_type": ["event_type", "eventtype", "type"],
        "timestamp_utc": ["timestamp_utc", "timestamputc", "event_at", "eventat", "created_at", "timestamp"],
        "field_changed": ["field_changed", "fieldchanged"],
        "old_value": ["old_value", "oldvalue"],
        "new_value": ["new_value", "newvalue"],
        "shot_id": ["shot_id", "shotid"],
    ]
}

private struct CSVFileResult {
    let rows: [[String: String?]]
    let stats: FileStats
}

private struct ParsedCSV {
    let rows: [[String?]]
    let skippedMalformedRows: Int
}

private struct FileStats {
    let parsedRows: Int
    let skippedMalformedRows: Int
}

private struct ImportRowCounts {
    var rowsSessions: Int
    var rowsShots: Int
    var rowsGuidedRows: Int
    var rowsIssues: Int
    var rowsIssueHistory: Int
    var issueHistoryParsedRows: Int
    var issueHistorySkippedMalformedRows: Int
    var issueHistorySkippedOrphanRows: Int

    static let zero = ImportRowCounts(
        rowsSessions: 0,
        rowsShots: 0,
        rowsGuidedRows: 0,
        rowsIssues: 0,
        rowsIssueHistory: 0,
        issueHistoryParsedRows: 0,
        issueHistorySkippedMalformedRows: 0,
        issueHistorySkippedOrphanRows: 0
    )
}

private struct ImportResult {
    let status: ImportRunStatus
    let sessionID: String?
    let zipName: String?
    let errorMessage: String?
    let rowCounts: ImportRowCounts
    let qualitySummary: CSVImportQualitySummary?
}

private struct IssueHistoryImportStats {
    let parsedRows: Int
    let skippedMalformedRows: Int
    let skippedOrphanRows: Int
    let upsertedRows: Int
}

private enum ImportRunStatus: String {
    case success
    case skipped
    case failed
}

enum CSVImportError: LocalizedError {
    case databaseUnavailable
    case multipleSessionIDs([String])
    case mismatchedShotSessionID(shotID: String, sessionID: String, expectedSessionID: String)

    var errorDescription: String? {
        switch self {
        case .databaseUnavailable:
            return "Database queue is unavailable."
        case .multipleSessionIDs(let sessionIDs):
            return "sessions.csv contains multiple distinct session_id values: \(sessionIDs.joined(separator: ", "))"
        case .mismatchedShotSessionID(let shotID, let sessionID, let expectedSessionID):
            return "shots.csv row \(shotID) has session_id \(sessionID), expected \(expectedSessionID)"
        }
    }
}
