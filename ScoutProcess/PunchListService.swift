//
//  PunchListService.swift
//  ScoutProcess
//

import Foundation
import GRDB

enum PunchListStatus: String, CaseIterable, Identifiable {
    case active
    case resolvedPendingVerification = "resolved_pending_verification"
    case resolved

    var id: String { rawValue }

    var label: String {
        switch self {
        case .active: return "Active"
        case .resolvedPendingVerification: return "Pending"
        case .resolved: return "Resolved"
        }
    }
}

enum PunchListPriority: String, CaseIterable, Identifiable {
    case low
    case medium
    case high
    case critical

    var id: String { rawValue }

    var label: String {
        switch self {
        case .low: return "Low"
        case .medium: return "Medium"
        case .high: return "High"
        case .critical: return "Critical"
        }
    }
}

enum PunchListTrade: String, CaseIterable, Identifiable {
    case general
    case concrete
    case masonry
    case steel
    case carpentry
    case roofing
    case drywall
    case painting
    case flooring
    case plumbing
    case hvac
    case electrical
    case fireProtection = "fire_protection"
    case sitework

    var id: String { rawValue }

    var label: String {
        switch self {
        case .general: return "General"
        case .concrete: return "Concrete"
        case .masonry: return "Masonry"
        case .steel: return "Steel"
        case .carpentry: return "Carpentry"
        case .roofing: return "Roofing"
        case .drywall: return "Drywall"
        case .painting: return "Painting"
        case .flooring: return "Flooring"
        case .plumbing: return "Plumbing"
        case .hvac: return "HVAC"
        case .electrical: return "Electrical"
        case .fireProtection: return "Fire Protection"
        case .sitework: return "Sitework"
        }
    }
}

struct PunchListItemSummary: FetchableRecord, Decodable, Identifiable {
    let id: Int64
    let sessionID: String
    let shotID: String?
    let issueID: String?
    let logicalShotIdentity: String
    let propertyID: String?
    let propertyName: String?
    let orgName: String?
    let building: String?
    let elevation: String?
    let detailType: String?
    let angleIndex: Int?
    let shotKey: String?
    let capturedAtUTC: String?
    let flaggedReason: String?
    let stampedJpegFilename: String?
    let status: String
    let priority: String
    let trade: String
    let assignedTo: String?
    let dueDate: String?
    let resolutionNote: String?
    let resolvedAtUTC: String?
    let relatedShotCount: Int

    enum CodingKeys: String, CodingKey {
        case id
        case sessionID = "session_id"
        case shotID = "shot_id"
        case issueID = "issue_id"
        case logicalShotIdentity = "logical_shot_identity"
        case propertyID = "property_id"
        case propertyName = "property_name"
        case orgName = "org_name"
        case building
        case elevation
        case detailType = "detail_type"
        case angleIndex = "angle_index"
        case shotKey = "shot_key"
        case capturedAtUTC = "captured_at_utc"
        case flaggedReason = "flagged_reason"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case status
        case priority
        case trade
        case assignedTo = "assigned_to"
        case dueDate = "due_date"
        case resolutionNote = "resolution_note"
        case resolvedAtUTC = "resolved_at_utc"
        case relatedShotCount = "related_shot_count"
    }
}

struct PunchListRelatedShot: FetchableRecord, Decodable, Identifiable {
    let shotID: String
    let sessionID: String
    let issueID: String?
    let propertyID: String?
    let shotKey: String?
    let logicalShotIdentity: String?
    let capturedAtUTC: String?
    let stampedJpegFilename: String?
    let isFlagged: Int
    let flaggedReason: String?
    let issueStatus: String?
    let issueResolvedAtUTC: String?

    var id: String { "\(sessionID)::\(shotID)" }

    enum CodingKeys: String, CodingKey {
        case shotID = "shot_id"
        case sessionID = "session_id"
        case issueID = "issue_id"
        case propertyID = "property_id"
        case shotKey = "shot_key"
        case logicalShotIdentity = "logical_shot_identity"
        case capturedAtUTC = "captured_at_utc"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case isFlagged = "is_flagged"
        case flaggedReason = "flagged_reason"
        case issueStatus = "issue_status"
        case issueResolvedAtUTC = "issue_resolved_at_utc"
    }
}

struct PunchListItemLocator: FetchableRecord, Decodable {
    let sessionID: String
    let issueID: String?
    let logicalShotIdentity: String
    let stampedJpegFilename: String?
    let propertyID: String?

    enum CodingKeys: String, CodingKey {
        case sessionID = "session_id"
        case issueID = "issue_id"
        case logicalShotIdentity = "logical_shot_identity"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case propertyID = "property_id"
    }
}

final class PunchListService {
    static let shared = PunchListService()

    private init() {}

    func syncFlaggedItems(forSessionID sessionID: String, db: Database) throws -> Int {
        let nowUTC = Self.iso8601Formatter.string(from: Date())
        try db.execute(
            sql: """
            WITH source AS (
                SELECT
                    s.session_id,
                    s.shot_id,
                    s.issue_id,
                    COALESCE(NULLIF(TRIM(s.logical_shot_identity), ''), NULLIF(TRIM(s.shot_key), ''), s.shot_id) AS logical_shot_identity_norm,
                    s.property_id,
                    se.property_name,
                    se.org_name,
                    s.building,
                    s.elevation,
                    s.detail_type,
                    s.angle_index,
                    s.shot_key,
                    s.captured_at_utc,
                    COALESCE(NULLIF(TRIM(s.flagged_reason), ''), NULLIF(TRIM(i.current_reason), '')) AS flagged_reason,
                    s.stamped_jpeg_filename,
                    CASE
                        WHEN i.resolved_at_utc IS NOT NULL AND TRIM(i.resolved_at_utc) <> '' THEN 'resolved'
                        WHEN LOWER(TRIM(COALESCE(i.current_status, ''))) = 'resolved' THEN 'resolved'
                        ELSE 'active'
                    END AS derived_status
                FROM shots s
                LEFT JOIN sessions se ON se.session_id = s.session_id
                LEFT JOIN issues i ON i.issue_id = s.issue_id
                WHERE s.session_id = ?
                  AND s.is_flagged = 1
            ),
            seeded AS (
                SELECT
                    src.*,
                    (
                        SELECT prev.id
                        FROM punch_list_items prev
                        WHERE
                            (
                                NULLIF(TRIM(src.issue_id), '') IS NOT NULL
                                AND NULLIF(TRIM(prev.issue_id), '') = NULLIF(TRIM(src.issue_id), '')
                            )
                            OR (
                                COALESCE(prev.property_id, '') = COALESCE(src.property_id, '')
                                AND LOWER(TRIM(COALESCE(prev.building, ''))) = LOWER(TRIM(COALESCE(src.building, '')))
                                AND LOWER(TRIM(COALESCE(prev.elevation, ''))) = LOWER(TRIM(COALESCE(src.elevation, '')))
                                AND LOWER(TRIM(COALESCE(prev.detail_type, ''))) = LOWER(TRIM(COALESCE(src.detail_type, '')))
                                AND COALESCE(prev.angle_index, -1) = COALESCE(src.angle_index, -1)
                            )
                        ORDER BY COALESCE(prev.updated_at_utc, '') DESC, COALESCE(prev.captured_at_utc, '') DESC, prev.id DESC
                        LIMIT 1
                    ) AS prior_item_id
                FROM source src
            )
            INSERT INTO punch_list_items (
                session_id,
                shot_id,
                issue_id,
                logical_shot_identity,
                property_id,
                property_name,
                org_name,
                building,
                elevation,
                detail_type,
                angle_index,
                shot_key,
                captured_at_utc,
                flagged_reason,
                stamped_jpeg_filename,
                status,
                priority,
                trade,
                assigned_to,
                due_date,
                resolution_note,
                resolved_at_utc,
                created_at_utc,
                updated_at_utc
            )
            SELECT
                seeded.session_id,
                seeded.shot_id,
                seeded.issue_id,
                seeded.logical_shot_identity_norm,
                seeded.property_id,
                seeded.property_name,
                seeded.org_name,
                seeded.building,
                seeded.elevation,
                seeded.detail_type,
                seeded.angle_index,
                seeded.shot_key,
                seeded.captured_at_utc,
                seeded.flagged_reason,
                seeded.stamped_jpeg_filename,
                CASE
                    WHEN seeded.derived_status = 'resolved' THEN 'resolved'
                    ELSE COALESCE(NULLIF(TRIM(prior.status), ''), seeded.derived_status, 'active')
                END,
                COALESCE(NULLIF(TRIM(prior.priority), ''), 'medium'),
                COALESCE(NULLIF(TRIM(prior.trade), ''), 'general'),
                prior.assigned_to,
                prior.due_date,
                prior.resolution_note,
                CASE
                    WHEN seeded.derived_status = 'resolved' THEN COALESCE(prior.resolved_at_utc, ?)
                    ELSE prior.resolved_at_utc
                END,
                ?,
                ?
            FROM seeded
            LEFT JOIN punch_list_items prior ON prior.id = seeded.prior_item_id
            ON CONFLICT(session_id, logical_shot_identity) DO UPDATE SET
                shot_id = excluded.shot_id,
                issue_id = COALESCE(excluded.issue_id, punch_list_items.issue_id),
                property_id = excluded.property_id,
                property_name = excluded.property_name,
                org_name = excluded.org_name,
                building = excluded.building,
                elevation = excluded.elevation,
                detail_type = excluded.detail_type,
                angle_index = excluded.angle_index,
                shot_key = excluded.shot_key,
                captured_at_utc = excluded.captured_at_utc,
                flagged_reason = COALESCE(excluded.flagged_reason, punch_list_items.flagged_reason),
                stamped_jpeg_filename = COALESCE(excluded.stamped_jpeg_filename, punch_list_items.stamped_jpeg_filename),
                status = CASE
                    WHEN excluded.status = 'resolved' THEN 'resolved'
                    WHEN punch_list_items.status = 'resolved' THEN 'resolved'
                    ELSE COALESCE(NULLIF(TRIM(punch_list_items.status), ''), excluded.status, 'active')
                END,
                priority = COALESCE(NULLIF(TRIM(punch_list_items.priority), ''), excluded.priority, 'medium'),
                trade = COALESCE(NULLIF(TRIM(punch_list_items.trade), ''), excluded.trade, 'general'),
                assigned_to = COALESCE(NULLIF(TRIM(punch_list_items.assigned_to), ''), excluded.assigned_to),
                due_date = COALESCE(NULLIF(TRIM(punch_list_items.due_date), ''), excluded.due_date),
                resolution_note = COALESCE(NULLIF(TRIM(punch_list_items.resolution_note), ''), excluded.resolution_note),
                resolved_at_utc = CASE
                    WHEN excluded.status = 'resolved' THEN COALESCE(punch_list_items.resolved_at_utc, excluded.updated_at_utc)
                    ELSE punch_list_items.resolved_at_utc
                END,
                updated_at_utc = excluded.updated_at_utc
            """,
            arguments: [sessionID, nowUTC, nowUTC, nowUTC]
        )

        try db.execute(
            sql: """
            UPDATE punch_list_items
            SET
                status = 'resolved',
                resolved_at_utc = COALESCE(resolved_at_utc, ?),
                updated_at_utc = ?
            WHERE session_id = ?
              AND issue_id IN (
                  SELECT issue_id
                  FROM issues
                  WHERE issue_id IS NOT NULL
                    AND (
                        (resolved_at_utc IS NOT NULL AND TRIM(resolved_at_utc) <> '')
                        OR LOWER(TRIM(COALESCE(current_status, ''))) = 'resolved'
                    )
              )
            """,
            arguments: [nowUTC, nowUTC, sessionID]
        )

        return try Int.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM punch_list_items WHERE session_id = ?",
            arguments: [sessionID]
        ) ?? 0
    }

    func fetchItems(
        status: PunchListStatus?,
        propertyFilter: String?
    ) throws -> [PunchListItemSummary] {
        guard let dbQueue = DatabaseManager.shared.dbQueue else { return [] }

        return try dbQueue.read { db in
            var conditions: [String] = []
            var arguments: StatementArguments = []

            if let status {
                conditions.append("status = ?")
                arguments += [status.rawValue]
            }

            if let propertyFilter, propertyFilter.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
                conditions.append("LOWER(COALESCE(property_name, '')) LIKE ?")
                arguments += ["%" + propertyFilter.lowercased() + "%"]
            }

            let whereClause = conditions.isEmpty ? "" : "WHERE " + conditions.joined(separator: " AND ")
            return try PunchListItemSummary.fetchAll(
                db,
                sql: """
                WITH grouped AS (
                    SELECT
                        pli.*,
                        COALESCE(pli.property_id, '') || '|' ||
                        LOWER(TRIM(COALESCE(pli.building, ''))) || '|' ||
                        LOWER(TRIM(COALESCE(pli.elevation, ''))) || '|' ||
                        LOWER(TRIM(COALESCE(pli.detail_type, ''))) || '|' ||
                        COALESCE(CAST(pli.angle_index AS TEXT), '') AS event_key,
                        COUNT(*) OVER (
                            PARTITION BY
                                COALESCE(pli.property_id, ''),
                                LOWER(TRIM(COALESCE(pli.building, ''))),
                                LOWER(TRIM(COALESCE(pli.elevation, ''))),
                                LOWER(TRIM(COALESCE(pli.detail_type, ''))),
                                COALESCE(pli.angle_index, -1)
                        ) AS related_shot_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                COALESCE(pli.property_id, ''),
                                LOWER(TRIM(COALESCE(pli.building, ''))),
                                LOWER(TRIM(COALESCE(pli.elevation, ''))),
                                LOWER(TRIM(COALESCE(pli.detail_type, ''))),
                                COALESCE(pli.angle_index, -1)
                            ORDER BY COALESCE(pli.captured_at_utc, '') DESC, pli.updated_at_utc DESC, pli.id DESC
                        ) AS row_rank
                    FROM punch_list_items pli
                    \(whereClause)
                )
                SELECT
                    id,
                    session_id,
                    shot_id,
                    issue_id,
                    logical_shot_identity,
                    property_id,
                    property_name,
                    org_name,
                    building,
                    elevation,
                    detail_type,
                    angle_index,
                    shot_key,
                    captured_at_utc,
                    flagged_reason,
                    stamped_jpeg_filename,
                    status,
                    priority,
                    COALESCE(NULLIF(TRIM(trade), ''), 'general') AS trade,
                    assigned_to,
                    due_date,
                    resolution_note,
                    resolved_at_utc,
                    related_shot_count
                FROM grouped
                WHERE row_rank = 1
                ORDER BY
                    CASE status
                        WHEN 'active' THEN 0
                        WHEN 'resolved_pending_verification' THEN 1
                        ELSE 2
                    END,
                    COALESCE(property_name, ''),
                    COALESCE(building, ''),
                    COALESCE(elevation, ''),
                    COALESCE(detail_type, ''),
                    COALESCE(angle_index, 0),
                    COALESCE(captured_at_utc, '') DESC
                """,
                arguments: arguments
            )
        }
    }

    func fetchRelatedShots(for item: PunchListItemSummary) throws -> [PunchListRelatedShot] {
        guard let dbQueue = DatabaseManager.shared.dbQueue else { return [] }

        return try dbQueue.read { db in
            let whereClause = """
            COALESCE(s.property_id, '') = ?
            AND LOWER(TRIM(COALESCE(s.building, ''))) = LOWER(TRIM(?))
            AND LOWER(TRIM(COALESCE(s.elevation, ''))) = LOWER(TRIM(?))
            AND LOWER(TRIM(COALESCE(s.detail_type, ''))) = LOWER(TRIM(?))
            AND COALESCE(s.angle_index, -1) = COALESCE(?, -1)
            """
            let arguments: StatementArguments = [
                item.propertyID ?? "",
                item.building ?? "",
                item.elevation ?? "",
                item.detailType ?? "",
                item.angleIndex
            ]
            return try PunchListRelatedShot.fetchAll(
                db,
                sql: """
                SELECT
                    s.shot_id,
                    s.session_id,
                    s.issue_id,
                    s.property_id,
                    s.shot_key,
                    s.logical_shot_identity,
                    s.captured_at_utc,
                    s.stamped_jpeg_filename,
                    s.is_flagged,
                    s.flagged_reason,
                    i.current_status AS issue_status,
                    i.resolved_at_utc AS issue_resolved_at_utc
                FROM shots s
                LEFT JOIN issues i ON i.issue_id = s.issue_id
                LEFT JOIN sessions se ON se.session_id = s.session_id
                WHERE \(whereClause)
                ORDER BY
                    COALESCE(se.imported_at, '') DESC,
                    COALESCE(s.captured_at_utc, '') DESC,
                    s.session_id DESC
                """,
                arguments: arguments
            )
        }
    }

    func updateItem(
        id: Int64,
        status: PunchListStatus,
        priority: PunchListPriority,
        trade: String,
        assignedTo: String?,
        dueDate: String?,
        resolutionNote: String?
    ) throws {
        guard let dbQueue = DatabaseManager.shared.dbQueue else { return }
        let nowUTC = Self.iso8601Formatter.string(from: Date())
        let resolvedAt = status == .resolved ? nowUTC : nil

        try dbQueue.write { db in
            let priorStatus = try String.fetchOne(
                db,
                sql: "SELECT status FROM punch_list_items WHERE id = ?",
                arguments: [id]
            )

            try db.execute(
                sql: """
                UPDATE punch_list_items
                SET
                    status = ?,
                    priority = ?,
                    trade = ?,
                    assigned_to = NULLIF(TRIM(?), ''),
                    due_date = NULLIF(TRIM(?), ''),
                    resolution_note = NULLIF(TRIM(?), ''),
                    resolved_at_utc = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                arguments: [
                    status.rawValue,
                    priority.rawValue,
                    trade.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? PunchListTrade.general.rawValue : trade,
                    assignedTo,
                    dueDate,
                    resolutionNote,
                    resolvedAt,
                    nowUTC,
                    id,
                ]
            )

            if priorStatus != status.rawValue {
                try db.execute(
                    sql: """
                    INSERT INTO punch_list_history (
                        punch_list_item_id,
                        action,
                        from_value,
                        to_value,
                        actor,
                        created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    arguments: [id, "status_change", priorStatus, status.rawValue, "ScoutProcess", nowUTC]
                )
            }
        }
    }

    func attachResolutionEvidence(
        itemID: Int64,
        sourceImageURL: URL,
        uploader: String?
    ) throws -> URL {
        guard let dbQueue = DatabaseManager.shared.dbQueue else {
            throw PunchListError.databaseUnavailable
        }

        let nowUTC = Self.iso8601Formatter.string(from: Date())
        let destinationURL = try dbQueue.write { db -> URL in
            guard let locator = try PunchListItemLocator.fetchOne(
                db,
                sql: """
                SELECT session_id, issue_id, logical_shot_identity, stamped_jpeg_filename, property_id
                FROM punch_list_items
                WHERE id = ?
                """,
                arguments: [itemID]
            ) else {
                throw PunchListError.itemNotFound
            }

            let targetURL = try Self.prepareResolutionDestination(
                for: locator,
                sourceURL: sourceImageURL
            )

            try db.execute(
                sql: """
                INSERT INTO punch_list_evidence (
                    punch_list_item_id,
                    file_path,
                    source_type,
                    captured_at_utc,
                    uploaded_at_utc,
                    uploader
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                arguments: [itemID, targetURL.path, "manual_resolution_upload", nowUTC, nowUTC, uploader]
            )

            try db.execute(
                sql: """
                UPDATE punch_list_items
                SET
                    status = 'resolved',
                    resolved_at_utc = COALESCE(resolved_at_utc, ?),
                    updated_at_utc = ?
                WHERE id = ?
                """,
                arguments: [nowUTC, nowUTC, itemID]
            )

            try db.execute(
                sql: """
                INSERT INTO punch_list_history (
                    punch_list_item_id,
                    action,
                    from_value,
                    to_value,
                    actor,
                    created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                arguments: [itemID, "resolution_upload", nil, targetURL.path, uploader, nowUTC]
            )
            return targetURL
        }

        return destinationURL
    }

    func bulkRenameTrade(from oldValue: String, to newValue: String) throws {
        guard let dbQueue = DatabaseManager.shared.dbQueue else { return }
        let oldTrimmed = oldValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let newTrimmed = newValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard oldTrimmed.isEmpty == false, newTrimmed.isEmpty == false else { return }
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE punch_list_items
                SET
                    trade = ?,
                    updated_at_utc = ?
                WHERE LOWER(TRIM(COALESCE(trade, ''))) = LOWER(TRIM(?))
                """,
                arguments: [newTrimmed, Self.iso8601Formatter.string(from: Date()), oldTrimmed]
            )
        }
    }

    func bulkClearTrade(_ value: String) throws {
        guard let dbQueue = DatabaseManager.shared.dbQueue else { return }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty == false else { return }
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE punch_list_items
                SET
                    trade = ?,
                    updated_at_utc = ?
                WHERE LOWER(TRIM(COALESCE(trade, ''))) = LOWER(TRIM(?))
                """,
                arguments: [PunchListTrade.general.rawValue, Self.iso8601Formatter.string(from: Date()), trimmed]
            )
        }
    }

    func bulkRenameAssignee(from oldValue: String, to newValue: String?) throws {
        guard let dbQueue = DatabaseManager.shared.dbQueue else { return }
        let oldTrimmed = oldValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard oldTrimmed.isEmpty == false else { return }
        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE punch_list_items
                SET
                    assigned_to = NULLIF(TRIM(?), ''),
                    updated_at_utc = ?
                WHERE LOWER(TRIM(COALESCE(assigned_to, ''))) = LOWER(TRIM(?))
                """,
                arguments: [newValue, Self.iso8601Formatter.string(from: Date()), oldTrimmed]
            )
        }
    }

    func clearIssueAndPunchListDataForDevelopment() throws {
        guard let dbQueue = DatabaseManager.shared.dbQueue else {
            throw PunchListError.databaseUnavailable
        }

        try dbQueue.write { db in
            try db.execute(sql: "DELETE FROM issue_history")
            try db.execute(sql: "DELETE FROM issues")
            try db.execute(sql: "DELETE FROM punch_list_history")
            try db.execute(sql: "DELETE FROM punch_list_evidence")
            try db.execute(sql: "DELETE FROM punch_list_items")
            try db.execute(
                sql: """
                UPDATE shots
                SET
                    issue_id = NULL,
                    is_flagged = 0,
                    flagged_reason = NULL,
                    stamped_jpeg_filename = NULL
                """
            )
        }
    }

    func resolveArchivedImageURL(
        filename: String,
        preferredSessionID: String? = nil,
        preferredPropertyID: String? = nil,
        preferredLogicalShotIdentity: String? = nil,
        preferredShotKey: String? = nil,
        preferredCapturedAtUTC: String? = nil
    ) -> URL? {
        let normalizedName = filename.trimmingCharacters(in: .whitespacesAndNewlines)
        guard normalizedName.isEmpty == false else { return nil }

        let clientsRoot = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Documents", isDirectory: true)
            .appendingPathComponent("ScoutArchive", isDirectory: true)
            .appendingPathComponent("Clients", isDirectory: true)

        guard let enumerator = FileManager.default.enumerator(
            at: clientsRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return nil
        }

        var matchedCandidates: [URL] = []
        for case let candidate as URL in enumerator {
            guard candidate.lastPathComponent.caseInsensitiveCompare(normalizedName) == .orderedSame else { continue }
            guard (try? candidate.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true else { continue }
            matchedCandidates.append(candidate)
        }

        guard matchedCandidates.isEmpty == false else { return nil }

        let sessionToken = preferredSessionID?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        let propertyToken = preferredPropertyID?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        let identityTokens = Self.identityTokens(from: preferredLogicalShotIdentity)
        let shotKeyTokens = Self.identityTokens(from: preferredShotKey)
        let preferredCaptureDate = Self.iso8601Formatter.date(from: preferredCapturedAtUTC ?? "")
            ?? Self.iso8601Fallback.date(from: preferredCapturedAtUTC ?? "")

        let scored = matchedCandidates.map { candidate -> (url: URL, score: Int, modified: Date) in
            let path = candidate.path(percentEncoded: false).lowercased()
            var score = 0
            if sessionToken.isEmpty == false && path.contains(sessionToken) {
                score += 100
            }
            if propertyToken.isEmpty == false && path.contains(propertyToken) {
                score += 40
            }

            var identityScore = 0
            for token in identityTokens where path.contains(token) {
                identityScore += 8
            }
            score += min(identityScore, 48)

            var shotKeyScore = 0
            for token in shotKeyTokens where path.contains(token) {
                shotKeyScore += 5
            }
            score += min(shotKeyScore, 30)

            let modified = (try? candidate.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate) ?? .distantPast
            return (candidate, score, modified)
        }

        let bestScore = scored.map(\.score).max() ?? 0
        if bestScore > 0 {
            let top = scored.filter { $0.score == bestScore }
            if let preferredCaptureDate {
                return top
                    .sorted { abs($0.modified.timeIntervalSince(preferredCaptureDate)) < abs($1.modified.timeIntervalSince(preferredCaptureDate)) }
                    .first?
                    .url
            }
            return top.sorted { $0.modified > $1.modified }.first?.url
        }

        if let preferredCaptureDate {
            return scored
                .sorted { abs($0.modified.timeIntervalSince(preferredCaptureDate)) < abs($1.modified.timeIntervalSince(preferredCaptureDate)) }
                .first?
                .url
        }

        return scored
            .sorted { $0.modified > $1.modified }
            .first?
            .url
    }

    private static func identityTokens(from raw: String?) -> [String] {
        guard let raw else { return [] }
        let normalized = raw
            .lowercased()
            .replacingOccurrences(of: #"[^a-z0-9]+"#, with: " ", options: .regularExpression)
        return Array(
            Set(
                normalized
                    .split(separator: " ")
                    .map(String.init)
                    .filter { $0.count >= 2 && ignoredIdentityTokens.contains($0) == false }
            )
        )
    }

    private static let ignoredIdentityTokens: Set<String> = [
        "a", "an", "the", "and", "or", "for", "to", "of",
        "flagged", "photo", "shot", "image", "img",
        "jpg", "jpeg", "png", "heic"
    ]

    private static func prepareResolutionDestination(
        for locator: PunchListItemLocator,
        sourceURL: URL
    ) throws -> URL {
        let fileManager = FileManager.default

        let destinationDirectory: URL
        if let stampedFilename = locator.stampedJpegFilename,
           let stampedURL = PunchListService.shared.resolveArchivedImageURL(filename: stampedFilename) {
            destinationDirectory = stampedURL
                .deletingLastPathComponent()
                .appendingPathComponent("Resolved", isDirectory: true)
        } else {
            destinationDirectory = fileManager.homeDirectoryForCurrentUser
                .appendingPathComponent("Documents", isDirectory: true)
                .appendingPathComponent("ScoutArchive", isDirectory: true)
                .appendingPathComponent("PunchListResolutions", isDirectory: true)
                .appendingPathComponent(locator.sessionID, isDirectory: true)
        }

        try fileManager.createDirectory(at: destinationDirectory, withIntermediateDirectories: true)

        let keySeed = locator.issueID?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
            ? locator.issueID!
            : locator.logicalShotIdentity
        let normalizedSeed = sanitizePathComponent(keySeed)
        let timestamp = filenameTimestampFormatter.string(from: Date())
        let ext = sourceURL.pathExtension.isEmpty ? "jpg" : sourceURL.pathExtension.lowercased()
        var targetURL = destinationDirectory.appendingPathComponent("\(normalizedSeed)_resolved_\(timestamp).\(ext)")
        if fileManager.fileExists(atPath: targetURL.path) {
            targetURL = destinationDirectory.appendingPathComponent("\(normalizedSeed)_resolved_\(timestamp)_\(UUID().uuidString.prefix(8)).\(ext)")
        }

        if fileManager.fileExists(atPath: targetURL.path) {
            try fileManager.removeItem(at: targetURL)
        }
        try fileManager.copyItem(at: sourceURL, to: targetURL)
        return targetURL
    }

    private static func sanitizePathComponent(_ raw: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty == false else { return "resolution" }
        let cleaned = trimmed.replacingOccurrences(
            of: #"[^A-Za-z0-9._-]+"#,
            with: "_",
            options: .regularExpression
        )
        return cleaned.isEmpty ? "resolution" : cleaned
    }

    private static let filenameTimestampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        return formatter
    }()

    private static let iso8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static let iso8601Fallback: ISO8601DateFormatter = {
        ISO8601DateFormatter()
    }()
}

enum PunchListError: Error {
    case databaseUnavailable
    case itemNotFound
}
