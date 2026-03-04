//
//  DatabaseRecords.swift
//  ScoutProcess
//

import Foundation
import GRDB

struct SessionRecord: Codable, FetchableRecord, PersistableRecord {
    var sessionID: String
    var propertyID: String
    var orgID: String?
    var orgName: String?
    var folderID: String?
    var propertyName: String?
    var propertyAddress: String?
    var propertyStreet: String?
    var propertyCity: String?
    var propertyState: String?
    var propertyZip: String?
    var primaryContactName: String?
    var primaryContactPhone: String?
    var startedAtUTC: String
    var endedAtUTC: String?
    var isBaseline: Int
    var status: String?
    var schemaVersion: String?
    var appVersion: String?
    var timeZone: String?
    var importedAt: String
    var zipName: String?

    static let databaseTableName = "sessions"

    enum CodingKeys: String, CodingKey {
        case sessionID = "session_id"
        case propertyID = "property_id"
        case orgID = "org_id"
        case orgName = "org_name"
        case folderID = "folder_id"
        case propertyName = "property_name"
        case propertyAddress = "property_address"
        case propertyStreet = "propertyStreet"
        case propertyCity = "propertyCity"
        case propertyState = "propertyState"
        case propertyZip = "propertyZip"
        case primaryContactName = "primary_contact_name"
        case primaryContactPhone = "primary_contact_phone"
        case startedAtUTC = "started_at_utc"
        case endedAtUTC = "ended_at_utc"
        case isBaseline = "is_baseline"
        case status
        case schemaVersion = "schema_version"
        case appVersion = "app_version"
        case timeZone = "time_zone"
        case importedAt = "imported_at"
        case zipName = "zip_name"
    }
}

struct ShotRecord: Codable, FetchableRecord, PersistableRecord {
    var shotID: String
    var sessionID: String
    var propertyID: String
    var propertyStreet: String?
    var propertyCity: String?
    var propertyState: String?
    var propertyZip: String?
    var building: String?
    var elevation: String?
    var detailType: String?
    var angleIndex: Int?
    var shotKey: String?
    var logicalShotIdentity: String?
    var captureKind: String?
    var isFlagged: Int
    var isGuided: Int
    var issueID: String?
    var capturedAtUTC: String?
    var latitude: Double?
    var longitude: Double?
    var lens: String?
    var originalFilename: String
    var originalByteSize: Int?
    var stampedJpegFilename: String?
    var flaggedReason: String?

    static let databaseTableName = "shots"

    enum CodingKeys: String, CodingKey {
        case shotID = "shot_id"
        case sessionID = "session_id"
        case propertyID = "property_id"
        case propertyStreet = "propertyStreet"
        case propertyCity = "propertyCity"
        case propertyState = "propertyState"
        case propertyZip = "propertyZip"
        case building
        case elevation
        case detailType = "detail_type"
        case angleIndex = "angle_index"
        case shotKey = "shot_key"
        case logicalShotIdentity = "logical_shot_identity"
        case captureKind = "capture_kind"
        case isFlagged = "is_flagged"
        case isGuided = "is_guided"
        case issueID = "issue_id"
        case capturedAtUTC = "captured_at_utc"
        case latitude
        case longitude
        case lens
        case originalFilename = "original_filename"
        case originalByteSize = "original_byte_size"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case flaggedReason = "flagged_reason"
    }
}

struct ZipImportRecord: Codable, FetchableRecord, PersistableRecord {
    var id: Int64?
    var zipFingerprint: String
    var zipFilename: String
    var importedAt: String
    var sessionID: String

    static let databaseTableName = "zip_imports"

    enum CodingKeys: String, CodingKey {
        case id
        case zipFingerprint
        case zipFilename
        case importedAt
        case sessionID
    }
}

struct IssueRecord: Codable, FetchableRecord, PersistableRecord {
    var issueID: String
    var propertyID: String
    var propertyStreet: String?
    var propertyCity: String?
    var propertyState: String?
    var propertyZip: String?
    var firstSeenSessionID: String?
    var lastCaptureSessionID: String?
    var currentStatus: String?
    var currentReason: String?
    var previousReason: String?
    var firstSeenAtUTC: String?
    var lastSeenAtUTC: String?
    var resolvedAtUTC: String?
    var shotKey: String?

    static let databaseTableName = "issues"

    enum CodingKeys: String, CodingKey {
        case issueID = "issue_id"
        case propertyID = "property_id"
        case propertyStreet = "propertyStreet"
        case propertyCity = "propertyCity"
        case propertyState = "propertyState"
        case propertyZip = "propertyZip"
        case firstSeenSessionID = "first_seen_session_id"
        case lastCaptureSessionID = "last_capture_session_id"
        case currentStatus = "current_status"
        case currentReason = "current_reason"
        case previousReason = "previous_reason"
        case firstSeenAtUTC = "first_seen_at_utc"
        case lastSeenAtUTC = "last_seen_at_utc"
        case resolvedAtUTC = "resolved_at_utc"
        case shotKey = "shot_key"
    }
}

struct IssueHistoryRecord: Codable, FetchableRecord, PersistableRecord {
    var eventID: String
    var issueID: String
    var sessionID: String
    var eventType: String
    var timestampUTC: String
    var fieldChanged: String?
    var oldValue: String?
    var newValue: String?
    var shotID: String?

    static let databaseTableName = "issue_history"

    enum CodingKeys: String, CodingKey {
        case eventID = "event_id"
        case issueID = "issue_id"
        case sessionID = "session_id"
        case eventType = "event_type"
        case timestampUTC = "timestamp_utc"
        case fieldChanged = "field_changed"
        case oldValue = "old_value"
        case newValue = "new_value"
        case shotID = "shot_id"
    }
}

struct GuidedRowRecord: Codable, FetchableRecord, PersistableRecord {
    var guidedRowID: String
    var sessionID: String
    var propertyID: String
    var propertyStreet: String?
    var propertyCity: String?
    var propertyState: String?
    var propertyZip: String?
    var building: String?
    var elevation: String?
    var detailType: String?
    var angleIndex: Int?
    var status: String?
    var isRetired: Int
    var retiredAt: String?
    var skipReason: String?
    var skipSessionID: String?

    static let databaseTableName = "guided_rows"

    enum CodingKeys: String, CodingKey {
        case guidedRowID = "guided_row_id"
        case sessionID = "session_id"
        case propertyID = "property_id"
        case propertyStreet = "propertyStreet"
        case propertyCity = "propertyCity"
        case propertyState = "propertyState"
        case propertyZip = "propertyZip"
        case building
        case elevation
        case detailType = "detail_type"
        case angleIndex = "angle_index"
        case status
        case isRetired = "is_retired"
        case retiredAt = "retired_at"
        case skipReason = "skip_reason"
        case skipSessionID = "skip_session_id"
    }
}
