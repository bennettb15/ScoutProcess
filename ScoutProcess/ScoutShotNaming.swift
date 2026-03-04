//
//  ScoutShotNaming.swift
//  ScoutProcess
//

import Foundation

enum ScoutShotNaming {
    struct StampLineFields {
        let elevation: String
        let angle: String
        let shotName: String
        let detailID: String
        let dateTime: String
    }

    static func sanitizeComponent(_ value: String?) -> String {
        let rawValue = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let replaced = rawValue.unicodeScalars.map { scalar -> Character in
            if CharacterSet.controlCharacters.contains(scalar)
                || scalar == "/"
                || scalar == "\\"
                || scalar == ":"
                || scalar == "*"
                || scalar == "?"
                || scalar == "\""
                || scalar == "<"
                || scalar == ">"
                || scalar == "|"
            {
                return "_"
            }
            return scalar == " " ? "_" : Character(scalar)
        }

        let collapsed = String(replaced).replacingOccurrences(of: "_+", with: "_", options: .regularExpression)
        let trimmed = collapsed.trimmingCharacters(in: CharacterSet(charactersIn: "_"))
        return trimmed.isEmpty ? "Item" : trimmed
    }

    static func shotKeyOrName(detailType: String?, angleIndex: Int?, shotKey: String?, shotName: String?) -> String {
        if let shotKey, shotKey.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return sanitizeComponent(shotKey)
        }

        if let shotName, shotName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return sanitizeComponent(shotName)
        }

        let detail = sanitizeComponent(detailType)
        let angle = "A\(angleIndex ?? 0)"
        return sanitizeComponent("\(detail)_\(angle)")
    }

    static func stampedJPEGFilename(
        building: String?,
        elevation: String?,
        detailType: String?,
        angleIndex: Int?,
        shotKey: String?,
        shotName: String?,
        isFlagged: Bool
    ) -> String {
        let resolvedShotKey = normalizedDetailID(shotKey: shotKey, angleIndex: angleIndex)
        let resolvedShotName = displayText(
            shotName?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false ? shotName : detailType,
            fallback: "Shot"
        )
        let parts = [
            sanitizeComponent(building),
            sanitizeComponent(elevation),
            sanitizeComponent(resolvedShotName),
            sanitizeComponent(resolvedShotKey),
        ]
        let baseName = parts.joined(separator: "_")
        let flaggedSuffix = isFlagged ? "_Flagged" : ""
        return "\(baseName)\(flaggedSuffix).jpg"
    }

    static func stampFields(
        elevationCode: String?,
        angle: String?,
        shotName: String?,
        shotKey: String?,
        angleIndex: Int?,
        capturedAt: String?,
        formatter: (String?) -> String
    ) -> StampLineFields {
        StampLineFields(
            elevation: displayText(elevationCode, fallback: "B1"),
            angle: displayText(angle, fallback: "North"),
            shotName: displayText(shotName, fallback: "General Elevation"),
            detailID: normalizedDetailID(shotKey: shotKey, angleIndex: angleIndex),
            dateTime: formatter(capturedAt)
        )
    }

    private static func normalizedDetailID(shotKey: String?, angleIndex: Int?) -> String {
        if let shotKey {
            let trimmed = shotKey.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
            if trimmed.range(of: #"^A\d+$"#, options: .regularExpression) != nil {
                return trimmed
            }
        }

        return "A\(angleIndex ?? 0)"
    }

    private static func displayText(_ value: String?, fallback: String) -> String {
        guard let value else { return fallback }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? fallback : trimmed
    }
}
