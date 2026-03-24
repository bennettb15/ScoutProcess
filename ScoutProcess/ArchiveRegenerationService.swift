//
//  ArchiveRegenerationService.swift
//  ScoutProcess
//

import AppKit
import CoreGraphics
import Foundation
import GRDB
import ImageIO
import UniformTypeIdentifiers

struct RegenerationSelection {
    var stampedImagery: Bool
    var propertyReport: Bool
    var flaggedItems: Bool
    var priorityItems: Bool

    var includesPDFs: Bool {
        propertyReport || flaggedItems || priorityItems
    }

    var isEmpty: Bool {
        stampedImagery == false && includesPDFs == false
    }
}

struct RegenerationResult {
    let generatedSessionFolder: URL
    let archiveSessionFolder: URL
}

enum ArchiveRegenerationError: LocalizedError {
    case noOutputsSelected
    case missingSessionJSON
    case missingOriginals
    case missingSessionsCSV
    case missingSessionID

    var errorDescription: String? {
        switch self {
        case .noOutputsSelected: "Choose at least one deliverable to regenerate."
        case .missingSessionJSON: "The selected archive session is missing session.json."
        case .missingOriginals: "The selected archive session is missing Originals."
        case .missingSessionsCSV: "The selected archive session is missing sessions.csv."
        case .missingSessionID: "Unable to resolve session_id from sessions.csv for PDF regeneration."
        }
    }
}

final class ArchiveRegenerationService {
    private let fileManager: FileManager
    private let jpegQuality: CGFloat = 0.85
    private let stampedJPEGMaxLongEdge: CGFloat = 2400
    private static let supportedStampImageExtensions: Set<String> = ["heic", "jpg", "jpeg", "png"]

    init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
    }

    func regenerate(
        archiveSessionFolderURL: URL,
        selection: RegenerationSelection
    ) throws -> RegenerationResult {
        guard selection.isEmpty == false else {
            throw ArchiveRegenerationError.noOutputsSelected
        }

        let sessionJSONURL = archiveSessionFolderURL.appending(path: "session.json")
        guard fileManager.fileExists(atPath: sessionJSONURL.path) else {
            throw ArchiveRegenerationError.missingSessionJSON
        }

        let originalsURL = archiveSessionFolderURL.appending(path: "Originals", directoryHint: .isDirectory)
        guard fileManager.fileExists(atPath: originalsURL.path) else {
            throw ArchiveRegenerationError.missingOriginals
        }

        let sessionFolderName = archiveSessionFolderURL.deletingLastPathComponent().lastPathComponent + " - " + archiveSessionFolderURL.lastPathComponent
        let tempRoot = fileManager.temporaryDirectory
            .appending(path: "ScoutProcessRegen", directoryHint: .isDirectory)
            .appending(path: UUID().uuidString, directoryHint: .isDirectory)
        let generatedSessionFolder = tempRoot.appending(path: sanitizeFileNameComponent(sessionFolderName), directoryHint: .isDirectory)
        try fileManager.createDirectory(at: generatedSessionFolder, withIntermediateDirectories: true)

        let manifest = try loadManifest(at: sessionJSONURL)
        let shouldGenerateStampedImagery = selection.stampedImagery || selection.includesPDFs

        if shouldGenerateStampedImagery {
            let stampedURL = generatedSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
            try fileManager.createDirectory(at: stampedURL, withIntermediateDirectories: true)
            try regenerateStampedImagery(
                manifest: manifest,
                archiveSessionFolderURL: archiveSessionFolderURL,
                originalsURL: originalsURL,
                destinationURL: stampedURL
            )
        }

        if selection.includesPDFs {
            let sessionsCSVURL = archiveSessionFolderURL.appending(path: "sessions.csv")
            guard fileManager.fileExists(atPath: sessionsCSVURL.path) else {
                throw ArchiveRegenerationError.missingSessionsCSV
            }

            guard let sessionID = resolveSessionID(from: sessionsCSVURL) else {
                throw ArchiveRegenerationError.missingSessionID
            }

            var imageFolders: [URL] = [archiveSessionFolderURL]
            let generatedStampedURL = generatedSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
            if fileManager.fileExists(atPath: generatedStampedURL.path) {
                imageFolders.insert(generatedSessionFolder, at: 0)
            }

            let generator = PDFSessionReportGenerator()
            if selection.propertyReport {
                _ = try generator.generateSessionReport(
                    sessionID: sessionID,
                    imageFolderURLs: imageFolders,
                    outputFolderURL: generatedSessionFolder,
                    zipName: nil
                )
            }
            if selection.flaggedItems {
                _ = try generator.generateFlaggedComparisonReport(
                    sessionID: sessionID,
                    imageFolderURLs: imageFolders,
                    outputFolderURL: generatedSessionFolder,
                    zipName: nil
                )
            }
            if selection.priorityItems {
                _ = try generator.generatePriorityItemsReport(
                    sessionID: sessionID,
                    imageFolderURLs: imageFolders,
                    outputFolderURL: generatedSessionFolder,
                    zipName: nil
                )
            }
        }

        return RegenerationResult(
            generatedSessionFolder: generatedSessionFolder,
            archiveSessionFolder: archiveSessionFolderURL
        )
    }

    func export(result: RegenerationResult, selection: RegenerationSelection, destinationRootURL: URL) throws -> URL {
        if selection.stampedImagery == false {
            let stampedFolder = result.generatedSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
            if fileManager.fileExists(atPath: stampedFolder.path) {
                try fileManager.removeItem(at: stampedFolder)
            }
        }

        try fileManager.createDirectory(at: destinationRootURL, withIntermediateDirectories: true)
        let selectedOutputCount =
            (selection.stampedImagery ? 1 : 0) +
            (selection.propertyReport ? 1 : 0) +
            (selection.flaggedItems ? 1 : 0) +
            (selection.priorityItems ? 1 : 0)
        let targetFolder: URL = selectedOutputCount > 1
            ? destinationRootURL.appending(path: result.generatedSessionFolder.lastPathComponent, directoryHint: .isDirectory)
            : destinationRootURL

        try fileManager.createDirectory(at: targetFolder, withIntermediateDirectories: true)

        if selection.stampedImagery {
            let stampedSource = result.generatedSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
            if fileManager.fileExists(atPath: stampedSource.path) {
                try copyReplacingIfNeeded(
                    from: stampedSource,
                    to: targetFolder.appending(path: "Stamped", directoryHint: .isDirectory)
                )
            }
        }

        if selection.includesPDFs {
            let pdfSource = result.generatedSessionFolder.appending(path: "PDF", directoryHint: .isDirectory)
            if fileManager.fileExists(atPath: pdfSource.path) {
                let pdfFiles = try fileManager.contentsOfDirectory(
                    at: pdfSource,
                    includingPropertiesForKeys: [.isRegularFileKey],
                    options: [.skipsHiddenFiles]
                )
                for pdfFile in pdfFiles where (try? pdfFile.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true {
                    try copyReplacingIfNeeded(
                        from: pdfFile,
                        to: targetFolder.appending(path: pdfFile.lastPathComponent)
                    )
                }
            }
        }

        try? fileManager.removeItem(at: result.generatedSessionFolder.deletingLastPathComponent())
        return targetFolder
    }

    private func regenerateStampedImagery(
        manifest: SessionManifest,
        archiveSessionFolderURL: URL,
        originalsURL: URL,
        destinationURL: URL
    ) throws {
        let sourceCatalog = buildSourceImageCatalog(in: originalsURL)
        var usedOutputNames: Set<String> = []
        let sessionID = resolveSessionID(from: archiveSessionFolderURL.appending(path: "sessions.csv"))
        let resolvedVisualIdentities = try resolvedShotIdentitySetForStamping(sessionID: sessionID)

        for shot in manifest.shots {
            let sourceName = shot.originalFilename?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard sourceName.isEmpty == false,
                  let sourceURL = resolveSourceImageURL(named: sourceName, in: sourceCatalog) else {
                continue
            }

            let stampedName = uniqueStampedFilename(
                preferredName: makeStampedFilename(
                    for: shot,
                    session: manifest,
                    forceFlagSuffix: shouldShowFlagVisual(for: shot, session: manifest, resolvedIdentitySet: resolvedVisualIdentities)
                ),
                usedNames: &usedOutputNames
            )
            let visualState = stampVisualState(for: shot, session: manifest, resolvedIdentitySet: resolvedVisualIdentities)
            try stampImage(
                sourceURL: sourceURL,
                destinationURL: destinationURL.appending(path: stampedName),
                stampText: makeStampText(for: shot, session: manifest),
                visualState: visualState
            )
        }
    }

    private func loadManifest(at url: URL) throws -> SessionManifest {
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(SessionManifest.self, from: data)
    }

    private func buildSourceImageCatalog(in originalsURL: URL) -> [String: [URL]] {
        guard let enumerator = fileManager.enumerator(
            at: originalsURL,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return [:]
        }

        var catalog: [String: [URL]] = [:]
        for case let fileURL as URL in enumerator {
            guard (try? fileURL.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true else { continue }
            guard Self.supportedStampImageExtensions.contains(fileURL.pathExtension.lowercased()) else { continue }
            catalog[fileURL.lastPathComponent.lowercased(), default: []].append(fileURL)
        }
        return catalog
    }

    private func resolveSourceImageURL(named sourceName: String, in catalog: [String: [URL]]) -> URL? {
        let matches = catalog[sourceName.lowercased()] ?? []
        return matches.sorted { $0.path.count < $1.path.count }.first
    }

    private func makeStampText(for shot: SessionManifest.Shot, session: SessionManifest) -> String {
        let fields = ScoutShotNaming.stampFields(
            elevationCode: shot.building ?? session.building,
            angle: shot.elevation ?? session.elevation,
            shotName: shot.shotName ?? shot.detailType ?? session.detailType,
            shotKey: shot.shotKey,
            angleIndex: shot.angleIndex,
            capturedAt: shot.capturedAtLocal,
            formatter: { [self] in formatLocalDate($0) }
        )
        return "\(fields.elevation) | \(fields.angle) | \(fields.shotName) | \(fields.detailID) • \(fields.dateTime)".uppercased()
    }

    private func makeStampedFilename(for shot: SessionManifest.Shot, session: SessionManifest, forceFlagSuffix: Bool = false) -> String {
        ScoutShotNaming.stampedJPEGFilename(
            building: shot.building ?? session.building,
            elevation: shot.elevation ?? session.elevation,
            detailType: shot.detailType ?? session.detailType,
            angleIndex: shot.angleIndex,
            shotKey: shot.shotKey,
            shotName: shot.shotName,
            isFlagged: shot.isFlagged == true || forceFlagSuffix
        )
    }

    private func formatLocalDate(_ rawValue: String?) -> String {
        guard let rawValue else {
            return Self.overlayDateOnlyFormatter.string(from: Date())
        }
        if let date = Self.inputDateFormatter.date(from: rawValue) ?? Self.isoDateFormatter.date(from: rawValue) {
            return Self.overlayDateOnlyFormatter.string(from: date)
        }
        return rawValue
    }

    private func stampImage(sourceURL: URL, destinationURL: URL, stampText: String, visualState: RegenerationVisualState) throws {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil),
              let original = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        let originalSize = CGSize(width: original.width, height: original.height)
        let size = scaledStampedOutputSize(for: originalSize)
        let colorSpace = original.colorSpace ?? CGColorSpace(name: CGColorSpace.sRGB) ?? CGColorSpaceCreateDeviceRGB()
        guard let context = CGContext(
            data: nil,
            width: Int(size.width),
            height: Int(size.height),
            bitsPerComponent: 8,
            bytesPerRow: 0,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        context.setAllowsAntialiasing(true)
        context.interpolationQuality = .high

        let drawRect = CGRect(origin: .zero, size: size)
        context.draw(original, in: drawRect)

        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.lineBreakMode = .byTruncatingTail

        let isPortrait = size.height > size.width
        let outputLongEdge = max(size.width, size.height)
        let overlayScale = max(min(outputLongEdge / stampedJPEGMaxLongEdge, 1), 0.72)
        let fontSize: CGFloat = max((isPortrait ? 49 : 42) * overlayScale, isPortrait ? 30 : 26)
        let pillHeight: CGFloat = max((isPortrait ? 96 : 84) * overlayScale, isPortrait ? 70 : 64)
        let horizontalPadding: CGFloat = max((isPortrait ? 30 : 27) * overlayScale, 20)
        let verticalPadding: CGFloat = max((isPortrait ? 19 : 17) * overlayScale, 13)
        let bottomMargin: CGFloat = max((isPortrait ? 36 : 32) * overlayScale, 22)
        let sideMargin: CGFloat = max((isPortrait ? 36 : 32) * overlayScale, 22)
        let cornerRadius: CGFloat = max((isPortrait ? 19 : 17) * overlayScale, 13)
        let maxOverlayWidth = size.width - (sideMargin * 2)
        let showsFlagGlyph = visualState != .none
        let glyphGap: CGFloat = showsFlagGlyph ? max(11 * overlayScale, 8) : 0
        let textAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: fontSize, weight: .semibold),
            .foregroundColor: NSColor.white,
            .paragraphStyle: paragraph,
        ]
        let glyphImage = showsFlagGlyph ? makeFlagGlyphImage(fontSize: fontSize, visualState: visualState) : nil
        let glyphSize = glyphImage?.size ?? .zero
        let resolvedStampText = fittedStampText(
            stampText,
            maxTextWidth: maxOverlayWidth - (horizontalPadding * 2) - glyphSize.width - glyphGap,
            attributes: textAttributes
        )
        let measuredText = (resolvedStampText as NSString).size(withAttributes: textAttributes)
        let pillWidth = min(maxOverlayWidth, measuredText.width + (horizontalPadding * 2) + glyphSize.width + glyphGap)
        let pillRect = CGRect(
            x: drawRect.maxX - sideMargin - pillWidth,
            y: drawRect.minY + bottomMargin,
            width: pillWidth,
            height: pillHeight
        )

        context.saveGState()
        let graphicsContext = NSGraphicsContext(cgContext: context, flipped: false)
        NSGraphicsContext.saveGraphicsState()
        NSGraphicsContext.current = graphicsContext
        let backgroundPath = NSBezierPath(roundedRect: pillRect, xRadius: cornerRadius, yRadius: cornerRadius)
        NSColor.black.withAlphaComponent(0.42).setFill()
        backgroundPath.fill()

        var textStartX = pillRect.minX + horizontalPadding
        if let glyphImage {
            let glyphRect = CGRect(
                x: textStartX,
                y: pillRect.minY + max(0, (pillRect.height - glyphSize.height) / 2) - 1,
                width: glyphSize.width,
                height: glyphSize.height
            )
            glyphImage.draw(in: glyphRect)
            textStartX += glyphSize.width + glyphGap
        }

        let textRect = CGRect(
            x: textStartX,
            y: pillRect.minY + max(0, (pillRect.height - measuredText.height) / 2) - 1,
            width: pillRect.maxX - horizontalPadding - textStartX,
            height: pillRect.height - (verticalPadding * 2)
        )
        (resolvedStampText as NSString).draw(in: textRect, withAttributes: textAttributes)

        NSGraphicsContext.restoreGraphicsState()
        context.restoreGState()

        guard let destination = CGImageDestinationCreateWithURL(
            destinationURL as CFURL,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            throw ScoutProcessError.invalidImage(destinationURL.lastPathComponent)
        }

        let properties: [CFString: Any] = [kCGImageDestinationLossyCompressionQuality: jpegQuality]
        guard let renderedImage = context.makeImage() else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }
        CGImageDestinationAddImage(destination, renderedImage, properties as CFDictionary)
        guard CGImageDestinationFinalize(destination) else {
            throw ScoutProcessError.invalidImage(destinationURL.lastPathComponent)
        }
    }

    private func scaledStampedOutputSize(for originalSize: CGSize) -> CGSize {
        let longEdge = max(originalSize.width, originalSize.height)
        guard longEdge > 0 else { return originalSize }
        guard longEdge > stampedJPEGMaxLongEdge else {
            return CGSize(width: round(originalSize.width), height: round(originalSize.height))
        }

        let scale = stampedJPEGMaxLongEdge / longEdge
        return CGSize(
            width: max(1, Int(round(originalSize.width * scale))),
            height: max(1, Int(round(originalSize.height * scale)))
        )
    }

    private func fittedStampText(_ stampText: String, maxTextWidth: CGFloat, attributes: [NSAttributedString.Key: Any]) -> String {
        if (stampText as NSString).size(withAttributes: attributes).width <= maxTextWidth {
            return stampText
        }
        let majorParts = stampText.components(separatedBy: " | ")
        guard majorParts.count == 4 else { return stampText }
        let tailParts = majorParts[3].components(separatedBy: " • ")
        guard tailParts.count == 2 else { return stampText }
        let prefix = "\(majorParts[0]) | \(majorParts[1]) | "
        let detailAndDate = " | \(tailParts[0]) • \(tailParts[1])"
        var shotName = majorParts[2]
        while shotName.count > 4 {
            shotName.removeLast()
            let candidate = "\(prefix)\(shotName)...\(detailAndDate)"
            if (candidate as NSString).size(withAttributes: attributes).width <= maxTextWidth {
                return candidate
            }
        }
        return "\(prefix)...\(detailAndDate)"
    }

    private func makeFlagGlyphImage(fontSize: CGFloat, visualState: RegenerationVisualState) -> NSImage? {
        let symbolConfig = NSImage.SymbolConfiguration(pointSize: fontSize * 0.82, weight: .bold)
        guard let symbolImage = NSImage(systemSymbolName: "flag.fill", accessibilityDescription: "Flagged")?
            .withSymbolConfiguration(symbolConfig) else {
            return nil
        }

        let tintedImage = symbolImage.copy() as? NSImage ?? symbolImage
        tintedImage.lockFocus()
        switch visualState {
        case .resolved:
            NSColor(calibratedRed: 0.149, green: 0.678, blue: 0.337, alpha: 1.0).set()
        case .flagged:
            NSColor(calibratedRed: 0.827, green: 0.184, blue: 0.184, alpha: 1.0).set()
        case .none:
            NSColor.white.set()
        }
        NSRect(origin: .zero, size: tintedImage.size).fill(using: .sourceAtop)
        tintedImage.unlockFocus()
        return tintedImage
    }

    private func resolvedShotIdentitySetForStamping(sessionID: String?) throws -> Set<String> {
        guard let sessionID, let dbQueue = DatabaseManager.shared.dbQueue else { return [] }
        return try dbQueue.read { db in
            let identities = try String.fetchAll(
                db,
                sql: """
                SELECT COALESCE(NULLIF(TRIM(s.logical_shot_identity), ''), NULLIF(TRIM(s.shot_key), ''), s.shot_id)
                FROM shots s
                LEFT JOIN issues i ON i.issue_id = s.issue_id
                WHERE s.session_id = ?
                  AND (
                      (i.resolved_at_utc IS NOT NULL AND TRIM(i.resolved_at_utc) <> '')
                      OR LOWER(TRIM(COALESCE(i.current_status, ''))) = 'resolved'
                  )
                  AND COALESCE(NULLIF(TRIM(i.last_capture_session_id), ''), s.session_id) = s.session_id
                """,
                arguments: [sessionID]
            )
            return Set(identities)
        }
    }

    private func shouldShowFlagVisual(for shot: SessionManifest.Shot, session: SessionManifest, resolvedIdentitySet: Set<String>) -> Bool {
        shot.isFlagged == true || stampVisualState(for: shot, session: session, resolvedIdentitySet: resolvedIdentitySet) == .resolved
    }

    private func stampVisualState(for shot: SessionManifest.Shot, session: SessionManifest, resolvedIdentitySet: Set<String>) -> RegenerationVisualState {
        let identity = stampIdentity(for: shot, session: session)
        if resolvedIdentitySet.contains(identity) { return .resolved }
        if shot.isFlagged == true { return .flagged }
        return .none
    }

    private func stampIdentity(for shot: SessionManifest.Shot, session: SessionManifest) -> String {
        if let logical = shot.logicalShotIdentity?.trimmingCharacters(in: .whitespacesAndNewlines), logical.isEmpty == false {
            return logical
        }
        if let key = shot.shotKey?.trimmingCharacters(in: .whitespacesAndNewlines), key.isEmpty == false {
            return key
        }
        return "\(shot.detailType ?? session.detailType ?? "Shot") A\(shot.angleIndex ?? 0)"
    }

    private func uniqueStampedFilename(preferredName: String, usedNames: inout Set<String>) -> String {
        let preferredLower = preferredName.lowercased()
        guard usedNames.contains(preferredLower) == false else {
            let baseURL = URL(fileURLWithPath: preferredName)
            let baseName = baseURL.deletingPathExtension().lastPathComponent
            let ext = baseURL.pathExtension.isEmpty ? "jpg" : baseURL.pathExtension
            for index in 2...999 {
                let candidate = "\(baseName)_\(index).\(ext)"
                if usedNames.contains(candidate.lowercased()) == false {
                    usedNames.insert(candidate.lowercased())
                    return candidate
                }
            }
            usedNames.insert(preferredLower)
            return preferredName
        }

        usedNames.insert(preferredLower)
        return preferredName
    }

    private func resolveSessionID(from sessionsCSVURL: URL) -> String? {
        guard let text = try? String(contentsOf: sessionsCSVURL, encoding: .utf8) else {
            return nil
        }
        let rows = parseCSV(text)
        return rows.first?["session_id"]?.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func parseCSV(_ text: String) -> [[String: String]] {
        let lines = text
            .split(whereSeparator: \.isNewline)
            .map(String.init)
            .filter { $0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false }
        guard let headerLine = lines.first else { return [] }
        let header = parseCSVLine(headerLine).map(canonicalCSVKey)
        return lines.dropFirst().map { line in
            let values = parseCSVLine(line)
            var row: [String: String] = [:]
            for (index, key) in header.enumerated() where values.indices.contains(index) {
                row[key] = values[index]
            }
            return row
        }
    }

    private func canonicalCSVKey(_ raw: String) -> String {
        raw.trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: "\u{feff}", with: "")
            .lowercased()
    }

    private func parseCSVLine(_ line: String) -> [String] {
        var values: [String] = []
        var current = ""
        var inQuotes = false
        var index = line.startIndex
        while index < line.endIndex {
            let char = line[index]
            if char == "\"" {
                let next = line.index(after: index)
                if inQuotes, next < line.endIndex, line[next] == "\"" {
                    current.append("\"")
                    index = line.index(after: next)
                    continue
                }
                inQuotes.toggle()
            } else if char == ",", inQuotes == false {
                values.append(current)
                current = ""
            } else {
                current.append(char)
            }
            index = line.index(after: index)
        }
        values.append(current)
        return values
    }

    private func copyReplacingIfNeeded(from source: URL, to destination: URL) throws {
        if fileManager.fileExists(atPath: destination.path) {
            try fileManager.removeItem(at: destination)
        }
        try fileManager.copyItem(at: source, to: destination)
    }

    private func sanitizeFileNameComponent(_ rawValue: String) -> String {
        let replaced = rawValue.unicodeScalars.map { scalar -> Character in
            if CharacterSet.controlCharacters.contains(scalar) || scalar == "/" || scalar == ":" || scalar == "\\" {
                return " "
            }
            return Character(scalar)
        }
        let collapsed = String(replaced).replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
        let trimmed = collapsed.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "Item" : trimmed
    }

    private static let overlayDateOnlyFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "MM/dd/yyyy"
        return formatter
    }()

    private static let inputDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        return formatter
    }()

    private static let isoDateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()
}

private enum RegenerationVisualState {
    case none
    case flagged
    case resolved
}
