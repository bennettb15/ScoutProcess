//
//  PDFSessionReportGenerator.swift
//  ScoutProcess
//

import AppKit
import CoreGraphics
import Foundation
import GRDB
import ImageIO
import UniformTypeIdentifiers

final class PDFSessionReportGenerator {
    private let includeHeader: Bool
    private let fileManager: FileManager
    private let databaseManager: DatabaseManager

    init(
        includeHeader: Bool = false,
        fileManager: FileManager = .default,
        databaseManager: DatabaseManager = .shared
    ) {
        self.includeHeader = includeHeader
        self.fileManager = fileManager
        self.databaseManager = databaseManager
    }

    func generateSessionReport(sessionID: String, extractedFolderURL: URL, zipName: String?) throws -> URL {
        guard let dbQueue = databaseManager.dbQueue else {
            throw PDFSessionReportError.databaseUnavailable
        }

        let reportContext = try dbQueue.read { db in
            guard let session = try SessionReportSession.fetchOne(db, sql: """
                SELECT
                    session_id,
                    org_name,
                    folder_id,
                    property_name,
                    property_address,
                    propertyStreet,
                    propertyCity,
                    propertyState,
                    propertyZip,
                    started_at_utc,
                    ended_at_utc
                FROM sessions
                WHERE session_id = ?
                """, arguments: [sessionID]) else {
                throw PDFSessionReportError.sessionNotFound(sessionID)
            }

            let shots = try SessionReportShot.fetchAll(db, sql: """
                SELECT
                    shots.shot_id,
                    shots.building,
                    shots.elevation,
                    shots.detail_type,
                    shots.angle_index,
                    shots.shot_key,
                    shots.logical_shot_identity,
                    shots.captured_at_utc,
                    shots.original_filename,
                    shots.stamped_jpeg_filename,
                    shots.is_flagged,
                    COALESCE(NULLIF(TRIM(shots.flagged_reason), ''), NULLIF(TRIM(issues.current_reason), '')) AS flagged_reason
                FROM shots
                LEFT JOIN issues ON issues.issue_id = shots.issue_id
                WHERE shots.session_id = ?
                ORDER BY captured_at_utc ASC, original_filename ASC
                """, arguments: [sessionID])

            return SessionReportContext(session: session, shots: shots)
        }

        let pdfFolder = try makePDFFolder(in: extractedFolderURL)
        let pdfURL = pdfFolder.appending(path: makePDFFileName(for: reportContext.session))
        let imageCatalog = ImageCatalog(sessionFolderURL: extractedFolderURL, fileManager: fileManager)

        try renderPDF(
            context: reportContext,
            imageCatalog: imageCatalog,
            pdfURL: pdfURL,
            zipName: zipName
        )

        log("Session report generated: \(pdfURL.path(percentEncoded: false))")
        return pdfURL
    }

    private func makePDFFolder(in archivedSessionFolderURL: URL) throws -> URL {
        let pdfFolderURL = archivedSessionFolderURL.appendingPathComponent("PDF", isDirectory: true)
        try fileManager.createDirectory(at: pdfFolderURL, withIntermediateDirectories: true)
        return pdfFolderURL
    }

    private func makePDFFileName(for session: SessionReportSession) -> String {
        let propertyName = sanitizeFileNameComponent(session.propertyName ?? "Unknown Property")
        let date = Self.parseUTCDate(session.startedAtUTC) ?? Date()
        let dateString = Self.fileDateFormatter.string(from: date)
        return "Scout Property Record - \(propertyName) - \(dateString).pdf"
    }

    private func renderPDF(
        context: SessionReportContext,
        imageCatalog: ImageCatalog,
        pdfURL: URL,
        zipName _: String?
    ) throws {
        guard let consumer = CGDataConsumer(url: pdfURL as CFURL) else {
            throw PDFSessionReportError.pdfContextCreationFailed(pdfURL.path)
        }

        var mediaBox = CGRect(origin: .zero, size: Self.pageSize)
        guard let pdfContext = CGContext(consumer: consumer, mediaBox: &mediaBox, nil) else {
            throw PDFSessionReportError.pdfContextCreationFailed(pdfURL.path)
        }

        var pageNumber = 1

        pdfContext.beginPDFPage(nil as CFDictionary?)
        drawCoverPage(in: pdfContext, context: context, pageNumber: pageNumber)
        pdfContext.endPDFPage()
        pageNumber += 1

        let photoEntries = context.shots.map { shot in
            let resolvedImage = imageCatalog.resolveImage(for: shot)
            if resolvedImage == nil {
                log("Session report warning: missing image for shot \(shot.shotID) (\(shot.originalFilename))")
            }
            return SessionReportPhotoEntry(shot: shot, imageURL: resolvedImage?.url)
        }

        for chunkStart in stride(from: 0, to: photoEntries.count, by: 2) {
            let pageEntries = Array(photoEntries[chunkStart..<min(chunkStart + 2, photoEntries.count)])
            pdfContext.beginPDFPage(nil as CFDictionary?)
            drawPhotoPage(
                in: pdfContext,
                context: context,
                entries: pageEntries,
                pageNumber: pageNumber
            )
            pdfContext.endPDFPage()
            pageNumber += 1
        }

        pdfContext.closePDF()
    }

    private func drawCoverPage(
        in pdfContext: CGContext,
        context: SessionReportContext,
        pageNumber: Int
    ) {
        let bounds = CGRect(origin: .zero, size: Self.pageSize)
        withGraphicsContext(pdfContext) {
            NSColor.white.setFill()
            bounds.fill()

            let leftMargin: CGFloat = 72
            var cursorY: CGFloat = bounds.height - 120

            drawText(
                "SCOUT Session Report",
                in: CGRect(x: leftMargin, y: cursorY, width: bounds.width - (leftMargin * 2), height: 34),
                font: .systemFont(ofSize: 28, weight: .bold)
            )
            cursorY -= 56

            let session = context.session
            let address = formattedAddress(for: session)
            let started = formattedUTC(session.startedAtUTC) ?? session.startedAtUTC
            let ended = formattedUTC(session.endedAtUTC) ?? "Open"
            let generatedAt = Self.displayDateTimeFormatter.string(from: Date())
            let shortSessionID = String(session.sessionID.prefix(8))

            let coverLines: [String] = [
                "Organization: \(session.orgName ?? "Unknown Org")",
                "Property: \(session.propertyName ?? "Unknown Property")",
                "Address: \(address)",
                "Session: \(started) to \(ended)",
                "Session ID: \(shortSessionID)",
                "Generated: \(generatedAt)",
            ]

            for line in coverLines {
                drawText(
                    line,
                    in: CGRect(x: leftMargin, y: cursorY, width: bounds.width - (leftMargin * 2), height: 24),
                    font: .systemFont(ofSize: 15, weight: .regular)
                )
                cursorY -= 30
            }

            drawFooter(pageNumber: pageNumber, in: bounds)
        }
    }

    private func drawPhotoPage(
        in pdfContext: CGContext,
        context: SessionReportContext,
        entries: [SessionReportPhotoEntry],
        pageNumber: Int
    ) {
        let bounds = CGRect(origin: .zero, size: Self.pageSize)
        withGraphicsContext(pdfContext) {
            NSColor.white.setFill()
            bounds.fill()

            let outerMargin: CGFloat = 36
            let footerHeight: CGFloat = 22
            let headerHeight: CGFloat = includeHeader ? 20 : 0
            let photoCaptionGap: CGFloat = 10
            let metadataHeight: CGFloat = 66
            let slotGap: CGFloat = 10

            if includeHeader {
                let headerLine = "\(context.session.propertyName ?? "Property")  |  \(formattedUTC(context.session.startedAtUTC) ?? context.session.startedAtUTC)"
                drawText(
                    headerLine,
                    in: CGRect(
                        x: outerMargin,
                        y: bounds.height - outerMargin - 14,
                        width: bounds.width - (outerMargin * 2),
                        height: 14
                    ),
                    font: .systemFont(ofSize: 10, weight: .medium),
                    color: .darkGray
                )
            }

            let contentTop = bounds.height - outerMargin - headerHeight
            let contentBottom = outerMargin + footerHeight
            let contentHeight = contentTop - contentBottom
            let slotHeight = (contentHeight - slotGap) / 2
            let slotWidth = bounds.width - (outerMargin * 2)

            for (index, entry) in entries.enumerated() {
                let slotTop = contentTop - CGFloat(index) * (slotHeight + slotGap)
                let captionRect = CGRect(
                    x: outerMargin,
                    y: slotTop - slotHeight,
                    width: slotWidth,
                    height: metadataHeight
                )
                let photoAvailableRect = CGRect(
                    x: outerMargin,
                    y: captionRect.maxY + photoCaptionGap,
                    width: slotWidth,
                    height: slotHeight - metadataHeight - photoCaptionGap
                )

                if let imageURL = entry.imageURL, let image = loadOptimizedCGImage(at: imageURL) {
                    let fittedRect = aspectFitRect(for: image, in: photoAvailableRect)
                    pdfContext.saveGState()
                    let clipPath = NSBezierPath(roundedRect: fittedRect, xRadius: 12, yRadius: 12)
                    clipPath.addClip()
                    pdfContext.draw(image, in: fittedRect)
                    pdfContext.restoreGState()
                    if entry.shot.isFlagged == 1 {
                        drawFlaggedBorder(around: fittedRect, in: pdfContext)
                    }
                } else {
                    if let imageURL = entry.imageURL {
                        log("Session report warning: could not decode image \(imageURL.lastPathComponent)")
                    }
                    NSColor(white: 0.95, alpha: 1).setFill()
                    photoAvailableRect.fill()
                    drawText(
                        "Image unavailable",
                        in: photoAvailableRect,
                        font: .systemFont(ofSize: 14, weight: .medium),
                        color: .darkGray,
                        alignment: .center,
                        verticalCenter: true
                    )
                }

                drawMetadataBlock(for: entry.shot, in: captionRect)
            }

            drawFooter(pageNumber: pageNumber, in: bounds)
        }
    }

    private func drawFooter(pageNumber: Int, in bounds: CGRect) {
        drawText(
            "Page \(pageNumber)",
            in: CGRect(x: 0, y: 20, width: bounds.width, height: 12),
            font: .systemFont(ofSize: 10, weight: .regular),
            color: .darkGray,
            alignment: .center
        )
    }

    private func drawMetadataBlock(for shot: SessionReportShot, in rect: CGRect) {
        let titleFont = NSFont.systemFont(ofSize: 11, weight: .semibold)
        let bodyFont = NSFont.systemFont(ofSize: 10, weight: .regular)
        let noteFont = NSFont.systemFont(ofSize: 10, weight: .regular)
        let lineHeight: CGFloat = 14
        let topY = rect.maxY - lineHeight

        let identityLine = [
            displayText(shot.building, fallback: "B1"),
            displayText(shot.elevation, fallback: "North"),
            displayText(shot.detailType, fallback: "General Elevation"),
            detailIdentifier(for: shot),
        ]
        .joined(separator: " | ")
        .uppercased()

        let capturedAt = formattedUTC(shot.capturedAtUTC) ?? "Unknown"

        drawText(
            identityLine,
            in: CGRect(x: rect.minX, y: topY, width: rect.width, height: lineHeight),
            font: titleFont,
            alignment: .center
        )
        drawText(
            capturedAt,
            in: CGRect(x: rect.minX, y: topY - lineHeight, width: rect.width, height: lineHeight),
            font: bodyFont,
            color: .darkGray,
            alignment: .center
        )

        if shot.isFlagged == 1 {
            let flaggedReason = shot.flaggedReason?.trimmingCharacters(in: .whitespacesAndNewlines)
            let noteText = (flaggedReason?.isEmpty == false) ? flaggedReason! : "Flagged"
            drawFlaggedNote(
                text: noteText,
                in: CGRect(x: rect.minX, y: topY - (lineHeight * 2), width: rect.width, height: lineHeight),
                font: noteFont
            )
        }
    }

    private func drawFlaggedBorder(around rect: CGRect, in context: CGContext) {
        let borderThickness: CGFloat = 3
        let borderOutset: CGFloat = 0.5
        let borderRect = rect.insetBy(dx: -borderOutset, dy: -borderOutset)

        context.saveGState()
        context.setStrokeColor(NSColor(calibratedRed: 0.827, green: 0.184, blue: 0.184, alpha: 1.0).cgColor)
        context.setLineWidth(borderThickness)
        let borderPath = CGPath(
            roundedRect: borderRect,
            cornerWidth: 12.5,
            cornerHeight: 12.5,
            transform: nil
        )
        context.addPath(borderPath)
        context.strokePath()
        context.restoreGState()
    }

    private func drawFlaggedNote(text: String, in rect: CGRect, font: NSFont) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.lineBreakMode = .byTruncatingTail

        let note = NSMutableAttributedString()
        note.append(NSAttributedString(
            string: "⚑ ",
            attributes: [
                .font: font,
                .foregroundColor: NSColor(calibratedRed: 0.827, green: 0.184, blue: 0.184, alpha: 1.0),
                .paragraphStyle: paragraph,
            ]
        ))
        note.append(NSAttributedString(
            string: text,
            attributes: [
                .font: font,
                .foregroundColor: NSColor.darkGray,
                .paragraphStyle: paragraph,
            ]
        ))
        note.draw(in: rect)
    }

    private func displayShotLabel(for shot: SessionReportShot) -> String {
        let trimmedShotKey = shot.shotKey?.trimmingCharacters(in: .whitespacesAndNewlines)
        if let trimmedShotKey, trimmedShotKey.isEmpty == false {
            return trimmedShotKey
        }
        return "\(displayText(shot.detailType, fallback: "Shot")) A\(shot.angleIndex ?? 0)"
    }

    private func detailIdentifier(for shot: SessionReportShot) -> String {
        let trimmedShotKey = shot.shotKey?.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        if let trimmedShotKey, trimmedShotKey.range(of: #"^A\d+$"#, options: .regularExpression) != nil {
            return trimmedShotKey
        }
        return "A\(shot.angleIndex ?? 0)"
    }

    private func displayText(_ value: String?, fallback: String) -> String {
        guard let value else { return fallback }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? fallback : trimmed
    }

    private func formattedAddress(for session: SessionReportSession) -> String {
        if let propertyAddress = session.propertyAddress?.trimmingCharacters(in: .whitespacesAndNewlines),
           propertyAddress.isEmpty == false {
            return propertyAddress
        }

        let cityStateZip = [
            session.propertyCity,
            [session.propertyState, session.propertyZip]
                .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { $0.isEmpty == false }
                .joined(separator: " "),
        ]
        .compactMap { value -> String? in
            guard let value else { return nil }
            let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
            return trimmed.isEmpty ? nil : trimmed
        }
        .joined(separator: ", ")

        let parts = [session.propertyStreet, cityStateZip]
            .compactMap { value -> String? in
                guard let value else { return nil }
                let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }

        return parts.isEmpty ? "Unknown address" : parts.joined(separator: ", ")
    }

    private func formattedUTC(_ rawValue: String?) -> String? {
        guard let rawValue else { return nil }
        if let date = Self.parseUTCDate(rawValue) {
            return Self.displayDateTimeFormatter.string(from: date)
        }
        return rawValue
    }

    private func sanitizeFileNameComponent(_ rawValue: String) -> String {
        let replaced = rawValue.unicodeScalars.map { scalar -> Character in
            if CharacterSet.controlCharacters.contains(scalar)
                || scalar == "/"
                || scalar == ":"
                || scalar == "\\"
            {
                return " "
            }
            return Character(scalar)
        }

        let collapsed = String(replaced).replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
        let trimmed = collapsed.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "Item" : trimmed
    }

    private func loadOptimizedCGImage(at url: URL) -> CGImage? {
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil) else {
            return nil
        }

        let thumbnailOptions: [CFString: Any] = [
            kCGImageSourceCreateThumbnailFromImageAlways: true,
            kCGImageSourceThumbnailMaxPixelSize: 2000,
            kCGImageSourceCreateThumbnailWithTransform: true,
        ]

        guard let resizedImage = CGImageSourceCreateThumbnailAtIndex(source, 0, thumbnailOptions as CFDictionary) else {
            return nil
        }

        let jpegData = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(
            jpegData,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            return resizedImage
        }

        let jpegOptions: [CFString: Any] = [
            kCGImageDestinationLossyCompressionQuality: 0.80,
        ]
        CGImageDestinationAddImage(destination, resizedImage, jpegOptions as CFDictionary)

        guard CGImageDestinationFinalize(destination),
              let optimizedSource = CGImageSourceCreateWithData(jpegData, nil),
              let optimizedImage = CGImageSourceCreateImageAtIndex(optimizedSource, 0, nil) else {
            return resizedImage
        }

        return optimizedImage
    }

    private func aspectFitRect(for image: CGImage, in container: CGRect) -> CGRect {
        let imageSize = CGSize(width: image.width, height: image.height)
        guard imageSize.width > 0, imageSize.height > 0, container.width > 0, container.height > 0 else {
            return container
        }

        let scale = min(container.width / imageSize.width, container.height / imageSize.height)
        let fittedSize = CGSize(width: imageSize.width * scale, height: imageSize.height * scale)
        return CGRect(
            x: container.minX + (container.width - fittedSize.width) / 2,
            y: container.minY + (container.height - fittedSize.height) / 2,
            width: fittedSize.width,
            height: fittedSize.height
        )
    }

    private func drawText(
        _ text: String,
        in rect: CGRect,
        font: NSFont,
        color: NSColor = .black,
        alignment: NSTextAlignment = .left,
        verticalCenter: Bool = false
    ) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = alignment
        paragraph.lineBreakMode = .byTruncatingTail

        let attributes: [NSAttributedString.Key: Any] = [
            .font: font,
            .foregroundColor: color,
            .paragraphStyle: paragraph,
        ]

        var drawRect = rect
        if verticalCenter {
            let measured = (text as NSString).boundingRect(
                with: CGSize(width: rect.width, height: .greatestFiniteMagnitude),
                options: [.usesLineFragmentOrigin, .usesFontLeading],
                attributes: attributes
            )
            drawRect.origin.y += max(0, (rect.height - ceil(measured.height)) / 2)
        }

        (text as NSString).draw(in: drawRect, withAttributes: attributes)
    }

    private func withGraphicsContext(_ context: CGContext, draw: () -> Void) {
        context.saveGState()
        let graphicsContext = NSGraphicsContext(cgContext: context, flipped: false)
        NSGraphicsContext.saveGraphicsState()
        NSGraphicsContext.current = graphicsContext
        draw()
        NSGraphicsContext.restoreGraphicsState()
        context.restoreGState()
    }

    private func log(_ message: String) {
        NSLog("[PDFSessionReportGenerator] %@", message)
    }

    private struct SessionReportContext {
        let session: SessionReportSession
        let shots: [SessionReportShot]
    }

    private struct SessionReportPhotoEntry {
        let shot: SessionReportShot
        let imageURL: URL?
    }

    private struct ImageCatalog {
        private enum ResolvedSource: String {
            case stamped = "STAMPED"
            case jpg = "JPG"
            case original = "ORIGINAL"
        }

        private struct ResolvedImage {
            let url: URL
            let source: ResolvedSource
            let prefix: String
        }

        private let stampedFiles: [URL]
        private let stampedFilesByLowerName: [String: [URL]]
        let allFiles: [URL]

        init(sessionFolderURL: URL, fileManager: FileManager) {
            allFiles = (fileManager.enumerator(
                at: sessionFolderURL,
                includingPropertiesForKeys: [.isRegularFileKey, .contentModificationDateKey],
                options: [.skipsHiddenFiles]
            )?.allObjects as? [URL])?.filter {
                (try? $0.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true
            } ?? []

            let stampedDirectory = sessionFolderURL.appendingPathComponent("Stamped", isDirectory: true)
            stampedFiles = allFiles.filter { fileURL in
                let fileExt = fileURL.pathExtension.lowercased()
                guard fileExt == "jpg" || fileExt == "jpeg" else { return false }
                return fileURL.deletingLastPathComponent().path == stampedDirectory.path
            }
            stampedFilesByLowerName = Dictionary(grouping: stampedFiles, by: { $0.lastPathComponent.lowercased() })
        }

        func resolveImage(for shot: SessionReportShot) -> (url: URL, isStamped: Bool)? {
            if let stampedMatch = resolveStampedImage(for: shot) {
                logResolution(for: shot, resolvedImage: stampedMatch)
                return (stampedMatch.url, true)
            }

            if let jpgMatch = resolveNonStampedJPEG(for: shot) {
                logResolution(for: shot, resolvedImage: jpgMatch)
                return (jpgMatch.url, false)
            }

            if let originalMatch = resolveOriginalImage(for: shot) {
                logResolution(for: shot, resolvedImage: originalMatch)
                return (originalMatch.url, false)
            }

            return nil
        }

        private func resolveStampedImage(for shot: SessionReportShot) -> ResolvedImage? {
            if let storedName = shot.stampedJpegFilename?.trimmingCharacters(in: .whitespacesAndNewlines),
               storedName.isEmpty == false,
               let exactMatch = bestMatch(in: stampedFilesByLowerName[storedName.lowercased()] ?? []) {
                return ResolvedImage(url: exactMatch, source: .stamped, prefix: storedName)
            }

            if let storedFallback = resolveStoredStampedImage(for: shot) {
                return storedFallback
            }

            let deterministicName = ScoutShotNaming.stampedJPEGFilename(
                building: shot.building,
                elevation: shot.elevation,
                detailType: shot.detailType,
                angleIndex: shot.angleIndex,
                shotKey: shot.shotKey,
                shotName: nil,
                isFlagged: shot.isFlagged == 1
            )
            if let exactMatch = bestMatch(in: stampedFilesByLowerName[deterministicName.lowercased()] ?? []) {
                return ResolvedImage(url: exactMatch, source: .stamped, prefix: deterministicName)
            }

            let prefix = stampedPrefix(for: shot)
            let prefixLower = prefix.lowercased()

            var matches = stampedFiles.filter { fileURL in
                let fileName = fileURL.lastPathComponent.lowercased()
                return fileName.hasPrefix(prefixLower)
            }

            if shot.isFlagged == 1 {
                let flaggedMatches = matches.filter {
                    $0.lastPathComponent.range(of: "flagged", options: .caseInsensitive) != nil
                }
                if flaggedMatches.isEmpty == false {
                    matches = flaggedMatches
                }
            }

            guard let bestMatch = bestMatch(in: matches) else { return nil }
            return ResolvedImage(url: bestMatch, source: .stamped, prefix: prefix)
        }

        private func resolveStoredStampedImage(for shot: SessionReportShot) -> ResolvedImage? {
            guard let storedName = shot.stampedJpegFilename?.trimmingCharacters(in: .whitespacesAndNewlines),
                  storedName.isEmpty == false else {
                return nil
            }

            let storedStem = normalizedFileStem(for: storedName)
            let normalizedStoredStem = storedStem.replacingOccurrences(of: "_flagged", with: "")
            let matches = stampedFiles.filter { fileURL in
                let candidateStem = normalizedFileStem(for: fileURL.lastPathComponent)
                if candidateStem == storedStem {
                    return true
                }
                return candidateStem.replacingOccurrences(of: "_flagged", with: "") == normalizedStoredStem
            }

            guard let bestMatch = bestMatch(in: matches) else { return nil }
            return ResolvedImage(url: bestMatch, source: .stamped, prefix: storedName)
        }

        private func resolveNonStampedJPEG(for shot: SessionReportShot) -> ResolvedImage? {
            let originalURL = URL(fileURLWithPath: shot.originalFilename)
            let baseName = originalURL.deletingPathExtension().lastPathComponent.lowercased()
            let jpgMatches = allFiles.filter { fileURL in
                let fileBase = fileURL.deletingPathExtension().lastPathComponent.lowercased()
                let fileExt = fileURL.pathExtension.lowercased()
                guard fileExt == "jpg" || fileExt == "jpeg" else { return false }
                return fileBase == baseName
            }

            guard let bestMatch = bestMatch(in: jpgMatches) else { return nil }
            return ResolvedImage(url: bestMatch, source: .jpg, prefix: stampedPrefix(for: shot))
        }

        private func resolveOriginalImage(for shot: SessionReportShot) -> ResolvedImage? {
            let originalURL = URL(fileURLWithPath: shot.originalFilename)
            let originalName = originalURL.lastPathComponent.lowercased()
            let baseName = originalURL.deletingPathExtension().lastPathComponent.lowercased()

            let originalMatches = allFiles.filter { fileURL in
                let fileName = fileURL.lastPathComponent.lowercased()
                let fileBase = fileURL.deletingPathExtension().lastPathComponent.lowercased()
                return fileName == originalName || fileBase == baseName
            }

            guard let bestMatch = bestMatch(in: originalMatches) else { return nil }
            return ResolvedImage(url: bestMatch, source: .original, prefix: stampedPrefix(for: shot))
        }

        private func bestMatch(in matches: [URL]) -> URL? {
            matches.sorted { lhs, rhs in
                let lhsDate = (try? lhs.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate) ?? .distantPast
                let rhsDate = (try? rhs.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate) ?? .distantPast
                if lhsDate != rhsDate {
                    return lhsDate > rhsDate
                }
                return lhs.path.count < rhs.path.count
            }.first
        }

        private func stampedPrefix(for shot: SessionReportShot) -> String {
            let fileName = ScoutShotNaming.stampedJPEGFilename(
                building: shot.building,
                elevation: shot.elevation,
                detailType: shot.detailType,
                angleIndex: shot.angleIndex,
                shotKey: shot.shotKey,
                shotName: nil,
                isFlagged: shot.isFlagged == 1
            )
            return URL(fileURLWithPath: fileName).deletingPathExtension().lastPathComponent
        }

        private func normalizedFileStem(for fileName: String) -> String {
            URL(fileURLWithPath: fileName)
                .deletingPathExtension()
                .lastPathComponent
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()
        }

        private func logResolution(for shot: SessionReportShot, resolvedImage: ResolvedImage) {
            NSLog(
                "[PDFSessionReportGenerator] PDF IMG: shot_id=%@ prefix=%@ resolved=%@ source=%@",
                shot.shotID,
                resolvedImage.prefix,
                resolvedImage.url.path(percentEncoded: false),
                resolvedImage.source.rawValue
            )
        }
    }

    private static let pageSize = CGSize(width: 612, height: 792)

    private static let folderDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()

    private static let displayTimeZone = TimeZone(identifier: "America/New_York") ?? .current

    private static let displayDateTimeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = displayTimeZone
        formatter.dateFormat = "MMMM d, yyyy '•' h:mm a"
        return formatter
    }()

    private static let fileDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = displayTimeZone
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()

    private static let iso8601FormatterWithFractionalSeconds: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static func parseUTCDate(_ rawValue: String) -> Date? {
        if let date = iso8601FormatterWithFractionalSeconds.date(from: rawValue) {
            return date
        }
        return ISO8601DateFormatter().date(from: rawValue)
    }
}

private struct SessionReportSession: FetchableRecord, Decodable {
    let sessionID: String
    let orgName: String?
    let folderID: String?
    let propertyName: String?
    let propertyAddress: String?
    let propertyStreet: String?
    let propertyCity: String?
    let propertyState: String?
    let propertyZip: String?
    let startedAtUTC: String
    let endedAtUTC: String?

    enum CodingKeys: String, CodingKey {
        case sessionID = "session_id"
        case orgName = "org_name"
        case folderID = "folder_id"
        case propertyName = "property_name"
        case propertyAddress = "property_address"
        case propertyStreet = "propertyStreet"
        case propertyCity = "propertyCity"
        case propertyState = "propertyState"
        case propertyZip = "propertyZip"
        case startedAtUTC = "started_at_utc"
        case endedAtUTC = "ended_at_utc"
    }
}

private struct SessionReportShot: FetchableRecord, Decodable {
    let shotID: String
    let building: String?
    let elevation: String?
    let detailType: String?
    let angleIndex: Int?
    let shotKey: String?
    let logicalShotIdentity: String?
    let capturedAtUTC: String?
    let originalFilename: String
    let stampedJpegFilename: String?
    let isFlagged: Int
    let flaggedReason: String?

    enum CodingKeys: String, CodingKey {
        case shotID = "shot_id"
        case building
        case elevation
        case detailType = "detail_type"
        case angleIndex = "angle_index"
        case shotKey = "shot_key"
        case logicalShotIdentity = "logical_shot_identity"
        case capturedAtUTC = "captured_at_utc"
        case originalFilename = "original_filename"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case isFlagged = "is_flagged"
        case flaggedReason = "flagged_reason"
    }
}

enum PDFSessionReportError: LocalizedError {
    case databaseUnavailable
    case sessionNotFound(String)
    case pdfContextCreationFailed(String)

    var errorDescription: String? {
        switch self {
        case .databaseUnavailable:
            return "Database is unavailable."
        case .sessionNotFound(let sessionID):
            return "Session \(sessionID) was not found in the database."
        case .pdfContextCreationFailed(let path):
            return "Unable to create PDF context for \(path)."
        }
    }
}
