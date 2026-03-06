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
                    property_id,
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
                    shots.latitude,
                    shots.longitude,
                    shots.original_filename,
                    shots.stamped_jpeg_filename,
                    shots.is_flagged,
                    COALESCE(NULLIF(TRIM(shots.flagged_reason), ''), NULLIF(TRIM(issues.current_reason), '')) AS flagged_reason
                FROM shots
                LEFT JOIN issues ON issues.issue_id = shots.issue_id
                WHERE shots.session_id = ?
                ORDER BY captured_at_utc ASC, original_filename ASC
                """, arguments: [sessionID])
            let guidedRows = try SessionReportGuidedRow.fetchAll(db, sql: """
                SELECT
                    session_id,
                    building,
                    elevation,
                    detail_type,
                    angle_index,
                    status,
                    is_retired,
                    retired_at,
                    skip_reason,
                    skip_session_id
                FROM guided_rows
                WHERE session_id = ?
                ORDER BY rowid ASC
                """, arguments: [sessionID])

            return SessionReportContext(session: session, shots: shots, guidedRows: guidedRows)
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

    func generateFlaggedComparisonReport(sessionID: String, extractedFolderURL: URL, zipName: String?) throws -> URL {
        guard let dbQueue = databaseManager.dbQueue else {
            throw PDFSessionReportError.databaseUnavailable
        }

        let reportContext = try dbQueue.read { db in
            guard let session = try SessionReportSession.fetchOne(db, sql: """
                SELECT
                    session_id,
                    property_id,
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
                    shots.latitude,
                    shots.longitude,
                    shots.original_filename,
                    shots.stamped_jpeg_filename,
                    shots.is_flagged,
                    COALESCE(NULLIF(TRIM(shots.flagged_reason), ''), NULLIF(TRIM(issues.current_reason), '')) AS flagged_reason
                FROM shots
                LEFT JOIN issues ON issues.issue_id = shots.issue_id
                WHERE shots.session_id = ?
                ORDER BY captured_at_utc ASC, original_filename ASC
                """, arguments: [sessionID])
            let guidedRows = try SessionReportGuidedRow.fetchAll(db, sql: """
                SELECT
                    session_id,
                    building,
                    elevation,
                    detail_type,
                    angle_index,
                    status,
                    is_retired,
                    retired_at,
                    skip_reason,
                    skip_session_id
                FROM guided_rows
                WHERE session_id = ?
                ORDER BY rowid ASC
                """, arguments: [sessionID])

            return SessionReportContext(session: session, shots: shots, guidedRows: guidedRows)
        }

        let pdfFolder = try makePDFFolder(in: extractedFolderURL)
        let pdfURL = pdfFolder.appending(path: makeFlaggedComparisonPDFFileName(for: reportContext.session))
        let imageCatalog = ImageCatalog(sessionFolderURL: extractedFolderURL, fileManager: fileManager)
        let propertyFolderURL = extractedFolderURL.deletingLastPathComponent()

        let comparisonEntries = try dbQueue.read { db in
            var entries: [FlaggedComparisonEntry] = []
            let flaggedShots = reportContext.shots
                .filter { $0.isFlagged == 1 }
                .sorted(by: compareShotsForComparison)
            var archivedFolderCache: [String: URL] = [:]

            for currentShot in flaggedShots {
                let currentImageURL = imageCatalog.resolveImage(for: currentShot)?.url
                let previousShot = try fetchPreviousComparableShot(
                    db: db,
                    currentSession: reportContext.session,
                    currentShot: currentShot
                )
                let previousImageURL = resolvePreviousImageURL(
                    previousShot: previousShot,
                    propertyFolderURL: propertyFolderURL,
                    archivedFolderCache: &archivedFolderCache
                )
                let currentDateText = formattedUTC(currentShot.capturedAtUTC) ?? "Unknown"
                let previousDateText = previousShot.flatMap { formattedUTC($0.capturedAtUTC) } ?? "None"
                let currentMonthYear = monthYearUTC(currentShot.capturedAtUTC) ?? "Unknown"
                let previousMonthYear = previousShot.flatMap { monthYearUTC($0.capturedAtUTC) } ?? "None"

                entries.append(
                    FlaggedComparisonEntry(
                        currentShot: currentShot,
                        currentImageURL: currentImageURL,
                        previousShot: previousShot,
                        previousImageURL: previousImageURL,
                        previousMissingReason: previousShot == nil ? "NO PREVIOUS SESSION IMAGE" : nil,
                        currentDateText: currentDateText,
                        previousDateText: previousDateText,
                        currentMonthYear: currentMonthYear,
                        previousMonthYear: previousMonthYear
                    )
                )
            }

            return entries
        }

        if comparisonEntries.isEmpty {
            throw PDFSessionReportError.noFlaggedItems(sessionID)
        }

        try renderFlaggedComparisonPDF(
            context: reportContext,
            comparisonEntries: comparisonEntries,
            imageCatalog: imageCatalog,
            pdfURL: pdfURL,
            zipName: zipName
        )

        log("Flagged comparison report generated: \(pdfURL.path(percentEncoded: false))")
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

    private func makeFlaggedComparisonPDFFileName(for session: SessionReportSession) -> String {
        let propertyName = sanitizeFileNameComponent(session.propertyName ?? "Unknown Property")
        let date = Self.parseUTCDate(session.startedAtUTC) ?? Date()
        let dateString = Self.fileDateFormatter.string(from: date)
        return "Flagged Comparison Report - \(propertyName) - \(dateString).pdf"
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

        let preparedEntries = preparePhotoEntries(context: context, imageCatalog: imageCatalog)
        let photoEntries = preparedEntries.entries
        let groupedPhotoSections = makeGroupedPhotoSections(
            from: photoEntries,
            retiredNotesBySection: preparedEntries.retiredNotesBySection
        )
        let coverImage: CGImage? = {
            guard let firstEntry = photoEntries.first else { return nil }
            guard firstEntry.isSkipped == false else { return nil }
            guard resolvedAngleIndex(for: firstEntry) == 1 else { return nil }
            guard let coverImageURL = firstEntry.imageURL else { return nil }
            return loadOptimizedCGImage(at: coverImageURL)
        }()

        var pageNumber = 1
        pdfContext.beginPDFPage(nil as CFDictionary?)
        drawCoverPage(
            in: pdfContext,
            context: context,
            pageNumber: pageNumber,
            coverImage: coverImage,
            title: "Visual Property Record"
        )
        pdfContext.endPDFPage()
        pageNumber += 1

        pdfContext.beginPDFPage(nil as CFDictionary?)
        drawDocumentationScopePage(in: pdfContext, context: context, pageNumber: pageNumber)
        pdfContext.endPDFPage()
        pageNumber += 1

        var assumedIndexPageCount = 1
        var photoSectionPlans: [PhotoSectionRenderPlan] = []
        var indexPagePlans: [DocumentationIndexPagePlan] = []
        for _ in 0..<3 {
            let photoStartPage = 3 + assumedIndexPageCount
            photoSectionPlans = makePhotoSectionRenderPlans(
                from: groupedPhotoSections,
                startingPage: photoStartPage
            )
            indexPagePlans = makeDocumentationIndexPagePlans(for: photoSectionPlans)
            let resolvedCount = max(1, indexPagePlans.count)
            if resolvedCount == assumedIndexPageCount {
                break
            }
            assumedIndexPageCount = resolvedCount
        }

        if indexPagePlans.isEmpty {
            indexPagePlans = [DocumentationIndexPagePlan(pageIndex: 0, lines: [])]
        }

        for indexPlan in indexPagePlans {
            pdfContext.beginPDFPage(nil as CFDictionary?)
            drawDocumentationIndexPage(
                in: pdfContext,
                context: context,
                pageNumber: pageNumber,
                plan: indexPlan
            )
            pdfContext.endPDFPage()
            pageNumber += 1
        }

        for sectionPlan in photoSectionPlans {
            for chunk in sectionPlan.pageChunks {
                let pageEntries = chunk.entries
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
        }

        pdfContext.closePDF()
    }

    private func renderFlaggedComparisonPDF(
        context: SessionReportContext,
        comparisonEntries: [FlaggedComparisonEntry],
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

        let coverImage: CGImage? = {
            guard let firstNonFlagged = context.shots.first(where: { $0.isFlagged == 0 }),
                  let url = imageCatalog.resolveImage(for: firstNonFlagged)?.url
            else { return nil }
            return loadOptimizedCGImage(at: url)
        }()

        var pageNumber = 1
        pdfContext.beginPDFPage(nil as CFDictionary?)
        drawCoverPage(
            in: pdfContext,
            context: context,
            pageNumber: pageNumber,
            coverImage: coverImage,
            title: "Flagged Comparison Report"
        )
        pdfContext.endPDFPage()
        pageNumber += 1

        pdfContext.beginPDFPage(nil as CFDictionary?)
        drawDocumentationScopePage(in: pdfContext, context: context, pageNumber: pageNumber)
        pdfContext.endPDFPage()
        pageNumber += 1

        let indexLines = makeFlaggedComparisonIndexLines(
            comparisonEntries: comparisonEntries,
            startingPage: pageNumber + 1
        )
        var indexPlans = makeDocumentationIndexPagePlans(from: indexLines)
        if indexPlans.isEmpty {
            indexPlans = [DocumentationIndexPagePlan(pageIndex: 0, lines: [])]
        }

        for plan in indexPlans {
            pdfContext.beginPDFPage(nil as CFDictionary?)
            drawDocumentationIndexPage(
                in: pdfContext,
                context: context,
                pageNumber: pageNumber,
                plan: plan
            )
            pdfContext.endPDFPage()
            pageNumber += 1
        }

        for entry in comparisonEntries {
            pdfContext.beginPDFPage(nil as CFDictionary?)
            drawFlaggedComparisonPage(
                in: pdfContext,
                context: context,
                entry: entry,
                pageNumber: pageNumber
            )
            pdfContext.endPDFPage()
            pageNumber += 1
        }

        pdfContext.closePDF()
    }

    private func drawFlaggedComparisonPage(
        in pdfContext: CGContext,
        context: SessionReportContext,
        entry: FlaggedComparisonEntry,
        pageNumber: Int
    ) {
        let bounds = CGRect(origin: .zero, size: Self.pageSize)
        withGraphicsContext(pdfContext) {
            NSColor.white.setFill()
            bounds.fill()
            drawCenteredHeaderLogo(in: bounds)

            let outerMargin: CGFloat = 18
            let footerHeight: CGFloat = 82
            let logoHeaderReserve: CGFloat = 34
            let photoStackVerticalOffset: CGFloat = 28
            let photoCaptionGap: CGFloat = 8
            let metadataHeight: CGFloat = 66
            let slotGap: CGFloat = 2

            let contentTop = bounds.height - outerMargin - logoHeaderReserve - photoStackVerticalOffset
            let contentBottom = outerMargin + footerHeight - photoStackVerticalOffset
            let contentHeight = contentTop - contentBottom
            let slotHeight = (contentHeight - slotGap) / 2
            let slotWidth = bounds.width - (outerMargin * 2)

            for index in 0..<2 {
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

                if index == 0 {
                    drawComparisonImage(
                        imageURL: entry.currentImageURL,
                        placeholderReason: "IMAGE UNAVAILABLE",
                        in: photoAvailableRect,
                        context: pdfContext
                    )
                    drawComparisonMetadata(
                        identityLine: captionIdentityLine(for: entry.currentShot),
                        sessionLine: "Current Session: \(entry.currentDateText)",
                        flaggedReason: entry.currentShot.flaggedReason,
                        in: captionRect
                    )
                } else {
                    let placeholderReason = entry.previousMissingReason ?? "IMAGE UNAVAILABLE"
                    drawComparisonImage(
                        imageURL: entry.previousImageURL,
                        placeholderReason: placeholderReason,
                        in: photoAvailableRect,
                        context: pdfContext
                    )
                    drawComparisonMetadata(
                        identityLine: captionIdentityLine(
                            building: entry.currentShot.building,
                            elevation: entry.currentShot.elevation,
                            detailType: entry.currentShot.detailType,
                            shotKey: entry.currentShot.shotKey,
                            angleIndex: entry.currentShot.angleIndex
                        ),
                        sessionLine: "Previous Session: \(entry.previousDateText)",
                        flaggedReason: entry.previousShot?.flaggedReason,
                        in: captionRect
                    )
                }
            }

            drawAddressFooter(
                pageNumber: pageNumber,
                in: bounds,
                address: formattedAddress(for: context.session)
            )
        }
    }

    private func drawComparisonImage(
        imageURL: URL?,
        placeholderReason: String,
        in rect: CGRect,
        context: CGContext
    ) {
        if let imageURL, let image = loadOptimizedCGImage(at: imageURL) {
            let fittedRect = aspectFitRect(for: image, in: rect)
            context.saveGState()
            let clipPath = NSBezierPath(roundedRect: fittedRect, xRadius: 12, yRadius: 12)
            clipPath.addClip()
            context.draw(image, in: fittedRect)
            context.restoreGState()
            drawFlaggedBorder(around: fittedRect, in: context)
        } else {
            let placeholderRect = aspectFitRect(
                for: CGSize(width: 4, height: 3),
                in: rect
            )
            drawSkippedPlaceholder(
                reason: placeholderReason,
                in: placeholderRect,
                context: context
            )
        }
    }

    private func drawComparisonMetadata(
        identityLine: String,
        sessionLine: String,
        flaggedReason: String?,
        in rect: CGRect
    ) {
        let titleFont = NSFont.systemFont(ofSize: 11, weight: .semibold)
        let bodyFont = NSFont.systemFont(ofSize: 10, weight: .regular)
        let lineHeight: CGFloat = 14
        let topY = rect.maxY - lineHeight

        drawText(
            identityLine,
            in: CGRect(x: rect.minX, y: topY, width: rect.width, height: lineHeight),
            font: titleFont,
            alignment: .center
        )
        let trimmedReason = flaggedReason?.trimmingCharacters(in: .whitespacesAndNewlines)
        if let trimmedReason, trimmedReason.isEmpty == false {
            drawFlaggedNote(
                text: trimmedReason,
                in: CGRect(x: rect.minX, y: topY - lineHeight, width: rect.width, height: lineHeight),
                font: bodyFont,
                alignment: .center
            )
            drawText(
                sessionLine,
                in: CGRect(x: rect.minX, y: topY - (lineHeight * 2), width: rect.width, height: lineHeight),
                font: bodyFont,
                alignment: .center
            )
        } else {
            drawText(
                sessionLine,
                in: CGRect(x: rect.minX, y: topY - lineHeight, width: rect.width, height: lineHeight),
                font: bodyFont,
                alignment: .center
            )
        }
    }

    private func makeFlaggedComparisonIndexLines(
        comparisonEntries: [FlaggedComparisonEntry],
        startingPage: Int
    ) -> [DocumentationIndexLine] {
        var lines: [DocumentationIndexLine] = []
        lines.append(DocumentationIndexLine(kind: .sectionHeader, text: "Flagged Items", pageNumber: nil, isFlagged: false))

        for (index, entry) in comparisonEntries.enumerated() {
            let page = startingPage + index
            let text = "\(captionIdentityLine(for: entry.currentShot)) | \(entry.currentMonthYear) vs \(entry.previousMonthYear)"
            lines.append(
                DocumentationIndexLine(
                    kind: .photoItem,
                    text: text,
                    pageNumber: page,
                    isFlagged: true
                )
            )
        }
        return lines
    }

    private func makeDocumentationIndexPagePlans(from lines: [DocumentationIndexLine]) -> [DocumentationIndexPagePlan] {
        if lines.isEmpty { return [] }
        let leftMargin: CGFloat = 64
        let rightMargin: CGFloat = 64
        let topY: CGFloat = Self.pageSize.height - 106
        let bottomY: CGFloat = 96
        let usableWidth = Self.pageSize.width - leftMargin - rightMargin

        var pages: [DocumentationIndexPagePlan] = []
        var pageIndex = 0
        var cursorY = topY
        var placedLines: [DocumentationIndexPlacedLine] = []

        func commitPageIfNeeded() {
            if placedLines.isEmpty == false {
                pages.append(DocumentationIndexPagePlan(pageIndex: pageIndex, lines: placedLines))
                placedLines = []
            }
        }

        for line in lines {
            let lineHeight: CGFloat = (line.kind == .sectionHeader) ? 18 : 15
            let lineSpacing: CGFloat = (line.kind == .sectionHeader) ? 8 : 3

            if cursorY - lineHeight < bottomY {
                commitPageIfNeeded()
                pageIndex += 1
                cursorY = topY
            }

            let lineRect = CGRect(x: leftMargin, y: cursorY - lineHeight, width: usableWidth, height: lineHeight)
            placedLines.append(DocumentationIndexPlacedLine(line: line, rect: lineRect))
            cursorY -= (lineHeight + lineSpacing)
        }

        commitPageIfNeeded()
        return pages
    }

    private func compareShotsForComparison(_ lhs: SessionReportShot, _ rhs: SessionReportShot) -> Bool {
        let lhsKey = comparisonSortTuple(for: lhs)
        let rhsKey = comparisonSortTuple(for: rhs)
        return lhsKey.lexicographicallyPrecedes(rhsKey, by: <)
    }

    private func comparisonSortTuple(for shot: SessionReportShot) -> [String] {
        [
            friendlyBuildingLabel(displayText(shot.building, fallback: "B1")).uppercased(),
            displayText(shot.elevation, fallback: "Unknown").uppercased(),
            displayText(shot.detailType, fallback: "General Elevation").uppercased(),
            String(format: "%05d", shot.angleIndex ?? 0),
            shot.capturedAtUTC ?? "",
            shot.originalFilename,
        ]
    }

    private func fetchPreviousComparableShot(
        db: Database,
        currentSession: SessionReportSession,
        currentShot: SessionReportShot
    ) throws -> PreviousComparableShot? {
        guard let propertyID = currentSession.propertyID else { return nil }

        return try PreviousComparableShot.fetchOne(db, sql: """
            SELECT
                shots.session_id,
                shots.shot_id,
                shots.building,
                shots.elevation,
                shots.detail_type,
                shots.angle_index,
                shots.shot_key,
                shots.captured_at_utc,
                shots.original_filename,
                shots.stamped_jpeg_filename,
                COALESCE(
                    NULLIF(TRIM(shots.flagged_reason), ''),
                    NULLIF(TRIM(issues.previous_reason), ''),
                    NULLIF(TRIM(issues.current_reason), '')
                ) AS flagged_reason
            FROM shots
            JOIN sessions ON sessions.session_id = shots.session_id
            LEFT JOIN issues ON issues.issue_id = shots.issue_id
            WHERE sessions.property_id = ?
              AND sessions.started_at_utc < ?
              AND UPPER(COALESCE(shots.building, '')) = UPPER(COALESCE(?, ''))
              AND UPPER(COALESCE(shots.elevation, '')) = UPPER(COALESCE(?, ''))
              AND UPPER(COALESCE(shots.detail_type, '')) = UPPER(COALESCE(?, ''))
              AND COALESCE(shots.angle_index, -1) = COALESCE(?, -1)
            ORDER BY sessions.started_at_utc DESC, shots.captured_at_utc DESC
            LIMIT 1
            """, arguments: [
            propertyID,
            currentSession.startedAtUTC,
            currentShot.building,
            currentShot.elevation,
            currentShot.detailType,
            currentShot.angleIndex,
        ])
    }

    private func resolvePreviousImageURL(
        previousShot: PreviousComparableShot?,
        propertyFolderURL: URL,
        archivedFolderCache: inout [String: URL]
    ) -> URL? {
        guard let previousShot else { return nil }
        let sessionFolder: URL
        if let cached = archivedFolderCache[previousShot.sessionID] {
            sessionFolder = cached
        } else {
            guard let found = findArchivedSessionFolder(
                sessionID: previousShot.sessionID,
                propertyFolderURL: propertyFolderURL
            ) else {
                return nil
            }
            archivedFolderCache[previousShot.sessionID] = found
            sessionFolder = found
        }

        if let stamped = previousShot.stampedJpegFilename, stamped.isEmpty == false {
            let stampedURL = sessionFolder.appending(path: "Stamped", directoryHint: .isDirectory).appending(path: stamped)
            if fileManager.fileExists(atPath: stampedURL.path) {
                return stampedURL
            }
        }

        let originalURL = sessionFolder.appending(path: "Originals", directoryHint: .isDirectory).appending(path: previousShot.originalFilename)
        if fileManager.fileExists(atPath: originalURL.path) {
            return originalURL
        }
        return nil
    }

    private func findArchivedSessionFolder(sessionID: String, propertyFolderURL: URL) -> URL? {
        let folders = (try? fileManager.contentsOfDirectory(
            at: propertyFolderURL,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        )) ?? []

        for folder in folders {
            guard (try? folder.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true else { continue }
            let sessionsCSVURL = folder.appending(path: "sessions.csv")
            guard let data = try? Data(contentsOf: sessionsCSVURL),
                  let text = String(data: data, encoding: .utf8) ?? String(data: data, encoding: .ascii)
            else { continue }
            let lines = text.split(whereSeparator: \.isNewline).map(String.init)
            guard lines.count >= 2 else { continue }
            let header = parseCSVLine(lines[0]).map { $0.lowercased() }
            guard let sessionIDIndex = header.firstIndex(of: "session_id") else { continue }
            let values = parseCSVLine(lines[1])
            if values.indices.contains(sessionIDIndex),
               values[sessionIDIndex].trimmingCharacters(in: .whitespacesAndNewlines).caseInsensitiveCompare(sessionID) == .orderedSame {
                return folder
            }
        }

        return nil
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

    private func preparePhotoEntries(
        context: SessionReportContext,
        imageCatalog: ImageCatalog
    ) -> PreparedPhotoEntries {
        var retiredSlotKeys = Set<String>()
        var retiredNotesBySection: [String: [String]] = [:]
        var skippedRowsBySlotKey: [String: SessionReportGuidedRow] = [:]

        for guidedRow in context.guidedRows {
            let slotKey = slotKey(
                building: guidedRow.building,
                elevation: guidedRow.elevation,
                detailType: guidedRow.detailType,
                shotKey: nil,
                angleIndex: guidedRow.angleIndex
            )
            let sectionKey = sectionGroupingKey(building: guidedRow.building, elevation: guidedRow.elevation)
            let retiredInCurrentSession = guidedRow.wasRetiredDuringSession(
                sessionStartedAtUTC: context.session.startedAtUTC,
                sessionEndedAtUTC: context.session.endedAtUTC
            )
            if guidedRow.isRetired, retiredInCurrentSession {
                retiredSlotKeys.insert(slotKey)
                let retiredNote = "Retired: \(captionIdentityLine(for: guidedRow)) was retired in this session."
                if retiredNotesBySection[sectionKey]?.contains(retiredNote) != true {
                    retiredNotesBySection[sectionKey, default: []].append(retiredNote)
                }
                continue
            }

            let trimmedSkipReason = guidedRow.skipReason?.trimmingCharacters(in: .whitespacesAndNewlines)
            let hasSkipReason = trimmedSkipReason?.isEmpty == false
            let isSkipForSession = (guidedRow.skipSessionID?.trimmingCharacters(in: .whitespacesAndNewlines) ?? context.session.sessionID) == context.session.sessionID
            if hasSkipReason && isSkipForSession {
                skippedRowsBySlotKey[slotKey] = guidedRow
            }
        }

        var entries: [SessionReportPhotoEntry] = context.shots.compactMap { shot in
            let computedSlotKey = slotKey(
                building: shot.building,
                elevation: shot.elevation,
                detailType: shot.detailType,
                shotKey: shot.shotKey,
                angleIndex: shot.angleIndex
            )
            if retiredSlotKeys.contains(computedSlotKey) {
                return nil
            }
            let resolvedImage = imageCatalog.resolveImage(for: shot)
            if resolvedImage == nil {
                log("Session report warning: missing image for shot \(shot.shotID) (\(shot.originalFilename))")
            }
            return SessionReportPhotoEntry(shot: shot, imageURL: resolvedImage?.url)
        }

        let slotsWithShots = Set(entries.map { entry in
            slotKey(
                building: entry.building,
                elevation: entry.elevation,
                detailType: entry.detailType,
                shotKey: entry.shotKey,
                angleIndex: entry.angleIndex
            )
        })

        for guidedRow in context.guidedRows {
            let rowSlotKey = slotKey(
                building: guidedRow.building,
                elevation: guidedRow.elevation,
                detailType: guidedRow.detailType,
                shotKey: nil,
                angleIndex: guidedRow.angleIndex
            )
            if retiredSlotKeys.contains(rowSlotKey) {
                continue
            }

            if slotsWithShots.contains(rowSlotKey) == false,
               let skippedRow = skippedRowsBySlotKey[rowSlotKey] {
                entries.append(SessionReportPhotoEntry(skippedGuidedRow: skippedRow))
            }
        }

        entries.sort(by: comparePhotoEntries)

        return PreparedPhotoEntries(entries: entries, retiredNotesBySection: retiredNotesBySection)
    }

    private func makeGroupedPhotoSections(
        from entries: [SessionReportPhotoEntry],
        retiredNotesBySection: [String: [String]]
    ) -> [GroupedPhotoSection] {
        var orderedKeys: [String] = []
        var groupedEntries: [String: [SessionReportPhotoEntry]] = [:]
        var groupedMeta: [String: (building: String, elevation: String)] = [:]

        for entry in entries {
            let building = friendlyBuildingLabel(displayText(entry.building, fallback: "Building"))
            let elevation = displayText(entry.elevation, fallback: "Unknown")
            let key = sectionGroupingKey(building: building, elevation: elevation)

            if groupedEntries[key] == nil {
                orderedKeys.append(key)
                groupedMeta[key] = (building: building, elevation: elevation)
                groupedEntries[key] = []
            }
            groupedEntries[key, default: []].append(entry)
        }

        for key in retiredNotesBySection.keys where orderedKeys.contains(key) == false {
            orderedKeys.append(key)
            if groupedMeta[key] == nil {
                let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
                let building = parts.first ?? "Building"
                let elevation = parts.count > 1 ? parts[1] : "Unknown"
                groupedMeta[key] = (building: building, elevation: elevation)
            }
        }

        return orderedKeys.compactMap { key in
            guard let meta = groupedMeta[key] else { return nil }
            let entries = groupedEntries[key] ?? []
            let title = "\(meta.building) \(meta.elevation) Elevation"
            return GroupedPhotoSection(
                key: key,
                title: title,
                entries: entries,
                retiredNotes: retiredNotesBySection[key] ?? []
            )
        }
    }

    private func makePhotoSectionRenderPlans(
        from groupedSections: [GroupedPhotoSection],
        startingPage: Int
    ) -> [PhotoSectionRenderPlan] {
        var plans: [PhotoSectionRenderPlan] = []
        var nextPage = startingPage

        for section in groupedSections {
            var chunks: [PhotoChunkPlan] = []
            for chunkStart in stride(from: 0, to: section.entries.count, by: 2) {
                let pageEntries = Array(section.entries[chunkStart..<min(chunkStart + 2, section.entries.count)])
                chunks.append(PhotoChunkPlan(pageNumber: nextPage, entries: pageEntries))
                nextPage += 1
            }

            let startPage = chunks.first?.pageNumber ?? nextPage
            let endPage = chunks.last?.pageNumber ?? startPage
            plans.append(
                PhotoSectionRenderPlan(
                    key: section.key,
                    title: section.title,
                    entries: section.entries,
                    retiredNotes: section.retiredNotes,
                    pageChunks: chunks,
                    startPage: startPage,
                    endPage: endPage
                )
            )
        }

        return plans
    }

    private func makeDocumentationIndexPagePlans(
        for sectionPlans: [PhotoSectionRenderPlan]
    ) -> [DocumentationIndexPagePlan] {
        let lines = makeDocumentationIndexLines(for: sectionPlans)
        if lines.isEmpty {
            return []
        }

        let leftMargin: CGFloat = 64
        let rightMargin: CGFloat = 64
        let topY: CGFloat = Self.pageSize.height - 106
        let bottomY: CGFloat = 96
        let usableWidth = Self.pageSize.width - leftMargin - rightMargin

        var pages: [DocumentationIndexPagePlan] = []
        var pageIndex = 0
        var cursorY = topY
        var placedLines: [DocumentationIndexPlacedLine] = []

        func commitPageIfNeeded() {
            if placedLines.isEmpty == false {
                pages.append(DocumentationIndexPagePlan(pageIndex: pageIndex, lines: placedLines))
                placedLines = []
            }
        }

        for line in lines {
            let lineHeight: CGFloat
            let lineSpacing: CGFloat
            switch line.kind {
            case .sectionHeader:
                lineHeight = 18
                lineSpacing = 8
            case .photoItem:
                lineHeight = 15
                lineSpacing = 3
            case .retiredSpacer:
                lineHeight = 6
                lineSpacing = 6
            case .retiredNote:
                lineHeight = 15
                lineSpacing = 2
            }

            if cursorY - lineHeight < bottomY {
                commitPageIfNeeded()
                pageIndex += 1
                cursorY = topY
            }

            let lineRect = CGRect(x: leftMargin, y: cursorY - lineHeight, width: usableWidth, height: lineHeight)
            placedLines.append(DocumentationIndexPlacedLine(line: line, rect: lineRect))
            cursorY -= (lineHeight + lineSpacing)
        }

        commitPageIfNeeded()
        return pages
    }

    private func makeDocumentationIndexLines(for sectionPlans: [PhotoSectionRenderPlan]) -> [DocumentationIndexLine] {
        var lines: [DocumentationIndexLine] = []
        for section in sectionPlans {
            let headerText: String
            if section.entries.isEmpty {
                headerText = section.title
            } else {
                headerText = "\(section.title) pages \(section.startPage)-\(section.endPage)"
            }
            lines.append(DocumentationIndexLine(
                kind: .sectionHeader,
                text: headerText,
                pageNumber: nil,
                isFlagged: false
            ))
            for (index, entry) in section.entries.enumerated() {
                let photoPageNumber = section.startPage + (index / 2)
                let caption = captionIdentityLine(for: entry)
                lines.append(DocumentationIndexLine(
                    kind: .photoItem,
                    text: caption,
                    pageNumber: photoPageNumber,
                    isFlagged: entry.isFlagged
                ))
            }

            if section.retiredNotes.isEmpty == false {
                lines.append(DocumentationIndexLine(
                    kind: .retiredSpacer,
                    text: "",
                    pageNumber: nil,
                    isFlagged: false
                ))
                for retiredNote in section.retiredNotes {
                    lines.append(DocumentationIndexLine(
                        kind: .retiredNote,
                        text: retiredNote,
                        pageNumber: nil,
                        isFlagged: false
                    ))
                }
            }
        }
        return lines
    }

    private func drawCoverPage(
        in pdfContext: CGContext,
        context: SessionReportContext,
        pageNumber: Int,
        coverImage: CGImage?,
        title: String
    ) {
        let bounds = CGRect(origin: .zero, size: Self.pageSize)
        withGraphicsContext(pdfContext) {
            NSColor.white.setFill()
            bounds.fill()

            let session = context.session
            let horizontalMargin: CGFloat = 72

            if let logoImage = loadReportLogoImage() {
                let logoContainer = CGRect(
                    x: horizontalMargin,
                    y: bounds.height - 160,
                    width: bounds.width - (horizontalMargin * 2),
                    height: 80
                )
                drawAspectFit(image: logoImage, in: logoContainer)
            } else {
                drawText(
                    "SCOUT",
                    in: CGRect(x: horizontalMargin, y: bounds.height - 150, width: bounds.width - (horizontalMargin * 2), height: 80),
                    font: .systemFont(ofSize: 66, weight: .bold),
                    color: .black,
                    alignment: .center
                )
            }

            let imageRect = CGRect(
                x: 126,
                y: bounds.height - 360,
                width: bounds.width - 252,
                height: 182
            )
            if let coverImage {
                let fitted = aspectFitRect(for: coverImage, in: imageRect)
                pdfContext.saveGState()
                let clipPath = NSBezierPath(roundedRect: fitted, xRadius: 12, yRadius: 12)
                clipPath.addClip()
                pdfContext.draw(coverImage, in: fitted)
                pdfContext.restoreGState()
            } else {
                NSColor.white.setFill()
                imageRect.fill()
            }

            drawText(
                title,
                in: CGRect(x: horizontalMargin, y: imageRect.minY - 54, width: bounds.width - (horizontalMargin * 2), height: 28),
                font: .systemFont(ofSize: 20, weight: .bold),
                color: .black,
                alignment: .center
            )

            let shotDates = context.shots.compactMap { shot -> Date? in
                guard let raw = shot.capturedAtUTC else { return nil }
                return Self.parseUTCDate(raw)
            }
            let firstShotDate = shotDates.first ?? Self.parseUTCDate(session.startedAtUTC)
            let lastShotDate = shotDates.last ?? Self.parseUTCDate(session.endedAtUTC ?? session.startedAtUTC)
            let weatherAnchorShot = context.shots.first { shot in
                shot.latitude != nil && shot.longitude != nil
            }
            let weatherSummary: String? = {
                guard let weatherAnchorShot,
                      let latitude = weatherAnchorShot.latitude,
                      let longitude = weatherAnchorShot.longitude,
                      let targetDate = firstShotDate else {
                    return nil
                }
                return fetchOpenMeteoWeatherSummary(
                    latitude: latitude,
                    longitude: longitude,
                    at: targetDate
                )
            }()
            let dateOfService = firstShotDate.map { Self.shortDateFormatter.string(from: $0) } ?? "Unknown"
            let timeWindow: String = {
                guard let firstShotDate else { return "Unknown" }
                let firstTime = Self.shortTimeFormatter.string(from: firstShotDate)
                guard let lastShotDate else { return firstTime }
                let lastTime = Self.shortTimeFormatter.string(from: lastShotDate)
                return firstTime == lastTime ? firstTime : "\(firstTime) to \(lastTime)"
            }()
            let reportDate = Self.shortDateFormatter.string(from: Date())

            let baseDetails: [(label: String?, value: String, isItalic: Bool)] = [
                ("Property Name:", session.propertyName ?? "Unknown Property", false),
                ("Property Address:", formattedAddress(for: session), false),
                ("Date of Service:", dateOfService, false),
                ("Time Window:", timeWindow, false),
                ("Report Reference ID:", session.sessionID, false),
                (nil, "", false),
                ("Prepared by:", "", false),
                (nil, "SCOUT - Visual Documentation Services", false),
                (nil, "Clear, time-stamped visual documentation of observable property conditions.", false),
                ("Report Date:", reportDate, false),
            ]
            var details = baseDetails
            if let weatherSummary {
                details.insert(("Weather at service start:", weatherSummary, false), at: 4)
                details.insert((nil, "Weather data provided by Open-Meteo.", true), at: 5)
            }
            details.insert((nil, "*Weather conditions may have affected visibility at the time of documentation", true), at: min(5, details.count))

            var lineY = imageRect.minY - 96
            for line in details {
                let lineRect = CGRect(x: horizontalMargin, y: lineY, width: bounds.width - (horizontalMargin * 2), height: 16)
                if line.isItalic {
                    drawText(
                        line.value,
                        in: lineRect,
                        font: NSFontManager.shared.convert(.systemFont(ofSize: 10), toHaveTrait: .italicFontMask),
                        color: .black,
                        alignment: .center
                    )
                } else if let label = line.label {
                    drawCenteredLabeledLine(
                        label: label,
                        value: line.value,
                        in: lineRect
                    )
                } else {
                    drawText(
                        line.value,
                        in: lineRect,
                        font: .systemFont(ofSize: 11, weight: .regular),
                        color: .black,
                        alignment: .center
                    )
                }
                if line.label == "Prepared by:" {
                    lineY -= 8
                }
                lineY -= (line.value.isEmpty && line.label == nil) ? 12 : 18
            }

            drawFooter(pageNumber: pageNumber, in: bounds)
        }
    }

    private func drawDocumentationScopePage(
        in pdfContext: CGContext,
        context: SessionReportContext,
        pageNumber: Int
    ) {
        let bounds = CGRect(origin: .zero, size: Self.pageSize)
        withGraphicsContext(pdfContext) {
            NSColor.white.setFill()
            bounds.fill()
            drawCenteredHeaderLogo(in: bounds)

            let leftMargin: CGFloat = 64
            let rightMargin: CGFloat = 64
            let contentWidth = bounds.width - leftMargin - rightMargin

            let sessionAddress = formattedAddress(for: context.session)

            let bodyRect = CGRect(
                x: leftMargin,
                y: 112,
                width: contentWidth,
                height: bounds.height - 214
            )
            drawDocumentationScopeBody(in: bodyRect)

            drawAddressFooter(
                pageNumber: pageNumber,
                in: bounds,
                address: sessionAddress
            )
        }
    }

    private func drawDocumentationIndexPage(
        in pdfContext: CGContext,
        context: SessionReportContext,
        pageNumber: Int,
        plan: DocumentationIndexPagePlan
    ) {
        let bounds = CGRect(origin: .zero, size: Self.pageSize)
        withGraphicsContext(pdfContext) {
            NSColor.white.setFill()
            bounds.fill()
            drawCenteredHeaderLogo(in: bounds)

            drawText(
                "Documentation Index",
                in: CGRect(x: 64, y: bounds.height - 100, width: bounds.width - 128, height: 24),
                font: .systemFont(ofSize: 24, weight: .bold),
                color: .black,
                alignment: .left
            )

            if plan.lines.isEmpty {
                drawText(
                    "No photos available for index.",
                    in: CGRect(x: 64, y: bounds.height - 130, width: bounds.width - 128, height: 16),
                    font: .systemFont(ofSize: 11, weight: .regular),
                    color: .black,
                    alignment: .left
                )
            } else {
                for placed in plan.lines {
                    switch placed.line.kind {
                    case .sectionHeader:
                        drawText(
                            placed.line.text,
                            in: placed.rect,
                            font: .systemFont(ofSize: 11, weight: .semibold),
                            color: .black,
                            alignment: .left
                        )
                    case .photoItem:
                        let pageRect = CGRect(
                            x: placed.rect.maxX - 34,
                            y: placed.rect.minY,
                            width: 34,
                            height: placed.rect.height
                        )
                        let textRect = CGRect(
                            x: placed.rect.minX,
                            y: placed.rect.minY,
                            width: placed.rect.width - 44,
                            height: placed.rect.height
                        )
                        if placed.line.isFlagged {
                            drawFlaggedNote(
                                text: placed.line.text,
                                in: textRect,
                                font: .systemFont(ofSize: 9, weight: .regular),
                                alignment: .left
                            )
                        } else {
                            drawText(
                                placed.line.text,
                                in: textRect,
                                font: .systemFont(ofSize: 9, weight: .regular),
                                color: .black,
                                alignment: .left
                            )
                        }

                        if placed.line.pageNumber != nil {
                            let leaderY = textRect.midY
                            pdfContext.saveGState()
                            pdfContext.setStrokeColor(NSColor.black.withAlphaComponent(0.35).cgColor)
                            pdfContext.setLineWidth(0.6)
                            pdfContext.setLineDash(phase: 0, lengths: [1.5, 2.5])
                            pdfContext.move(to: CGPoint(x: textRect.maxX + 2, y: leaderY))
                            pdfContext.addLine(to: CGPoint(x: pageRect.minX - 4, y: leaderY))
                            pdfContext.strokePath()
                            pdfContext.restoreGState()
                        }

                        if let pageNumber = placed.line.pageNumber {
                            drawText(
                                "\(pageNumber)",
                                in: pageRect,
                                font: .systemFont(ofSize: 9, weight: .regular),
                                color: .black,
                                alignment: .right
                            )
                        }
                    case .retiredSpacer:
                        break
                    case .retiredNote:
                        drawText(
                            placed.line.text,
                            in: placed.rect,
                            font: .systemFont(ofSize: 9, weight: .semibold),
                            color: .black,
                            alignment: .left
                        )
                    }
                }
            }

            drawAddressFooter(
                pageNumber: pageNumber,
                in: bounds,
                address: formattedAddress(for: context.session)
            )
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
            drawCenteredHeaderLogo(in: bounds)

            let outerMargin: CGFloat = 18
            let footerHeight: CGFloat = 82
            let headerHeight: CGFloat = includeHeader ? 20 : 0
            let logoHeaderReserve: CGFloat = 34
            let photoStackVerticalOffset: CGFloat = 28
            let photoCaptionGap: CGFloat = 8
            let metadataHeight: CGFloat = 66
            let slotGap: CGFloat = 2

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
                    color: .black
                )
            }

            let contentTop = bounds.height - outerMargin - headerHeight - logoHeaderReserve - photoStackVerticalOffset
            let contentBottom = outerMargin + footerHeight - photoStackVerticalOffset
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

                if entry.isSkipped {
                    let placeholderRect = aspectFitRect(
                        for: CGSize(width: 4, height: 3),
                        in: photoAvailableRect
                    )
                    drawSkippedPlaceholder(
                        reason: entry.skipReason ?? "Skipped",
                        in: placeholderRect,
                        context: pdfContext
                    )
                } else {
                    if let imageURL = entry.imageURL, let image = loadOptimizedCGImage(at: imageURL) {
                        let fittedRect = aspectFitRect(for: image, in: photoAvailableRect)
                        pdfContext.saveGState()
                        let clipPath = NSBezierPath(roundedRect: fittedRect, xRadius: 12, yRadius: 12)
                        clipPath.addClip()
                        pdfContext.draw(image, in: fittedRect)
                        pdfContext.restoreGState()
                        if entry.isFlagged {
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
                            color: .black,
                            alignment: .center,
                            verticalCenter: true
                        )
                    }
                }

                drawMetadataBlock(for: entry, in: captionRect)
            }

            drawAddressFooter(
                pageNumber: pageNumber,
                in: bounds,
                address: formattedAddress(for: context.session)
            )
        }
    }

    private func drawFooter(pageNumber: Int, in bounds: CGRect) {
        let contentMargin: CGFloat = 64
        drawText(
            "Page \(pageNumber)",
            in: CGRect(x: 0, y: 10, width: bounds.width - contentMargin, height: 12),
            font: .systemFont(ofSize: 10, weight: .regular),
            color: .black,
            alignment: .right
        )
    }

    private func drawAddressFooter(pageNumber: Int, in bounds: CGRect, address: String) {
        let contentMargin: CGFloat = 64
        _ = drawWrappedText(
            "This visual property record provides visual documentation only. It does not constitute an inspection, assessment, certification, or determination of condition, safety, or compliance. Documentation reflects only what was visually captured at the time of the site visit.",
            in: CGRect(x: contentMargin, y: 24, width: bounds.width - (contentMargin * 2), height: 40),
            font: .systemFont(ofSize: 8, weight: .regular),
            color: .black
        )
        drawText(
            address,
            in: CGRect(x: contentMargin, y: 10, width: bounds.width - (contentMargin * 2), height: 12),
            font: .systemFont(ofSize: 9, weight: .regular),
            color: .black,
            alignment: .left
        )
        drawText(
            "Page \(pageNumber)",
            in: CGRect(x: 0, y: 10, width: bounds.width - contentMargin, height: 12),
            font: .systemFont(ofSize: 10, weight: .regular),
            color: .black,
            alignment: .right
        )
    }

    private func drawSkippedPlaceholder(reason: String, in rect: CGRect, context: CGContext) {
        context.saveGState()
        context.setFillColor(NSColor.white.cgColor)
        let fillPath = CGPath(roundedRect: rect, cornerWidth: 12, cornerHeight: 12, transform: nil)
        context.addPath(fillPath)
        context.fillPath()
        context.restoreGState()

        context.saveGState()
        context.setStrokeColor(NSColor.black.cgColor)
        context.setLineWidth(3)
        let path = CGPath(roundedRect: rect, cornerWidth: 12, cornerHeight: 12, transform: nil)
        context.addPath(path)
        context.strokePath()
        context.restoreGState()

        drawCenteredPlaceholderReason(reason, in: rect.insetBy(dx: 16, dy: 16))
    }

    private func drawCenteredPlaceholderReason(_ reason: String, in rect: CGRect) {
        let text = formatSkipReasonDisplay(reason)
        let stackedText = text
            .split(separator: " ")
            .map(String.init)
            .joined(separator: "\n")
        let font = NSFont.systemFont(ofSize: 14, weight: .semibold)
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.lineBreakMode = .byWordWrapping
        let attributes: [NSAttributedString.Key: Any] = [
            .font: font,
            .foregroundColor: NSColor.black,
            .paragraphStyle: paragraph,
        ]
        let measured = (stackedText as NSString).boundingRect(
            with: CGSize(width: rect.width, height: .greatestFiniteMagnitude),
            options: [.usesLineFragmentOrigin, .usesFontLeading],
            attributes: attributes
        )
        let drawRect = CGRect(
            x: rect.minX,
            y: rect.midY - (ceil(measured.height) / 2),
            width: rect.width,
            height: ceil(measured.height)
        )
        (stackedText as NSString).draw(in: drawRect, withAttributes: attributes)
    }

    private func formatSkipReasonDisplay(_ raw: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        let withBasicSeparators = trimmed
            .replacingOccurrences(of: "_", with: " ")
            .replacingOccurrences(of: "-", with: " ")

        // Break camelCase / PascalCase / alpha-numeric boundaries into separate words.
        let withWordBoundaries = withBasicSeparators
            .replacingOccurrences(
                of: "(?<=[a-z])(?=[A-Z])",
                with: " ",
                options: .regularExpression
            )
            .replacingOccurrences(
                of: "(?<=[A-Za-z])(?=\\d)",
                with: " ",
                options: .regularExpression
            )
            .replacingOccurrences(
                of: "(?<=\\d)(?=[A-Za-z])",
                with: " ",
                options: .regularExpression
            )

        let compact = withWordBoundaries
            .components(separatedBy: .whitespacesAndNewlines)
            .filter { $0.isEmpty == false }
            .joined(separator: " ")
        return compact.uppercased()
    }

    private func drawMetadataBlock(for entry: SessionReportPhotoEntry, in rect: CGRect) {
        let titleFont = NSFont.systemFont(ofSize: 11, weight: .semibold)
        let bodyFont = NSFont.systemFont(ofSize: 10, weight: .regular)
        let lineHeight: CGFloat = 14
        let topY = rect.maxY - lineHeight

        let identityLine = captionIdentityLine(for: entry)
        let secondLineY = topY - lineHeight
        let thirdLineY = topY - (lineHeight * 2)

        drawText(
            identityLine,
            in: CGRect(x: rect.minX, y: topY, width: rect.width, height: lineHeight),
            font: titleFont,
            alignment: .center
        )

        let capturedAt = entry.isSkipped ? " " : (formattedUTC(entry.capturedAtUTC) ?? "Unknown")

        if entry.isFlagged {
            let flaggedReason = entry.flaggedReason?.trimmingCharacters(in: .whitespacesAndNewlines)
            let noteText = (flaggedReason?.isEmpty == false) ? flaggedReason! : "Flagged"
            drawFlaggedNote(
                text: noteText,
                in: CGRect(x: rect.minX, y: secondLineY, width: rect.width, height: lineHeight),
                font: bodyFont,
                alignment: .center
            )
            drawText(
                capturedAt,
                in: CGRect(x: rect.minX, y: thirdLineY, width: rect.width, height: lineHeight),
                font: bodyFont,
                color: .black,
                alignment: .center
            )
        } else {
            drawText(
                capturedAt,
                in: CGRect(x: rect.minX, y: secondLineY, width: rect.width, height: lineHeight),
                font: bodyFont,
                color: .black,
                alignment: .center
            )
        }
    }

    private func captionIdentityLine(for shot: SessionReportShot) -> String {
        captionIdentityLine(
            building: shot.building,
            elevation: shot.elevation,
            detailType: shot.detailType,
            shotKey: shot.shotKey,
            angleIndex: shot.angleIndex
        )
    }

    private func captionIdentityLine(for guidedRow: SessionReportGuidedRow) -> String {
        captionIdentityLine(
            building: guidedRow.building,
            elevation: guidedRow.elevation,
            detailType: guidedRow.detailType,
            shotKey: nil,
            angleIndex: guidedRow.angleIndex
        )
    }

    private func captionIdentityLine(for entry: SessionReportPhotoEntry) -> String {
        captionIdentityLine(
            building: entry.building,
            elevation: entry.elevation,
            detailType: entry.detailType,
            shotKey: entry.shotKey,
            angleIndex: entry.angleIndex
        )
    }

    private func captionIdentityLine(
        building: String?,
        elevation: String?,
        detailType: String?,
        shotKey: String?,
        angleIndex: Int?
    ) -> String {
        let angleIdentifier = detailIdentifier(shotKey: shotKey, angleIndex: angleIndex)
        return [
            friendlyBuildingLabel(displayText(building, fallback: "B1")),
            displayText(elevation, fallback: "North"),
            displayText(detailType, fallback: "General Elevation"),
            friendlyAngleLabel(angleIdentifier),
        ]
        .joined(separator: " | ")
        .uppercased()
    }

    private func sectionGroupingKey(building: String?, elevation: String?) -> String {
        let normalizedBuilding = friendlyBuildingLabel(displayText(building, fallback: "Building")).uppercased()
        let normalizedElevation = displayText(elevation, fallback: "Unknown").uppercased()
        return "\(normalizedBuilding)|\(normalizedElevation)"
    }

    private func slotKey(
        building: String?,
        elevation: String?,
        detailType: String?,
        shotKey: String?,
        angleIndex: Int?
    ) -> String {
        [
            displayText(building, fallback: "B1").uppercased(),
            displayText(elevation, fallback: "North").uppercased(),
            displayText(detailType, fallback: "General Elevation").uppercased(),
            detailIdentifier(shotKey: shotKey, angleIndex: angleIndex).uppercased(),
        ].joined(separator: "|")
    }

    private func comparePhotoEntries(_ lhs: SessionReportPhotoEntry, _ rhs: SessionReportPhotoEntry) -> Bool {
        let lhsBuilding = buildingSortKey(lhs.building)
        let rhsBuilding = buildingSortKey(rhs.building)
        if lhsBuilding.sortText != rhsBuilding.sortText {
            return lhsBuilding.sortText < rhsBuilding.sortText
        }
        if lhsBuilding.number != rhsBuilding.number {
            return lhsBuilding.number < rhsBuilding.number
        }

        let lhsElevation = displayText(lhs.elevation, fallback: "Unknown").uppercased()
        let rhsElevation = displayText(rhs.elevation, fallback: "Unknown").uppercased()
        if lhsElevation != rhsElevation {
            return lhsElevation < rhsElevation
        }

        let lhsDetail = displayText(lhs.detailType, fallback: "General Elevation").uppercased()
        let rhsDetail = displayText(rhs.detailType, fallback: "General Elevation").uppercased()
        if lhsDetail != rhsDetail {
            return lhsDetail < rhsDetail
        }

        let lhsAngle = resolvedAngleIndex(for: lhs)
        let rhsAngle = resolvedAngleIndex(for: rhs)
        if lhsAngle != rhsAngle {
            return lhsAngle < rhsAngle
        }

        if lhs.isSkipped != rhs.isSkipped {
            return lhs.isSkipped == false
        }

        let lhsCaptured = parseCapturedDate(lhs.capturedAtUTC)
        let rhsCaptured = parseCapturedDate(rhs.capturedAtUTC)
        if lhsCaptured != rhsCaptured {
            return lhsCaptured < rhsCaptured
        }

        return lhs.originalFilename.localizedCaseInsensitiveCompare(rhs.originalFilename) == .orderedAscending
    }

    private func resolvedAngleIndex(for entry: SessionReportPhotoEntry) -> Int {
        if let angleIndex = entry.angleIndex {
            return angleIndex
        }
        let detail = detailIdentifier(shotKey: entry.shotKey, angleIndex: nil)
        if let number = numericSuffix(forPrefix: "A", in: detail), let intValue = Int(number) {
            return intValue
        }
        return 0
    }

    private func buildingSortKey(_ value: String?) -> (sortText: String, number: Int) {
        let normalized = displayText(value, fallback: "B0").uppercased()
        if let number = numericSuffix(forPrefix: "B", in: normalized), let intValue = Int(number) {
            return ("B", intValue)
        }
        return (normalized, 0)
    }

    private func parseCapturedDate(_ raw: String?) -> Date {
        guard let raw else { return .distantFuture }
        return Self.parseUTCDate(raw) ?? .distantFuture
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

    private func drawFlaggedNote(
        text: String,
        in rect: CGRect,
        font: NSFont,
        alignment: NSTextAlignment
    ) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = alignment
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
                .foregroundColor: NSColor.black,
                .paragraphStyle: paragraph,
            ]
        ))
        note.draw(in: rect)
    }

    private func drawCenteredHeaderLogo(in bounds: CGRect) {
        guard let logoImage = loadReportLogoImage() else { return }
        let width: CGFloat = 170
        let height: CGFloat = 34
        let rect = CGRect(
            x: (bounds.width - width) / 2,
            y: bounds.height - 56,
            width: width,
            height: height
        )
        drawAspectFit(image: logoImage, in: rect)
    }

    private func displayShotLabel(for shot: SessionReportShot) -> String {
        let trimmedShotKey = shot.shotKey?.trimmingCharacters(in: .whitespacesAndNewlines)
        if let trimmedShotKey, trimmedShotKey.isEmpty == false {
            return trimmedShotKey
        }
        return "\(displayText(shot.detailType, fallback: "Shot")) A\(shot.angleIndex ?? 0)"
    }

    private func detailIdentifier(for shot: SessionReportShot) -> String {
        detailIdentifier(shotKey: shot.shotKey, angleIndex: shot.angleIndex)
    }

    private func detailIdentifier(shotKey: String?, angleIndex: Int?) -> String {
        let trimmedShotKey = shotKey?.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        if let trimmedShotKey, trimmedShotKey.range(of: #"^A\d+$"#, options: .regularExpression) != nil {
            return trimmedShotKey
        }
        return "A\(angleIndex ?? 0)"
    }

    private func friendlyBuildingLabel(_ value: String) -> String {
        if let number = numericSuffix(forPrefix: "B", in: value) {
            return "Building \(number)"
        }
        return value
    }

    private func friendlyAngleLabel(_ value: String) -> String {
        if let number = numericSuffix(forPrefix: "A", in: value) {
            return "Angle \(number)"
        }
        return value
    }

    private func numericSuffix(forPrefix prefix: String, in value: String) -> String? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        let pattern = "^\(NSRegularExpression.escapedPattern(for: prefix))\\s*(\\d+)$"
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return nil
        }
        let fullRange = NSRange(trimmed.startIndex..<trimmed.endIndex, in: trimmed)
        guard let match = regex.firstMatch(in: trimmed, options: [], range: fullRange),
              match.numberOfRanges > 1,
              let numberRange = Range(match.range(at: 1), in: trimmed) else {
            return nil
        }
        return String(trimmed[numberRange])
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
        return aspectFitRect(for: imageSize, in: container)
    }

    private func aspectFitRect(for imageSize: CGSize, in container: CGRect) -> CGRect {
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
            drawRect = drawRect.insetBy(dx: 0, dy: -1)
        } else {
            drawRect = CGRect(
                x: rect.minX,
                y: rect.minY - 1,
                width: rect.width,
                height: rect.height + 2
            )
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

    private func loadReportLogoImage() -> NSImage? {
        NSImage(named: "ScoutLogoBlue")
            ?? NSImage(named: "ScoutProcessLogoBlue")
            ?? NSImage(named: "ScoutProcessLogoWhite")
            ?? NSImage(named: "ScoutLogoWhite")
    }

    private func drawAspectFit(image: NSImage, in container: CGRect) {
        guard image.size.width > 0, image.size.height > 0 else {
            image.draw(in: container)
            return
        }
        let scale = min(container.width / image.size.width, container.height / image.size.height)
        let size = CGSize(width: image.size.width * scale, height: image.size.height * scale)
        let rect = CGRect(
            x: container.minX + (container.width - size.width) / 2,
            y: container.minY + (container.height - size.height) / 2,
            width: size.width,
            height: size.height
        )
        image.draw(in: rect)
    }

    private func drawCenteredLabeledLine(label: String, value: String, in rect: CGRect) {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .left
        paragraph.lineBreakMode = .byTruncatingTail

        let labelAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 11, weight: .semibold),
            .foregroundColor: NSColor.black,
            .paragraphStyle: paragraph,
        ]
        let valueAttributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 11, weight: .regular),
            .foregroundColor: NSColor.black,
            .paragraphStyle: paragraph,
        ]

        let labelSize = (label as NSString).size(withAttributes: labelAttributes)
        let valueSize = (value as NSString).size(withAttributes: valueAttributes)
        let totalWidth = ceil(labelSize.width + (value.isEmpty ? 0 : 4) + valueSize.width)
        let startX = rect.minX + max(0, (rect.width - totalWidth) / 2)
        let textRect = CGRect(x: rect.minX, y: rect.minY - 1, width: rect.width, height: rect.height + 2)

        (label as NSString).draw(
            in: CGRect(x: startX, y: textRect.minY, width: ceil(labelSize.width), height: textRect.height),
            withAttributes: labelAttributes
        )
        if value.isEmpty == false {
            (value as NSString).draw(
                in: CGRect(x: startX + ceil(labelSize.width) + 4, y: textRect.minY, width: rect.maxX - startX, height: textRect.height),
                withAttributes: valueAttributes
            )
        }
    }

    private func drawDocumentationScopeBody(in rect: CGRect) {
        let titleColor = NSColor.black
        let bodyColor = NSColor.black
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .left
        paragraph.lineBreakMode = .byWordWrapping
        paragraph.lineSpacing = 1

        let body = NSMutableAttributedString()
        func append(_ text: String, font: NSFont) {
            body.append(NSAttributedString(
                string: text,
                attributes: [
                    .font: font,
                    .foregroundColor: bodyColor,
                    .paragraphStyle: paragraph,
                ]
            ))
        }
        func appendHeader(_ text: String) {
            body.append(NSAttributedString(
                string: text,
                attributes: [
                    .font: NSFont.systemFont(ofSize: 19, weight: .bold),
                    .foregroundColor: titleColor,
                    .paragraphStyle: paragraph,
                ]
            ))
        }

        appendHeader("Documentation Scope\n")
        append("SCOUT documents observable property features as they appear at the time of service. Deliverables consist of time-stamped photographs and structured notes intended for reference and visual comparison over time.\n\n", font: .systemFont(ofSize: 11, weight: .regular))
        appendHeader("Inclusions\n")
        append("•  Visual documentation of accessible exterior areas\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Visual documentation of accessible interior common areas (if applicable)\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Time-stamped photographs organized by area or elevation\n\n", font: .systemFont(ofSize: 11, weight: .regular))
        appendHeader("Exclusions\n")
        append("This report does not include:\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Inspections, evaluations, or professional assessments\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Engineering, architectural, or code compliance analysis\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Testing, probing, monitoring, or measurements\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Identification of concealed or latent conditions\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Opinions regarding cause, severity, responsibility, or repair methods\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("•  Cost estimates, pricing, or scope recommendations\n\n", font: .systemFont(ofSize: 11, weight: .regular))
        appendHeader("Limitations\n")
        append("Documentation was limited by accessibility, visibility, weather conditions, lighting, and site conditions present at the time of service.\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("Location-based descriptive notes are limited to factual identification of visually observable conditions only.\n", font: .systemFont(ofSize: 11, weight: .regular))
        append("This record reflects conditions observed only at the documented date and time. No ongoing monitoring or updates are implied.\n\n", font: .systemFont(ofSize: 11, weight: .regular))
        appendHeader("Use of record\n")
        append("This report is intended for documentation and recordkeeping purposes only. It is not suitable for design, construction planning, engineering evaluation, or regulatory compliance.\n", font: .systemFont(ofSize: 10.5, weight: .regular))
        append("Use of this report is limited to the client identified on Page 1 unless otherwise authorized in writing by SCOUT.", font: .systemFont(ofSize: 10.5, weight: .regular))

        body.draw(in: rect)
    }

    @discardableResult
    private func drawWrappedText(
        _ text: String,
        in rect: CGRect,
        font: NSFont,
        color: NSColor = .black,
        lineHeightMultiple: CGFloat = 1.1
    ) -> CGFloat {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .left
        paragraph.lineBreakMode = .byWordWrapping
        paragraph.lineHeightMultiple = lineHeightMultiple

        let attributes: [NSAttributedString.Key: Any] = [
            .font: font,
            .foregroundColor: color,
            .paragraphStyle: paragraph,
        ]
        let attributed = NSAttributedString(string: text, attributes: attributes)
        let measured = attributed.boundingRect(
            with: CGSize(width: rect.width, height: .greatestFiniteMagnitude),
            options: [.usesLineFragmentOrigin, .usesFontLeading]
        )
        let height = ceil(measured.height) + 2
        let drawRect = CGRect(x: rect.minX, y: rect.maxY - height - 1, width: rect.width, height: height)
        attributed.draw(in: drawRect)
        return drawRect.minY
    }

    @discardableResult
    private func drawBulletList(
        _ items: [String],
        in rect: CGRect,
        font: NSFont,
        color: NSColor = .black
    ) -> CGFloat {
        var cursorY = rect.maxY
        for item in items {
            let bulletX = rect.minX + 3
            let textX = rect.minX + 16
            drawText(
                "•",
                in: CGRect(x: bulletX, y: cursorY - 14, width: 10, height: 14),
                font: font,
                color: color
            )
            cursorY = drawWrappedText(
                item,
                in: CGRect(x: textX, y: 0, width: rect.width - 16, height: cursorY),
                font: font,
                color: color
            ) - 4
        }
        return cursorY
    }

    private func fetchOpenMeteoWeatherSummary(
        latitude: Double,
        longitude: Double,
        at targetDate: Date
    ) -> String? {
        let day = Self.fileDateFormatter.string(from: targetDate)
        var components = URLComponents(string: "https://archive-api.open-meteo.com/v1/archive")
        components?.queryItems = [
            URLQueryItem(name: "latitude", value: String(format: "%.6f", latitude)),
            URLQueryItem(name: "longitude", value: String(format: "%.6f", longitude)),
            URLQueryItem(name: "start_date", value: day),
            URLQueryItem(name: "end_date", value: day),
            URLQueryItem(name: "hourly", value: "temperature_2m,weather_code,wind_speed_10m,precipitation"),
            URLQueryItem(name: "temperature_unit", value: "fahrenheit"),
            URLQueryItem(name: "wind_speed_unit", value: "mph"),
            URLQueryItem(name: "precipitation_unit", value: "inch"),
            URLQueryItem(name: "timezone", value: "America/New_York"),
        ]
        guard let url = components?.url else { return nil }

        let config = URLSessionConfiguration.ephemeral
        config.timeoutIntervalForRequest = 8
        config.timeoutIntervalForResource = 8
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }

        let semaphore = DispatchSemaphore(value: 0)
        var responseData: Data?
        session.dataTask(with: url) { data, _, _ in
            responseData = data
            semaphore.signal()
        }.resume()
        _ = semaphore.wait(timeout: .now() + 9)

        guard let responseData,
              let payload = try? JSONDecoder().decode(OpenMeteoArchiveResponse.self, from: responseData),
              let hourly = payload.hourly else {
            return nil
        }

        let timeFormatter = DateFormatter()
        timeFormatter.locale = Locale(identifier: "en_US_POSIX")
        timeFormatter.timeZone = Self.displayTimeZone
        timeFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm"

        let timestamps = hourly.time.compactMap { timeFormatter.date(from: $0) }
        guard timestamps.isEmpty == false else { return nil }

        let targetIndex = timestamps.enumerated().min { lhs, rhs in
            abs(lhs.element.timeIntervalSince(targetDate)) < abs(rhs.element.timeIntervalSince(targetDate))
        }?.offset
        guard let targetIndex else { return nil }

        let temp = hourly.temperature2m[safe: targetIndex]
        let code = hourly.weatherCode[safe: targetIndex]
        let wind = hourly.windSpeed10m[safe: targetIndex]
        let precipitation = hourly.precipitation[safe: targetIndex]

        var parts: [String] = []
        if let code { parts.append(Self.weatherCodeDescription(for: code)) }
        if let temp { parts.append(String(format: "%.0f°F", temp)) }
        if let wind { parts.append(String(format: "Wind %.0f mph", wind)) }
        if let precipitation, precipitation > 0.0 {
            parts.append(String(format: "Precipitation %.2f in", precipitation))
        }

        return parts.isEmpty ? nil : parts.joined(separator: " | ")
    }

    private static func weatherCodeDescription(for code: Int) -> String {
        switch code {
        case 0: return "Clear"
        case 1: return "Mostly Clear"
        case 2: return "Partly Cloudy"
        case 3: return "Overcast"
        case 45, 48: return "Fog"
        case 51, 53, 55, 56, 57: return "Drizzle"
        case 61, 63, 65, 66, 67: return "Rain"
        case 71, 73, 75, 77: return "Snow"
        case 80, 81, 82: return "Rain Showers"
        case 85, 86: return "Snow Showers"
        case 95, 96, 99: return "Thunderstorm"
        default: return "Variable Conditions"
        }
    }

    private struct SessionReportContext {
        let session: SessionReportSession
        let shots: [SessionReportShot]
        let guidedRows: [SessionReportGuidedRow]
    }

    private struct FlaggedComparisonEntry {
        let currentShot: SessionReportShot
        let currentImageURL: URL?
        let previousShot: PreviousComparableShot?
        let previousImageURL: URL?
        let previousMissingReason: String?
        let currentDateText: String
        let previousDateText: String
        let currentMonthYear: String
        let previousMonthYear: String
    }

    private struct SessionReportPhotoEntry {
        let shot: SessionReportShot?
        let guidedRow: SessionReportGuidedRow?
        let imageURL: URL?

        init(shot: SessionReportShot, imageURL: URL?) {
            self.shot = shot
            self.guidedRow = nil
            self.imageURL = imageURL
        }

        init(skippedGuidedRow: SessionReportGuidedRow) {
            self.shot = nil
            self.guidedRow = skippedGuidedRow
            self.imageURL = nil
        }

        var isSkipped: Bool { guidedRow != nil }
        var skipReason: String? { guidedRow?.skipReason }
        var isFlagged: Bool { shot?.isFlagged == 1 }
        var flaggedReason: String? { shot?.flaggedReason }
        var capturedAtUTC: String? { shot?.capturedAtUTC }
        var building: String? { shot?.building ?? guidedRow?.building }
        var elevation: String? { shot?.elevation ?? guidedRow?.elevation }
        var detailType: String? { shot?.detailType ?? guidedRow?.detailType }
        var angleIndex: Int? { shot?.angleIndex ?? guidedRow?.angleIndex }
        var shotKey: String? { shot?.shotKey }
        var originalFilename: String { shot?.originalFilename ?? "" }
    }

    private struct GroupedPhotoSection {
        let key: String
        let title: String
        let entries: [SessionReportPhotoEntry]
        let retiredNotes: [String]
    }

    private struct PhotoChunkPlan {
        let pageNumber: Int
        let entries: [SessionReportPhotoEntry]
    }

    private struct PhotoSectionRenderPlan {
        let key: String
        let title: String
        let entries: [SessionReportPhotoEntry]
        let retiredNotes: [String]
        let pageChunks: [PhotoChunkPlan]
        let startPage: Int
        let endPage: Int
    }

    private struct DocumentationIndexLine {
        enum Kind {
            case sectionHeader
            case photoItem
            case retiredSpacer
            case retiredNote
        }

        let kind: Kind
        let text: String
        let pageNumber: Int?
        let isFlagged: Bool
    }

    private struct DocumentationIndexPlacedLine {
        let line: DocumentationIndexLine
        let rect: CGRect
    }

    private struct DocumentationIndexPagePlan {
        let pageIndex: Int
        let lines: [DocumentationIndexPlacedLine]
    }

    private struct PreparedPhotoEntries {
        let entries: [SessionReportPhotoEntry]
        let retiredNotesBySection: [String: [String]]
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

    private static let shortDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = displayTimeZone
        formatter.dateFormat = "MM/dd/yyyy"
        return formatter
    }()

    private static let shortTimeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = displayTimeZone
        formatter.dateFormat = "h:mm a"
        return formatter
    }()

    private static let monthYearFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = displayTimeZone
        formatter.dateFormat = "MMMM yyyy"
        return formatter
    }()

    private static let iso8601FormatterWithFractionalSeconds: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    fileprivate static func parseUTCDate(_ rawValue: String) -> Date? {
        if let date = iso8601FormatterWithFractionalSeconds.date(from: rawValue) {
            return date
        }
        return ISO8601DateFormatter().date(from: rawValue)
    }

    private func monthYearUTC(_ rawValue: String?) -> String? {
        guard let rawValue, let date = Self.parseUTCDate(rawValue) else { return nil }
        return Self.monthYearFormatter.string(from: date)
    }
}

private struct SessionReportSession: FetchableRecord, Decodable {
    let sessionID: String
    let propertyID: String?
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
        case propertyID = "property_id"
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
    let latitude: Double?
    let longitude: Double?
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
        case latitude
        case longitude
        case originalFilename = "original_filename"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case isFlagged = "is_flagged"
        case flaggedReason = "flagged_reason"
    }
}

private struct SessionReportGuidedRow: FetchableRecord, Decodable {
    let sessionID: String
    let building: String?
    let elevation: String?
    let detailType: String?
    let angleIndex: Int?
    let status: String?
    let isRetiredRaw: Int?
    let retiredAtUTC: String?
    let skipReason: String?
    let skipSessionID: String?

    var isRetired: Bool {
        if isRetiredRaw == 1 { return true }
        let trimmedStatus = status?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmedStatus == "retired"
    }

    func wasRetiredDuringSession(sessionStartedAtUTC: String, sessionEndedAtUTC: String?) -> Bool {
        guard isRetired else { return false }
        guard let retiredAtUTC else { return false }
        guard let sessionStart = PDFSessionReportGenerator.parseUTCDate(sessionStartedAtUTC) else {
            return false
        }
        let retiredDate = SessionReportGuidedRow.parseFlexibleDate(retiredAtUTC)
            ?? SessionReportGuidedRow.parseUsingSessionDatePrefix(retiredAtUTC)

        guard let retiredDate else { return false }

        if let sessionEndedAtUTC,
           let sessionEnd = PDFSessionReportGenerator.parseUTCDate(sessionEndedAtUTC) {
            return retiredDate >= sessionStart && retiredDate <= sessionEnd
        }

        let fallbackEnd = sessionStart.addingTimeInterval(24 * 60 * 60)
        return retiredDate >= sessionStart && retiredDate <= fallbackEnd
    }

    private static func parseFlexibleDate(_ raw: String) -> Date? {
        if let iso = PDFSessionReportGenerator.parseUTCDate(raw) {
            return iso
        }

        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)

        let formats = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm",
            "MM/dd/yyyy HH:mm:ss",
            "MM/dd/yyyy HH:mm",
            "MM/dd/yyyy h:mma",
            "MM/dd/yyyy h:mm a",
        ]

        for format in formats {
            formatter.dateFormat = format
            if let parsed = formatter.date(from: raw) {
                return parsed
            }
        }
        return nil
    }

    private static func parseUsingSessionDatePrefix(_ raw: String) -> Date? {
        guard raw.count >= 10 else { return nil }
        let prefix = String(raw.prefix(10))
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter.date(from: prefix)
    }

    enum CodingKeys: String, CodingKey {
        case sessionID = "session_id"
        case building
        case elevation
        case detailType = "detail_type"
        case angleIndex = "angle_index"
        case status
        case isRetiredRaw = "is_retired"
        case retiredAtUTC = "retired_at"
        case skipReason = "skip_reason"
        case skipSessionID = "skip_session_id"
    }
}

private struct PreviousComparableShot: FetchableRecord, Decodable {
    let sessionID: String
    let shotID: String
    let building: String?
    let elevation: String?
    let detailType: String?
    let angleIndex: Int?
    let shotKey: String?
    let capturedAtUTC: String?
    let originalFilename: String
    let stampedJpegFilename: String?
    let flaggedReason: String?

    enum CodingKeys: String, CodingKey {
        case sessionID = "session_id"
        case shotID = "shot_id"
        case building
        case elevation
        case detailType = "detail_type"
        case angleIndex = "angle_index"
        case shotKey = "shot_key"
        case capturedAtUTC = "captured_at_utc"
        case originalFilename = "original_filename"
        case stampedJpegFilename = "stamped_jpeg_filename"
        case flaggedReason = "flagged_reason"
    }
}

private struct OpenMeteoArchiveResponse: Decodable {
    let hourly: OpenMeteoHourly?
}

private struct OpenMeteoHourly: Decodable {
    let time: [String]
    let temperature2m: [Double]
    let weatherCode: [Int]
    let windSpeed10m: [Double]
    let precipitation: [Double]

    enum CodingKeys: String, CodingKey {
        case time
        case temperature2m = "temperature_2m"
        case weatherCode = "weather_code"
        case windSpeed10m = "wind_speed_10m"
        case precipitation
    }
}

private extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}

enum PDFSessionReportError: LocalizedError {
    case databaseUnavailable
    case sessionNotFound(String)
    case noFlaggedItems(String)
    case pdfContextCreationFailed(String)

    var errorDescription: String? {
        switch self {
        case .databaseUnavailable:
            return "Database is unavailable."
        case .sessionNotFound(let sessionID):
            return "Session \(sessionID) was not found in the database."
        case .noFlaggedItems(let sessionID):
            return "Session \(sessionID) has no flagged items."
        case .pdfContextCreationFailed(let path):
            return "Unable to create PDF context for \(path)."
        }
    }
}
