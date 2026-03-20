//
//  ScoutProcessPipeline.swift
//  ScoutProcess
//

import AppKit
import CryptoKit
import CoreGraphics
import Foundation
import GRDB
import ImageIO
import UniformTypeIdentifiers

struct ScoutProcessDirectories {
    let base: URL
    let input: URL
    let working: URL
    let failed: URL
    let duplicate: URL
    let archiveRoot: URL
    let clientsRoot: URL

    static func defaultDirectories() -> Self {
        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
        let base = docs.appending(path: "ScoutProcess", directoryHint: .isDirectory)
        let archiveRoot = docs.appending(path: "ScoutArchive", directoryHint: .isDirectory)
        let clientsRoot = archiveRoot.appending(path: "Clients", directoryHint: .isDirectory)
        return Self(
            base: base,
            input: base.appending(path: "Input", directoryHint: .isDirectory),
            working: base.appending(path: "Working", directoryHint: .isDirectory),
            failed: base.appending(path: "Failed", directoryHint: .isDirectory),
            duplicate: base.appending(path: "Duplicate", directoryHint: .isDirectory),
            archiveRoot: archiveRoot,
            clientsRoot: clientsRoot
        )
    }
}

extension URL {
    var tildePath: String {
        (path(percentEncoded: false) as NSString).abbreviatingWithTildeInPath
    }
}

enum ScoutProcessError: LocalizedError {
    case missingSessionJSON
    case missingOriginals
    case unzipFailed(String)
    case noShots
    case sourceImageMissing(String)
    case invalidImage(String)
    case missingRequiredMetadata(String)
    case duplicateZIP(String)

    var errorDescription: String? {
        switch self {
        case .missingSessionJSON: "session.json is missing at the session root."
        case .missingOriginals: "Originals folder is missing at the session root."
        case .unzipFailed(let output): "Unzip failed. \(output)"
        case .noShots: "session.json contains no shots."
        case .sourceImageMissing(let name): "Source image missing: \(name)"
        case .invalidImage(let name): "Unable to decode image: \(name)"
        case .missingRequiredMetadata(let field): "Missing required metadata: \(field)"
        case .duplicateZIP(let sessionID): "Duplicate ZIP detected. Already imported as session \(sessionID)."
        }
    }
}

struct SessionManifest: Decodable {
    var orgName: String?
    var orgNameAtCapture: String?
    var orgId: String?
    var folderId: String?
    var folderIDAtCapture: String?
    var propertyName: String?
    var propertyNameAtCapture: String?
    var propertyId: String?
    var startedAt: String?
    var captureProfile: String?
    var building: String?
    var elevation: String?
    var detailType: String?
    var shots: [Shot]
    var issues: [Issue]?

    struct Shot: Decodable {
        var originalFilename: String?
        var stampedFilename: String?
        var shotKey: String?
        var logicalShotIdentity: String?
        var shotName: String?
        var building: String?
        var elevation: String?
        var detailType: String?
        var angleIndex: Int?
        var capturedAtLocal: String?
        var isFlagged: Bool?
        var issueID: String?
        var issueId: String?
        var currentReason: String?
        var propertyName: String?
    }

    struct Issue: Decodable {
        var id: String?
        var issueID: String?
        var issueId: String?
        var currentReason: String?
        var reason: String?
        var name: String?
    }
}

private enum StampFlagVisualState {
    case none
    case flagged
    case resolved
}

final class ScoutProcessController {
    struct DiagnosticsSnapshot {
        let queueCount: Int
        let queuedPathsCount: Int
        let observationsCount: Int
        let processedThisRunCount: Int
    }

    struct SessionArchiveMetadata {
        let orgNameResolved: String
        let folderId: String
        let propertyNameResolved: String
        let sessionDate: String
    }

    struct QueueUpdate {
        let fileName: String
        let status: ScoutProcessModel.QueueStatus
        let detail: String
        let updatedAt: Date
        let destinationURL: URL?
    }

    typealias AppState = ScoutProcessModel.AppState
    typealias QueueStatus = ScoutProcessModel.QueueStatus

    var onLog: ((String) -> Void)?
    var onStateChange: ((AppState) -> Void)?
    var onQueueUpdate: ((QueueUpdate) -> Void)?
    var onLastProcessed: ((Date) -> Void)?

    private let directories: ScoutProcessDirectories
    private let fileManager = FileManager.default
    private let decoder = JSONDecoder()
    // AppKit image rendering can hop through lower-priority worker threads.
    // Keeping the pipeline at .default avoids QoS inversion warnings during stamping.
    private let processingQueue = DispatchQueue(label: "com.bennett.scoutprocess.pipeline", qos: .default)
    private let watcherQueue = DispatchQueue(label: "com.bennett.scoutprocess.watcher", qos: .utility)
    private let stateLock = NSLock()
    private var watcherTask: Task<Void, Never>?
    private var directoryFileDescriptor: CInt = -1
    private var directoryMonitor: DispatchSourceFileSystemObject?
    private var isRunning = false
    private var processedThisRun: Set<String> = []
    private var queue: [URL] = []
    private var queuedPaths: Set<String> = []
    private var isProcessing = false
    private var observations: [String: FileObservation] = [:]

    private let jpegQuality: CGFloat = 0.85
    private let stabilityInterval: TimeInterval = 0.75
    private let pollIntervalNanoseconds: UInt64 = 1_000_000_000
    private let enableSessionReportPDF = true
    private let enableFlaggedComparisonPDF = true
    private static let supportedStampImageExtensions: Set<String> = ["heic", "jpg", "jpeg", "png"]

    private enum ScanAction {
        case none
        case created
        case modified
        case queuedStable
        case waiting(String)
    }

    init(directories: ScoutProcessDirectories) {
        self.directories = directories
        decoder.dateDecodingStrategy = .iso8601
    }

    func start() {
        guard !isRunning else { return }
        isRunning = true
        createDirectoriesIfNeeded()
        log("Watcher started. Watching: \(directories.input.tildePath)/")
        startDirectoryMonitor()
        watcherTask = Task.detached { [weak self] in
            await self?.watchLoop()
        }
    }

    func stop() {
        isRunning = false
        watcherTask?.cancel()
        watcherTask = nil
        stopDirectoryMonitor()
    }

    func enqueueImportedZip(_ url: URL) {
        guard url.pathExtension.lowercased() == "zip" else { return }
        guard fileManager.fileExists(atPath: url.path) else { return }

        let enqueued = withStateLock { () -> Bool in
            guard queuedPaths.contains(url.path) == false else { return false }

            observations.removeValue(forKey: url.path)
            queue.append(url)
            queuedPaths.insert(url.path)
            return true
        }

        guard enqueued else { return }
        log("Detected: \(url.lastPathComponent) (event: created)")
        updateQueue(url.lastPathComponent, status: .pending, detail: "Queued from drag and drop")
    }

    func diagnosticsSnapshot() -> DiagnosticsSnapshot {
        withStateLock {
            DiagnosticsSnapshot(
                queueCount: queue.count,
                queuedPathsCount: queuedPaths.count,
                observationsCount: observations.count,
                processedThisRunCount: processedThisRun.count
            )
        }
    }

    private func createDirectoriesIfNeeded() {
        for directory in [directories.base, directories.input, directories.working, directories.failed, directories.duplicate, directories.archiveRoot, directories.clientsRoot] {
            do {
                try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            } catch {
                log("Failed to create directory \(directory.lastPathComponent): \(error.localizedDescription)")
                onStateChange?(.error)
            }
        }
    }

    private func startDirectoryMonitor() {
        stopDirectoryMonitor()

        directoryFileDescriptor = open(directories.input.path, O_EVTONLY)
        guard directoryFileDescriptor >= 0 else {
            log("Failed to start directory monitor for Input.")
            return
        }

        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: directoryFileDescriptor,
            eventMask: [.write, .rename, .delete, .extend, .attrib, .link],
            queue: watcherQueue
        )

        source.setEventHandler { [weak self] in
            self?.scanInputDirectory()
        }

        source.setCancelHandler { [weak self] in
            guard let self else { return }
            if self.directoryFileDescriptor >= 0 {
                close(self.directoryFileDescriptor)
                self.directoryFileDescriptor = -1
            }
        }

        directoryMonitor = source
        source.resume()
        scanInputDirectory()
    }

    private func stopDirectoryMonitor() {
        if let directoryMonitor {
            directoryMonitor.cancel()
            self.directoryMonitor = nil
        } else if directoryFileDescriptor >= 0 {
            close(directoryFileDescriptor)
            directoryFileDescriptor = -1
        }
    }

    private func watchLoop() async {
        while !Task.isCancelled, isRunning {
            // Periodic rescan keeps the "file is stable" timer advancing even after the last write event.
            scanInputDirectory()
            if let nextURL = dequeueNextZipForProcessing() {
                await process(zipURL: nextURL)
            }

            try? await Task.sleep(nanoseconds: pollIntervalNanoseconds)
        }
    }

    private func scanInputDirectory() {
        guard let enumerator = try? fileManager.contentsOfDirectory(
            at: directories.input,
            includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey, .isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return
        }

        let now = Date()

        for url in enumerator where url.pathExtension.lowercased() == "zip" {
            let values = try? url.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey, .isRegularFileKey])
            guard values?.isRegularFile == true else { continue }

            let size = values?.fileSize ?? 0
            let modifiedAt = values?.contentModificationDate ?? now

            let action = withStateLock { () -> ScanAction in
                guard queuedPaths.contains(url.path) == false else { return .none }

                if let observation = observations[url.path] {
                    if observation.lastSize != size || observation.lastModifiedAt != modifiedAt {
                        observations[url.path] = FileObservation(
                            firstDetectedAt: observation.firstDetectedAt,
                            lastSize: size,
                            lastModifiedAt: modifiedAt,
                            stableSince: now
                        )
                        return .waiting("ZIP not ready, size changing")
                    }

                    if now.timeIntervalSince(observation.stableSince) >= stabilityInterval,
                       now.timeIntervalSince(modifiedAt) >= stabilityInterval {
                        guard isZIPReadable(at: url) else {
                            return .waiting("ZIP not ready, file not readable")
                        }
                        queue.append(url)
                        queuedPaths.insert(url.path)
                        observations.removeValue(forKey: url.path)
                        return .queuedStable
                    }

                    observations[url.path] = observation
                    return .none
                }

                observations[url.path] = FileObservation(
                    firstDetectedAt: now,
                    lastSize: size,
                    lastModifiedAt: modifiedAt,
                    stableSince: now
                )
                return .created
            }

            switch action {
            case .created:
                log("Timing zip-detected file=\(url.lastPathComponent) elapsed_ms=0")
                log("Timing readiness-start file=\(url.lastPathComponent) elapsed_ms=0")
            case .modified:
                log("Detected: \(url.lastPathComponent) (event: modified)")
            case .queuedStable:
                updateQueue(url.lastPathComponent, status: .pending, detail: "Queued for processing")
                log("Timing readiness-end file=\(url.lastPathComponent) elapsed_ms=\(millisecondsSince(firstDetectedAt(for: url.path), now: now))")
                log("Detected stable ZIP \(url.lastPathComponent)")
            case .waiting(let reason):
                log("Timing readiness-wait file=\(url.lastPathComponent) elapsed_ms=\(millisecondsSince(firstDetectedAt(for: url.path), now: now)) reason=\(reason)")
            case .none:
                break
            }
        }
    }

    private func process(zipURL: URL) async {
        onStateChange?(.processing)
        updateQueue(zipURL.lastPathComponent, status: .processing, detail: "Preparing")
        await Task.yield()

        do {
            let result = try await runPipelineAsync(zipURL: zipURL)
            onLastProcessed?(Date())
            log("Completed \(zipURL.lastPathComponent) -> \(result.processedFolder.lastPathComponent)")
            updateQueue(zipURL.lastPathComponent, status: .done, detail: "Archived", destinationURL: result.processedFolder)
            onStateChange?(.idle)
        } catch {
            log("Failed \(zipURL.lastPathComponent): \(error.localizedDescription)")
            updateQueue(zipURL.lastPathComponent, status: .failed, detail: error.localizedDescription)
            onStateChange?(.error)
        }

        withStateLock {
            isProcessing = false
        }
    }

    private func runPipelineAsync(zipURL: URL) async throws -> PipelineResult {
        try await withCheckedThrowingContinuation { continuation in
            processingQueue.async { [self] in
                do {
                    let result = try runPipeline(zipURL: zipURL)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func runPipeline(zipURL: URL) throws -> PipelineResult {
        let moveStartedAt = Date()
        log("Timing move-start file=\(zipURL.lastPathComponent) elapsed_ms=0")
        let workingZipURL = try moveZipIntoWorking(zipURL)
        logTiming("move-end", startedAt: moveStartedAt, detail: "file=\(zipURL.lastPathComponent) path=\(workingZipURL.path(percentEncoded: false))")

        let fingerprintStartedAt = Date()
        log("Timing fingerprint-start file=\(workingZipURL.lastPathComponent) elapsed_ms=0")
        let zipFingerprint = try fingerprint(for: workingZipURL)
        logTiming("fingerprint-end", startedAt: fingerprintStartedAt, detail: "file=\(workingZipURL.lastPathComponent)")
        let sessionFolder = workingSessionFolderURL(for: workingZipURL)
        var processedCount = 0
        var lastStep = "initializing"

        do {
            lastStep = "unzipping archive"
            let extractionStartedAt = Date()
            log("Timing extraction-start file=\(workingZipURL.lastPathComponent) elapsed_ms=0")
            try unzip(zipURL: workingZipURL, destination: sessionFolder)
            logTiming("extraction-end", startedAt: extractionStartedAt, detail: "file=\(workingZipURL.lastPathComponent)")
            log("Unzipped into \(sessionFolder.lastPathComponent)")

            lastStep = "validating contents"
            let resolvedSessionFolder = try resolveSessionRoot(in: sessionFolder)
            let sessionJSONURL = resolvedSessionFolder.appending(path: "session.json")
            let originalsURL = resolvedSessionFolder.appending(path: "Originals", directoryHint: .isDirectory)

            let manifest = try loadManifest(at: sessionJSONURL)
            let archiveMetadata = try extractArchiveMetadata(from: manifest)

            let importResult: CSVImportSessionResult
            do {
                importResult = try CSVImportService.shared.importSessionFolder(
                    at: resolvedSessionFolder,
                    zipName: workingZipURL.lastPathComponent,
                    zipFingerprint: zipFingerprint
                )
            } catch {
                log("CSV import failed for \(resolvedSessionFolder.lastPathComponent): \(error.localizedDescription)")
                throw error
            }

            if let duplicateSessionID = importResult.duplicateSessionID {
                let duplicateZipURL = uniqueDestinationURL(in: directories.duplicate, preferredName: workingZipURL.lastPathComponent)
                try moveReplacingIfNeeded(from: workingZipURL, to: duplicateZipURL)
                if fileManager.fileExists(atPath: sessionFolder.path) {
                    try? fileManager.removeItem(at: sessionFolder)
                }
                log("Duplicate ZIP detected. Already imported as session \(duplicateSessionID).")
                throw ScoutProcessError.duplicateZIP(duplicateSessionID)
            }

            try writeValidationFile(at: resolvedSessionFolder, manifest: manifest)

            lastStep = "generating stamped JPGs"
            let stampedURL = resolvedSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
            try fileManager.createDirectory(at: stampedURL, withIntermediateDirectories: true)
            log("Rendering \(manifest.shots.count) stamped JPGs")
            let sourceCatalog = buildSourceImageCatalog(in: originalsURL)
            var stampingErrors: [String] = []
            var usedOutputNames: Set<String> = []
            let resolvedVisualIdentities = try resolvedShotIdentitySetForStamping(sessionID: importResult.sessionID)

            for (index, shot) in manifest.shots.enumerated() {
                let sourceName = shot.originalFilename?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                let detectedAsImage = isSupportedStampImageFileName(sourceName)

                guard !sourceName.isEmpty else {
                    let message = "Stamp input: source=<missing> detectedAsImage=false generated=false output=<none> error=Shot \(index + 1) originalFilename is empty"
                    log(message)
                    stampingErrors.append(message)
                    continue
                }

                guard let sourceURL = resolveSourceImageURL(named: sourceName, in: sourceCatalog) else {
                    let message = "Stamp input: source=\(originalsURL.appending(path: sourceName).path(percentEncoded: false)) detectedAsImage=\(detectedAsImage) generated=false output=<none> error=Source image missing"
                    log(message)
                    stampingErrors.append(message)
                    continue
                }

                let stampedName = uniqueStampedFilename(
                    preferredName: makeStampedFilename(
                        for: shot,
                        session: manifest,
                        forceFlagSuffix: shouldShowFlagVisual(for: shot, session: manifest, resolvedIdentitySet: resolvedVisualIdentities)
                    ),
                    sourceURL: sourceURL,
                    usedNames: &usedOutputNames
                )
                let destinationURL = stampedURL.appending(path: stampedName)
                let replacedExisting = fileManager.fileExists(atPath: destinationURL.path)

                do {
                    let isFlagged = shot.isFlagged == true
                    let visualState = stampVisualState(for: shot, session: manifest, resolvedIdentitySet: resolvedVisualIdentities)
                    try stampImage(
                        sourceURL: sourceURL,
                        destinationURL: destinationURL,
                        stampText: makeStampText(for: shot, session: manifest),
                        visualState: visualState
                    )
                    try validateStampedOutput(at: destinationURL)
                    try updateShotAssetMetadata(
                        sessionID: importResult.sessionID,
                        shot: shot,
                        session: manifest,
                        stampedFilename: stampedName
                    )
                    processedCount += 1
                    log("Stamp input: source=\(sourceURL.path(percentEncoded: false)) detectedAsImage=\(detectedAsImage) generated=true output=\(destinationURL.path(percentEncoded: false)) flagged=\(isFlagged) borderApplied=false replaced=\(replacedExisting) error=<none>")
                    log("Stamped image created: \(stampedName) | flagged=\(isFlagged) | border not applied")
                } catch {
                    let message = "Stamp input: source=\(sourceURL.path(percentEncoded: false)) detectedAsImage=\(detectedAsImage) generated=false output=\(destinationURL.path(percentEncoded: false)) flagged=\(shot.isFlagged == true) borderApplied=false error=\(error.localizedDescription)"
                    log(message)
                    stampingErrors.append(message)
                }
            }

            log("Stamp summary: requested=\(manifest.shots.count) generated=\(processedCount) failed=\(stampingErrors.count)")
            if let sessionID = importResult.sessionID,
               let dbQueue = DatabaseManager.shared.dbQueue {
                do {
                    let syncedCount = try dbQueue.write { db in
                        try PunchListService.shared.syncFlaggedItems(forSessionID: sessionID, db: db)
                    }
                    log("Punch list sync complete for session \(sessionID): \(syncedCount) item(s)")
                } catch {
                    log("Punch list sync failed for session \(sessionID): \(error.localizedDescription)")
                }
            }

            lastStep = "routing archived session"
            let archivedSessionURL = try archiveSessionFolder(
                from: resolvedSessionFolder,
                metadata: archiveMetadata
            )

            if enableSessionReportPDF {
                let resolvedSessionIDForPDF = importResult.sessionID
                    ?? resolveSessionIDFromSessionsCSV(in: archivedSessionURL)
                if let importedSessionID = resolvedSessionIDForPDF {
                    if importResult.sessionID == nil {
                        log("Session report fallback: using session ID from sessions.csv: \(importedSessionID)")
                    }
                    do {
                        let pdfURL = try PDFSessionReportGenerator().generateSessionReport(
                            sessionID: importedSessionID,
                            extractedFolderURL: archivedSessionURL,
                            zipName: workingZipURL.lastPathComponent
                        )
                        log("Session report created at \(pdfURL.path(percentEncoded: false))")
                    } catch {
                        log("Session report generation failed for session \(importedSessionID): \(error.localizedDescription)")
                    }
                } else {
                    log("Session report generation skipped: imported session ID was unavailable.")
                }
            }

            if enableFlaggedComparisonPDF {
                let resolvedSessionIDForPDF = importResult.sessionID
                    ?? resolveSessionIDFromSessionsCSV(in: archivedSessionURL)
                if let importedSessionID = resolvedSessionIDForPDF {
                    do {
                        let comparisonPDFURL = try PDFSessionReportGenerator().generateFlaggedComparisonReport(
                            sessionID: importedSessionID,
                            extractedFolderURL: archivedSessionURL,
                            zipName: workingZipURL.lastPathComponent
                        )
                        log("Flagged comparison report created at \(comparisonPDFURL.path(percentEncoded: false))")
                    } catch PDFSessionReportError.noFlaggedItems {
                        log("Flagged comparison report skipped: no flagged items in session \(importedSessionID).")
                    } catch {
                        log("Flagged comparison report generation failed for session \(importedSessionID): \(error.localizedDescription)")
                    }

                    do {
                        let priorityPDFURL = try PDFSessionReportGenerator().generatePriorityItemsReport(
                            sessionID: importedSessionID,
                            extractedFolderURL: archivedSessionURL,
                            zipName: workingZipURL.lastPathComponent
                        )
                        log("Priority items report created at \(priorityPDFURL.path(percentEncoded: false))")
                    } catch PDFSessionReportError.noFlaggedItems {
                        log("Priority items report skipped: no flagged items in session \(importedSessionID).")
                    } catch {
                        log("Priority items report generation failed for session \(importedSessionID): \(error.localizedDescription)")
                    }
                } else {
                    log("Flagged comparison report generation skipped: imported session ID was unavailable.")
                }
            }

            if sessionFolder != resolvedSessionFolder, fileManager.fileExists(atPath: sessionFolder.path) {
                try? fileManager.removeItem(at: sessionFolder)
            }

            try fileManager.removeItem(at: workingZipURL)
            log("Deleted processed ZIP \(workingZipURL.lastPathComponent)")
            if let qualitySummary = importResult.qualitySummary {
                log("IMPORT QUALITY SUMMARY")
                log(
                    "sessions=\(qualitySummary.rowsSessions) shots=\(qualitySummary.rowsShots) guided_rows=\(qualitySummary.rowsGuidedRows) issues=\(qualitySummary.rowsIssues) issue_history_upserted=\(qualitySummary.rowsIssueHistory) issue_history_parsed=\(qualitySummary.issueHistoryParsedRows) issue_history_skippedMalformed=\(qualitySummary.issueHistorySkippedMalformedRows) issue_history_skippedOrphan=\(qualitySummary.issueHistorySkippedOrphanRows)"
                )
            }

            return PipelineResult(processedFolder: archivedSessionURL)
        } catch {
            let failureSourceURL = bestFailureSourceURL(for: sessionFolder)
            let failedSessionURL = uniqueDestinationURL(in: directories.failed, preferredName: failureSourceURL.lastPathComponent)
            if fileManager.fileExists(atPath: failureSourceURL.path) {
                try? moveReplacingIfNeeded(from: failureSourceURL, to: failedSessionURL)
                writeErrorLog(at: failedSessionURL, step: lastStep, error: error, processedCount: processedCount)
                if failureSourceURL != sessionFolder, fileManager.fileExists(atPath: sessionFolder.path) {
                    try? fileManager.removeItem(at: sessionFolder)
                }
            }

            let failedZipURL = uniqueDestinationURL(in: directories.failed, preferredName: workingZipURL.lastPathComponent)
            if fileManager.fileExists(atPath: workingZipURL.path) {
                try? moveReplacingIfNeeded(from: workingZipURL, to: failedZipURL)
            } else if fileManager.fileExists(atPath: zipURL.path) {
                try? moveReplacingIfNeeded(from: zipURL, to: failedZipURL)
            }

            throw error
        }
    }

    private func dequeueNextZipForProcessing() -> URL? {
        withStateLock {
            guard !isProcessing, let nextURL = queue.first else { return nil }
            isProcessing = true
            queue.removeFirst()
            queuedPaths.remove(nextURL.path)
            return nextURL
        }
    }

    private func withStateLock<T>(_ body: () -> T) -> T {
        stateLock.lock()
        defer { stateLock.unlock() }
        return body()
    }

    private func firstDetectedAt(for path: String) -> Date {
        withStateLock {
            observations[path]?.firstDetectedAt ?? Date()
        }
    }

    private func isZIPReadable(at url: URL) -> Bool {
        do {
            let handle = try FileHandle(forReadingFrom: url)
            try handle.close()
            return true
        } catch {
            return false
        }
    }

    private func millisecondsSince(_ startedAt: Date, now: Date = Date()) -> Int {
        Int(now.timeIntervalSince(startedAt) * 1000)
    }

    private func logTiming(_ step: String, startedAt: Date, detail: String) {
        log("Timing \(step) \(detail) elapsed_ms=\(millisecondsSince(startedAt))")
    }

    private func moveZipIntoWorking(_ zipURL: URL) throws -> URL {
        let destination = uniqueDestinationURL(in: directories.working, preferredName: zipURL.lastPathComponent)
        try moveReplacingIfNeeded(from: zipURL, to: destination)
        log("Moved ZIP into Working: \(destination.lastPathComponent)")
        return destination
    }

    private func extractArchiveMetadata(from manifest: SessionManifest) throws -> SessionArchiveMetadata {
        let orgName = try sanitizeFolderComponent(
            manifest.orgNameAtCapture ?? manifest.orgName,
            requiredField: "orgName"
        )
        let folderId = try requiredMetadataValue(
            manifest.folderId ?? manifest.folderIDAtCapture,
            fieldName: "folderId"
        )
        let propertyName = try sanitizeFolderComponent(
            manifest.propertyNameAtCapture ?? manifest.propertyName,
            requiredField: "propertyName"
        )

        let dateSource = manifest.startedAt
            ?? manifest.shots.first?.capturedAtLocal

        guard let rawDate = dateSource,
              let date = Self.parseDate(rawDate) else {
            throw ScoutProcessError.missingRequiredMetadata("startedAt")
        }

        return SessionArchiveMetadata(
            orgNameResolved: orgName,
            folderId: folderId,
            propertyNameResolved: propertyName,
            sessionDate: Self.archiveDateFormatter.string(from: date)
        )
    }

    private func archiveSessionFolder(
        from sourceSessionFolder: URL,
        metadata: SessionArchiveMetadata
    ) throws -> URL {
        let orgFolder = directories.clientsRoot.appending(path: metadata.orgNameResolved, directoryHint: .isDirectory)
        let propertyFolderName = "\(metadata.folderId) \(metadata.propertyNameResolved)"
        let propertyFolder = orgFolder.appending(path: propertyFolderName, directoryHint: .isDirectory)

        try fileManager.createDirectory(at: propertyFolder, withIntermediateDirectories: true)

        let sessionFolder = nextSessionFolder(in: propertyFolder, datePrefix: metadata.sessionDate)
        let originalsDestination = sessionFolder.appending(path: "Originals", directoryHint: .isDirectory)
        let stampedDestination = sessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
        let pdfDestination = sessionFolder.appending(path: "PDF", directoryHint: .isDirectory)

        try fileManager.createDirectory(at: sessionFolder, withIntermediateDirectories: true)
        try fileManager.createDirectory(at: originalsDestination, withIntermediateDirectories: true)
        try fileManager.createDirectory(at: stampedDestination, withIntermediateDirectories: true)
        try fileManager.createDirectory(at: pdfDestination, withIntermediateDirectories: true)

        let originalsSource = sourceSessionFolder.appending(path: "Originals", directoryHint: .isDirectory)
        if fileManager.fileExists(atPath: originalsSource.path) {
            try moveReplacingIfNeeded(from: originalsSource, to: originalsDestination)
            log("Moved \(originalsSource.path(percentEncoded: false)) -> \(originalsDestination.path(percentEncoded: false))")
        }

        let stampedSource = sourceSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
        if fileManager.fileExists(atPath: stampedSource.path) {
            try moveReplacingIfNeeded(from: stampedSource, to: stampedDestination)
            log("Moved \(stampedSource.path(percentEncoded: false)) -> \(stampedDestination.path(percentEncoded: false))")
        }

        let rootContents = try fileManager.contentsOfDirectory(
            at: sourceSessionFolder,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        )

        for item in rootContents {
            let values = try? item.resourceValues(forKeys: [.isRegularFileKey])
            guard values?.isRegularFile == true else { continue }

            let destination = sessionFolder.appending(path: item.lastPathComponent)
            try moveReplacingIfNeeded(from: item, to: destination)
            log("Moved \(item.path(percentEncoded: false)) -> \(destination.path(percentEncoded: false))")
        }

        try? fileManager.removeItem(at: sourceSessionFolder)
        return sessionFolder
    }

    private func nextSessionFolder(in propertyFolder: URL, datePrefix: String) -> URL {
        let existingNames = (try? fileManager.contentsOfDirectory(
            at: propertyFolder,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        ))?.compactMap(\.lastPathComponent) ?? []

        var sequence = 1
        while true {
            let suffix = String(format: "%02d", sequence)
            let folderName = "\(datePrefix)_\(suffix)"
            if existingNames.contains(folderName) == false,
               !fileManager.fileExists(atPath: propertyFolder.appending(path: folderName).path) {
                return propertyFolder.appending(path: folderName, directoryHint: .isDirectory)
            }
            sequence += 1
        }
    }

    private func requiredMetadataValue(_ value: String?, fieldName: String) throws -> String {
        guard let value else {
            throw ScoutProcessError.missingRequiredMetadata(fieldName)
        }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw ScoutProcessError.missingRequiredMetadata(fieldName)
        }
        return trimmed
    }

    private func sanitizeFolderComponent(_ value: String?, requiredField: String) throws -> String {
        let rawValue = try requiredMetadataValue(value, fieldName: requiredField)
        return sanitizeArchiveComponent(rawValue)
    }

    private func resolveSessionRoot(in container: URL) throws -> URL {
        if isValidSessionRoot(container) {
            return container
        }

        let children = try fileManager.contentsOfDirectory(
            at: container,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        )

        let directoriesOnly = children.filter {
            (try? $0.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true
        }

        for child in directoriesOnly {
            if isValidSessionRoot(child) {
                return child
            }
        }

        if fileManager.fileExists(atPath: container.appending(path: "session.json").path) == false {
            throw ScoutProcessError.missingSessionJSON
        }

        throw ScoutProcessError.missingOriginals
    }

    private func isValidSessionRoot(_ url: URL) -> Bool {
        let sessionJSONURL = url.appending(path: "session.json")
        let originalsURL = url.appending(path: "Originals", directoryHint: .isDirectory)
        guard fileManager.fileExists(atPath: sessionJSONURL.path) else { return false }

        var isDirectory: ObjCBool = false
        return fileManager.fileExists(atPath: originalsURL.path, isDirectory: &isDirectory) && isDirectory.boolValue
    }

    private func bestFailureSourceURL(for sessionFolder: URL) -> URL {
        if let resolved = try? resolveSessionRoot(in: sessionFolder) {
            return resolved
        }
        return sessionFolder
    }

    private func workingSessionFolderURL(for zipURL: URL) -> URL {
        let stamp = Self.folderStampFormatter.string(from: Date())
        let baseName = zipURL.deletingPathExtension().lastPathComponent
        return directories.working.appending(path: "\(sanitize(baseName))_\(stamp)", directoryHint: .isDirectory)
    }

    private func unzip(zipURL: URL, destination: URL) throws {
        try fileManager.createDirectory(at: destination, withIntermediateDirectories: true)

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/unzip")
        process.arguments = ["-q", zipURL.path, "-d", destination.path]

        let outputPipe = Pipe()
        process.standardOutput = outputPipe
        process.standardError = outputPipe

        try process.run()
        process.waitUntilExit()

        let output = String(data: outputPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
        if process.terminationStatus != 0 {
            throw ScoutProcessError.unzipFailed(output.trimmingCharacters(in: .whitespacesAndNewlines))
        }
    }

    private func loadManifest(at url: URL) throws -> SessionManifest {
        let data = try Data(contentsOf: url)
        let manifest = try decoder.decode(SessionManifest.self, from: data)
        guard !manifest.shots.isEmpty else {
            throw ScoutProcessError.noShots
        }
        return manifest
    }

    private func writeValidationFile(at sessionFolder: URL, manifest: SessionManifest) throws {
        let lines = [
            "Validation: PASS",
            "Generated: \(Self.logDateFormatter.string(from: Date()))",
            "Shots: \(manifest.shots.count)",
            "Issues: \(manifest.issues?.count ?? 0)",
            "Originals: PASS",
            "session.json: PASS"
        ]
        let content = lines.joined(separator: "\n")
        try content.write(to: sessionFolder.appending(path: "validation.txt"), atomically: true, encoding: .utf8)
    }

    private func writeErrorLog(at sessionFolder: URL, step: String, error: Error, processedCount: Int) {
        let lines = [
            "Step: \(step)",
            "Error: \(error.localizedDescription)",
            "Debug: \(String(describing: error))",
            "Processed before failure: \(processedCount)",
            "Timestamp: \(Self.logDateFormatter.string(from: Date()))"
        ]
        let content = lines.joined(separator: "\n")
        try? content.write(to: sessionFolder.appending(path: "error.txt"), atomically: true, encoding: .utf8)
    }

    private func buildIssueLookup(from issues: [SessionManifest.Issue]) -> [String: String] {
        var lookup: [String: String] = [:]

        for issue in issues {
            let key = issue.id ?? issue.issueID ?? issue.issueId
            let value = issue.currentReason ?? issue.reason ?? issue.name
            if let key, let value, !key.isEmpty, !value.isEmpty {
                lookup[key] = value
            }
        }

        return lookup
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

    private func formatLocalTimestamp(_ rawValue: String?) -> String {
        guard let rawValue else {
            return Self.overlayDateFormatter.string(from: Date())
        }

        if let date = Self.inputDateFormatter.date(from: rawValue) ?? Self.isoDateFormatter.date(from: rawValue) {
            return Self.overlayDateFormatter.string(from: date)
        }

        return rawValue
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

    private func stampImage(sourceURL: URL, destinationURL: URL, stampText: String, visualState: StampFlagVisualState) throws {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil),
              let original = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        let size = CGSize(width: original.width, height: original.height)
        guard size.width > 0, size.height > 0 else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        let colorSpace = original.colorSpace ?? CGColorSpace(name: CGColorSpace.sRGB) ?? CGColorSpaceCreateDeviceRGB()
        guard let context = CGContext(
            data: nil,
            width: original.width,
            height: original.height,
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
        let fontSize: CGFloat = isPortrait ? 56 : 48
        let pillHeight: CGFloat = isPortrait ? 108 : 96
        let horizontalPadding: CGFloat = isPortrait ? 36 : 32
        let verticalPadding: CGFloat = isPortrait ? 22 : 20
        let bottomMargin: CGFloat = isPortrait ? 40 : 36
        let sideMargin: CGFloat = isPortrait ? 40 : 36
        let cornerRadius: CGFloat = isPortrait ? 22 : 20
        let maxOverlayWidth = size.width - (sideMargin * 2)
        let showsFlagGlyph = visualState != .none
        let glyphGap: CGFloat = showsFlagGlyph ? 14 : 0
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

        if fileManager.fileExists(atPath: destinationURL.path) {
            try fileManager.removeItem(at: destinationURL)
        }

        guard let destination = CGImageDestinationCreateWithURL(
            destinationURL as CFURL,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            throw ScoutProcessError.invalidImage(destinationURL.lastPathComponent)
        }

        let properties: [CFString: Any] = [
            kCGImageDestinationLossyCompressionQuality: jpegQuality
        ]
        guard let renderedImage = context.makeImage() else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        CGImageDestinationAddImage(destination, renderedImage, properties as CFDictionary)
        guard CGImageDestinationFinalize(destination) else {
            throw ScoutProcessError.invalidImage(destinationURL.lastPathComponent)
        }
    }

    private func fittedStampText(
        _ stampText: String,
        maxTextWidth: CGFloat,
        attributes: [NSAttributedString.Key: Any]
    ) -> String {
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
        let minCharacters = 4

        while shotName.count > minCharacters {
            shotName.removeLast()
            let candidate = "\(prefix)\(shotName)...\(detailAndDate)"
            if (candidate as NSString).size(withAttributes: attributes).width <= maxTextWidth {
                return candidate
            }
        }

        return "\(prefix)...\(detailAndDate)"
    }

    private func makeFlagGlyphImage(fontSize: CGFloat, visualState: StampFlagVisualState) -> NSImage? {
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

    private func shouldShowFlagVisual(for shot: SessionManifest.Shot, session: SessionManifest, resolvedIdentitySet: Set<String>) -> Bool {
        shot.isFlagged == true || stampVisualState(for: shot, session: session, resolvedIdentitySet: resolvedIdentitySet) == .resolved
    }

    private func stampVisualState(for shot: SessionManifest.Shot, session: SessionManifest, resolvedIdentitySet: Set<String>) -> StampFlagVisualState {
        let identity = stampIdentity(for: shot, session: session)
        if resolvedIdentitySet.contains(identity) {
            return .resolved
        }
        if shot.isFlagged == true {
            return .flagged
        }
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
            guard isSupportedStampImageFileName(fileURL.lastPathComponent) else { continue }
            catalog[fileURL.lastPathComponent.lowercased(), default: []].append(fileURL)
        }
        return catalog
    }

    private func resolveSourceImageURL(named sourceName: String, in catalog: [String: [URL]]) -> URL? {
        let matches = catalog[sourceName.lowercased()] ?? []
        return matches.sorted { lhs, rhs in
            lhs.path.count < rhs.path.count
        }.first
    }

    private func isSupportedStampImageFileName(_ fileName: String) -> Bool {
        let pathExtension = URL(fileURLWithPath: fileName).pathExtension.lowercased()
        return Self.supportedStampImageExtensions.contains(pathExtension)
    }

    private func uniqueStampedFilename(preferredName: String, sourceURL _: URL, usedNames: inout Set<String>) -> String {
        let preferredLower = preferredName.lowercased()
        guard usedNames.contains(preferredLower) else {
            usedNames.insert(preferredLower)
            return preferredName
        }

        let baseURL = URL(fileURLWithPath: preferredName)
        let baseName = baseURL.deletingPathExtension().lastPathComponent
        let ext = baseURL.pathExtension.isEmpty ? "jpg" : baseURL.pathExtension
        for index in 2...999 {
            let indexedCandidate = "\(baseName)_\(index).\(ext)"
            let indexedLower = indexedCandidate.lowercased()
            if usedNames.contains(indexedLower) == false {
                usedNames.insert(indexedLower)
                log("Stamp output collision detected. Using \(indexedCandidate) instead of \(preferredName).")
                return indexedCandidate
            }
        }

        usedNames.insert(preferredLower)
        return preferredName
    }

    private func validateStampedOutput(at url: URL) throws {
        guard fileManager.fileExists(atPath: url.path) else {
            throw ScoutProcessError.invalidImage(url.lastPathComponent)
        }

        let fileSize = (try? fileManager.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?.intValue ?? 0
        guard fileSize > 0 else {
            throw ScoutProcessError.invalidImage(url.lastPathComponent)
        }
    }

    private func uniqueDestinationURL(in parent: URL, preferredName: String) -> URL {
        let sanitizedName = sanitizeFileNamePreservingExtension(preferredName)
        let baseURL = parent.appending(path: sanitizedName)
        guard fileManager.fileExists(atPath: baseURL.path) else { return baseURL }

        let name = baseURL.deletingPathExtension().lastPathComponent
        let ext = baseURL.pathExtension

        for index in 1...999 {
            let candidateName = ext.isEmpty ? "\(name)_\(index)" : "\(name)_\(index).\(ext)"
            let candidate = parent.appending(path: candidateName)
            if !fileManager.fileExists(atPath: candidate.path) {
                return candidate
            }
        }

        return parent.appending(path: UUID().uuidString + (ext.isEmpty ? "" : ".\(ext)"))
    }

    private func moveReplacingIfNeeded(from source: URL, to destination: URL) throws {
        if fileManager.fileExists(atPath: destination.path) {
            try fileManager.removeItem(at: destination)
        }
        try fileManager.moveItem(at: source, to: destination)
    }

    private func updateQueue(_ fileName: String, status: QueueStatus, detail: String, destinationURL: URL? = nil) {
        onQueueUpdate?(QueueUpdate(fileName: fileName, status: status, detail: detail, updatedAt: Date(), destinationURL: destinationURL))
    }

    private func log(_ message: String) {
        onLog?("\(Self.timeOnlyFormatter.string(from: Date())) \(message)")
    }

    private func sanitizeArchiveComponent(_ value: String) -> String {
        let replaced = value.unicodeScalars.map { scalar -> Character in
            if CharacterSet.controlCharacters.contains(scalar) || scalar == "/" || scalar == ":" || scalar == "\\" {
                return " "
            }
            return Character(scalar)
        }

        let collapsed = String(replaced).replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
        let trimmed = collapsed.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "Item" : trimmed
    }

    private func sanitize(_ value: String) -> String {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        let mapped = trimmed.map { character -> Character in
            if character.isLetter || character.isNumber {
                return character
            }
            return "_"
        }
        let collapsed = String(mapped).replacingOccurrences(of: "_+", with: "_", options: .regularExpression)
        return collapsed.trimmingCharacters(in: CharacterSet(charactersIn: "_")).isEmpty ? "Item" : collapsed.trimmingCharacters(in: CharacterSet(charactersIn: "_"))
    }

    private func sanitizeFileNamePreservingExtension(_ value: String) -> String {
        let url = URL(fileURLWithPath: value)
        let name = ScoutShotNaming.sanitizeComponent(url.deletingPathExtension().lastPathComponent)
        let ext = url.pathExtension
        return ext.isEmpty ? name : "\(name).\(ext)"
    }

    private func updateShotAssetMetadata(
        sessionID: String?,
        shot: SessionManifest.Shot,
        session: SessionManifest,
        stampedFilename: String
    ) throws {
        guard let sessionID, let dbQueue = DatabaseManager.shared.dbQueue else { return }

        let shotKey = shot.shotKey?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
            ? shot.shotKey!
            : "\(shot.detailType ?? session.detailType ?? "Shot") A\(shot.angleIndex ?? 0)"
        let logicalShotIdentity = shot.logicalShotIdentity?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
            ? shot.logicalShotIdentity!
            : shotKey

        let flaggedReason = shot.currentReason?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
            ? shot.currentReason
            : nil

        try dbQueue.write { db in
            try db.execute(
                sql: """
                UPDATE shots
                SET stamped_jpeg_filename = ?,
                    flagged_reason = COALESCE(?, flagged_reason)
                WHERE session_id = ? AND logical_shot_identity = ?
                """,
                arguments: [stampedFilename, flaggedReason, sessionID, logicalShotIdentity]
            )

            if db.changesCount == 0, let originalFilename = shot.originalFilename?.trimmingCharacters(in: .whitespacesAndNewlines), originalFilename.isEmpty == false {
                try db.execute(
                    sql: """
                    UPDATE shots
                    SET stamped_jpeg_filename = ?,
                        flagged_reason = COALESCE(?, flagged_reason)
                    WHERE session_id = ? AND original_filename = ?
                    """,
                    arguments: [stampedFilename, flaggedReason, sessionID, originalFilename]
                )
            }

            if db.changesCount == 0 {
                try db.execute(
                    sql: """
                    UPDATE shots
                    SET stamped_jpeg_filename = ?,
                        flagged_reason = COALESCE(?, flagged_reason)
                    WHERE session_id = ? AND shot_key = ?
                    """,
                    arguments: [stampedFilename, flaggedReason, sessionID, shotKey]
                )
            }
        }
    }

    private func resolveSessionIDFromSessionsCSV(in sessionFolderURL: URL) -> String? {
        let sessionsCSVURL = sessionFolderURL.appending(path: "sessions.csv")
        guard let data = try? Data(contentsOf: sessionsCSVURL),
              let text = String(data: data, encoding: .utf8) ?? String(data: data, encoding: .ascii)
        else {
            return nil
        }

        let lines = text
            .split(whereSeparator: \.isNewline)
            .map(String.init)
            .filter { $0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false }
        guard lines.count >= 2 else { return nil }

        let header = parseCSVLine(lines[0]).map { canonicalCSVKey($0) }
        guard let sessionIDIndex = header.firstIndex(of: "session_id") else { return nil }

        for line in lines.dropFirst() {
            let values = parseCSVLine(line)
            guard values.indices.contains(sessionIDIndex) else { continue }
            let sessionID = values[sessionIDIndex].trimmingCharacters(in: .whitespacesAndNewlines)
            if sessionID.isEmpty == false {
                return sessionID
            }
        }
        return nil
    }

    private func canonicalCSVKey(_ raw: String) -> String {
        raw
            .trimmingCharacters(in: .whitespacesAndNewlines)
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

    private func fingerprint(for url: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }

        var hasher = SHA256()
        while true {
            let data = try handle.read(upToCount: 1_048_576) ?? Data()
            if data.isEmpty {
                break
            }
            hasher.update(data: data)
        }

        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }

    private struct PipelineResult {
        let processedFolder: URL
    }

    private struct FileObservation {
        let firstDetectedAt: Date
        let lastSize: Int
        let lastModifiedAt: Date
        let stableSince: Date
    }

    private static let folderStampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        return formatter
    }()

    private static let overlayDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "MM/dd/yyyy h:mma"
        return formatter
    }()

    private static let overlayDateOnlyFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "MM/dd/yyyy"
        return formatter
    }()

    private static let timeOnlyFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()

    private static let logDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss Z"
        return formatter
    }()

    private static let archiveDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()

    private static let inputDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        return formatter
    }()

    private static let isoDateFormatter = ISO8601DateFormatter()

    private static func parseDate(_ rawValue: String) -> Date? {
        if let date = inputDateFormatter.date(from: rawValue) {
            return date
        }

        if let date = isoDateFormatter.date(from: rawValue) {
            return date
        }

        let localISOFormatter = ISO8601DateFormatter()
        localISOFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return localISOFormatter.date(from: rawValue)
    }
}
