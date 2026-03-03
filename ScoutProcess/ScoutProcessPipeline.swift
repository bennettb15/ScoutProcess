//
//  ScoutProcessPipeline.swift
//  ScoutProcess
//

import AppKit
import CoreGraphics
import Foundation
import ImageIO
import UniformTypeIdentifiers

struct ScoutProcessDirectories {
    let base: URL
    let input: URL
    let working: URL
    let failed: URL
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

    var errorDescription: String? {
        switch self {
        case .missingSessionJSON: "session.json is missing at the session root."
        case .missingOriginals: "Originals folder is missing at the session root."
        case .unzipFailed(let output): "Unzip failed. \(output)"
        case .noShots: "session.json contains no shots."
        case .sourceImageMissing(let name): "Source image missing: \(name)"
        case .invalidImage(let name): "Unable to decode image: \(name)"
        case .missingRequiredMetadata(let field): "Missing required metadata: \(field)"
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
    var building: String?
    var elevation: String?
    var detailType: String?
    var shots: [Shot]
    var issues: [Issue]?

    struct Shot: Decodable {
        var originalFilename: String?
        var stampedFilename: String?
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
    private let stabilityInterval: TimeInterval = 2.0
    private let pollIntervalNanoseconds: UInt64 = 1_000_000_000

    private enum ScanAction {
        case none
        case created
        case modified
        case queuedStable
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
            guard processedThisRun.contains(url.lastPathComponent) == false else { return false }
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
        for directory in [directories.base, directories.input, directories.working, directories.failed, directories.archiveRoot, directories.clientsRoot] {
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
                guard processedThisRun.contains(url.lastPathComponent) == false else { return .none }
                guard queuedPaths.contains(url.path) == false else { return .none }

                if let observation = observations[url.path] {
                    if observation.lastSize == size,
                       now.timeIntervalSince(observation.stableSince) >= stabilityInterval,
                       now.timeIntervalSince(modifiedAt) >= stabilityInterval {
                        queue.append(url)
                        queuedPaths.insert(url.path)
                        observations.removeValue(forKey: url.path)
                        return .queuedStable
                    }

                    if observation.lastSize != size || observation.lastModifiedAt != modifiedAt {
                        observations[url.path] = FileObservation(lastSize: size, lastModifiedAt: modifiedAt, stableSince: now)
                        return .modified
                    }

                    observations[url.path] = observation
                    return .none
                }

                observations[url.path] = FileObservation(lastSize: size, lastModifiedAt: modifiedAt, stableSince: now)
                return .created
            }

            switch action {
            case .created:
                log("Detected: \(url.lastPathComponent) (event: created)")
            case .modified:
                log("Detected: \(url.lastPathComponent) (event: modified)")
            case .queuedStable:
                updateQueue(url.lastPathComponent, status: .pending, detail: "Queued for processing")
                log("Detected stable ZIP \(url.lastPathComponent)")
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
            _ = withStateLock {
                processedThisRun.insert(zipURL.lastPathComponent)
            }
            onLastProcessed?(Date())
            log("Completed \(zipURL.lastPathComponent) -> \(result.processedFolder.lastPathComponent)")
            updateQueue(zipURL.lastPathComponent, status: .done, detail: "Archived", destinationURL: result.processedFolder)
            onStateChange?(.idle)
        } catch {
            _ = withStateLock {
                processedThisRun.insert(zipURL.lastPathComponent)
            }
            log("Failed \(zipURL.lastPathComponent): \(error.localizedDescription)")
            updateQueue(zipURL.lastPathComponent, status: .failed, detail: error.localizedDescription)
            onStateChange?(.error)
        }

        _ = withStateLock {
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
        let workingZipURL = try moveZipIntoWorking(zipURL)
        let sessionFolder = workingSessionFolderURL(for: workingZipURL)
        var processedCount = 0
        var lastStep = "initializing"

        do {
            lastStep = "unzipping archive"
            try unzip(zipURL: workingZipURL, destination: sessionFolder)
            log("Unzipped into \(sessionFolder.lastPathComponent)")

            lastStep = "validating contents"
            let resolvedSessionFolder = try resolveSessionRoot(in: sessionFolder)
            let sessionJSONURL = resolvedSessionFolder.appending(path: "session.json")
            let originalsURL = resolvedSessionFolder.appending(path: "Originals", directoryHint: .isDirectory)

            let manifest = try loadManifest(at: sessionJSONURL)
            let issueLookup = buildIssueLookup(from: manifest.issues ?? [])
            let archiveMetadata = try extractArchiveMetadata(from: manifest)

            try writeValidationFile(at: resolvedSessionFolder, manifest: manifest)

            lastStep = "generating stamped JPGs"
            let stampedURL = resolvedSessionFolder.appending(path: "Stamped", directoryHint: .isDirectory)
            try fileManager.createDirectory(at: stampedURL, withIntermediateDirectories: true)
            log("Rendering \(manifest.shots.count) stamped JPGs")

            for (index, shot) in manifest.shots.enumerated() {
                let sourceName = shot.originalFilename ?? ""
                guard !sourceName.isEmpty else {
                    throw ScoutProcessError.sourceImageMissing("Shot \(index + 1) originalFilename is empty")
                }
                let sourceURL = originalsURL.appending(path: sourceName)
                guard fileManager.fileExists(atPath: sourceURL.path) else {
                    throw ScoutProcessError.sourceImageMissing(sourceName)
                }

                let stampedName = makeStampedFilename(for: shot, session: manifest)
                let destinationURL = stampedURL.appending(path: stampedName)
                try stampImage(
                    sourceURL: sourceURL,
                    destinationURL: destinationURL,
                    overlay: makeOverlayLines(for: shot, session: manifest, issues: issueLookup)
                )
                processedCount += 1
            }

            lastStep = "routing archived session"
            let archivedSessionURL = try archiveSessionFolder(
                from: resolvedSessionFolder,
                metadata: archiveMetadata
            )

            if sessionFolder != resolvedSessionFolder, fileManager.fileExists(atPath: sessionFolder.path) {
                try? fileManager.removeItem(at: sessionFolder)
            }

            try fileManager.removeItem(at: workingZipURL)
            log("Deleted processed ZIP \(workingZipURL.lastPathComponent)")

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

    private func makeOverlayLines(
        for shot: SessionManifest.Shot,
        session: SessionManifest,
        issues: [String: String]
    ) -> [String] {
        let propertyName = shot.propertyName ?? session.propertyNameAtCapture ?? session.propertyName ?? "Property"
        let building = shot.building ?? session.building ?? "Building"
        let elevation = shot.elevation ?? session.elevation ?? "Elevation"
        let detailType = shot.detailType ?? session.detailType ?? "Detail"
        let angleIndex = shot.angleIndex ?? 0

        var lines = [
            propertyName,
            "\(building) | \(elevation) | \(detailType) | Angle \(angleIndex)",
            formatLocalTimestamp(shot.capturedAtLocal)
        ]

        if shot.isFlagged == true {
            let issueKey = shot.issueID ?? shot.issueId
            if let explicitReason = shot.currentReason, !explicitReason.isEmpty {
                lines.append("Note: \(explicitReason)")
            } else if let issueKey, let reason = issues[issueKey] {
                lines.append("Note: \(reason)")
            } else {
                lines.append("Note")
            }
        }

        return lines
    }

    private func makeStampedFilename(for shot: SessionManifest.Shot, session: SessionManifest) -> String {
        if let stampedFilename = shot.stampedFilename, !stampedFilename.isEmpty {
            let normalizedName = stampedFilename.lowercased().hasSuffix(".jpg") ? stampedFilename : "\(stampedFilename).jpg"
            return shot.isFlagged == true ? appendFlaggedSuffix(to: normalizedName) : normalizedName
        }

        let building = sanitize(shot.building ?? session.building ?? "Building")
        let elevation = sanitize(shot.elevation ?? session.elevation ?? "Elevation")
        let detailType = sanitize(shot.detailType ?? session.detailType ?? "Detail")
        let angle = shot.angleIndex ?? 0
        let timestamp = fallbackFileTimestamp(from: shot.capturedAtLocal)
        let baseName = "\(building)_\(elevation)_\(detailType)_A\(angle)_\(timestamp).jpg"
        return shot.isFlagged == true ? appendFlaggedSuffix(to: baseName) : baseName
    }

    private func appendFlaggedSuffix(to fileName: String) -> String {
        let url = URL(fileURLWithPath: fileName)
        let ext = url.pathExtension
        let baseName = url.deletingPathExtension().lastPathComponent

        guard baseName.hasSuffix("_Flagged") == false else {
            return fileName
        }

        return ext.isEmpty ? "\(baseName)_Flagged" : "\(baseName)_Flagged.\(ext)"
    }

    private func fallbackFileTimestamp(from capturedAtLocal: String?) -> String {
        guard let capturedAtLocal, let date = Self.inputDateFormatter.date(from: capturedAtLocal) ?? Self.isoDateFormatter.date(from: capturedAtLocal) else {
            return Self.fileDateFormatter.string(from: Date())
        }
        return Self.fileDateFormatter.string(from: date)
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

    private func stampImage(sourceURL: URL, destinationURL: URL, overlay: [String]) throws {
        guard let original = NSImage(contentsOf: sourceURL) else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        let size = original.size
        guard size.width > 0, size.height > 0 else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
        }

        let rendered = NSImage(size: size)
        rendered.lockFocus()

        original.draw(in: NSRect(origin: .zero, size: size))

        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .left
        paragraph.lineBreakMode = .byTruncatingTail

        let primaryFontSize = max(34, min(66, size.width * 0.034))
        let secondaryFontSize = max(28, min(54, size.width * 0.027))
        let issueFontSize = max(26, min(50, size.width * 0.026))

        let lineAttributes: [[NSAttributedString.Key: Any]] = overlay.enumerated().map { index, _ in
            let font: NSFont
            if index == 0 {
                font = NSFont.systemFont(ofSize: primaryFontSize, weight: .bold)
            } else if index == overlay.count - 1, overlay.count == 4 {
                font = NSFont.systemFont(ofSize: issueFontSize, weight: .semibold)
            } else {
                font = NSFont.systemFont(ofSize: secondaryFontSize, weight: .semibold)
            }

            return [
                .font: font,
                .foregroundColor: NSColor.white,
                .paragraphStyle: paragraph
            ]
        }

        let horizontalPadding = max(24, size.width * 0.026)
        let verticalPadding = max(18, size.height * 0.015)
        let interLineSpacing = max(8, size.height * 0.004)
        let bottomMargin = max(22, size.height * 0.02)

        var measuredRects: [NSRect] = []
        var textBlockWidth: CGFloat = 0
        var textBlockHeight: CGFloat = 0

        for (index, line) in overlay.enumerated() {
            let rect = (line as NSString).boundingRect(
                with: NSSize(width: size.width * 0.82, height: .greatestFiniteMagnitude),
                options: [.usesLineFragmentOrigin, .usesFontLeading],
                attributes: lineAttributes[index]
            ).integral
            measuredRects.append(rect)
            textBlockWidth = max(textBlockWidth, rect.width)
            textBlockHeight += rect.height
            if index < overlay.count - 1 {
                textBlockHeight += interLineSpacing
            }
        }

        let backgroundRect = NSRect(
            x: horizontalPadding,
            y: bottomMargin,
            width: min(size.width - (horizontalPadding * 2), textBlockWidth + (horizontalPadding * 0.95)),
            height: textBlockHeight + (verticalPadding * 2)
        )

        let cornerRadius = max(22, min(34, size.width * 0.016))
        let backgroundPath = NSBezierPath(roundedRect: backgroundRect, xRadius: cornerRadius, yRadius: cornerRadius)
        NSColor.black.withAlphaComponent(0.68).setFill()
        backgroundPath.fill()

        var currentY = backgroundRect.maxY - verticalPadding
        let textX = backgroundRect.minX + (horizontalPadding * 0.5)

        for (index, line) in overlay.enumerated() {
            let rect = measuredRects[index]
            currentY -= rect.height
            let textRect = NSRect(
                x: textX,
                y: currentY,
                width: backgroundRect.width - (horizontalPadding * 0.7),
                height: rect.height + 4
            )
            (line as NSString).draw(in: textRect, withAttributes: lineAttributes[index])
            currentY -= interLineSpacing
        }

        rendered.unlockFocus()

        guard let tiff = rendered.tiffRepresentation,
              let rep = NSBitmapImageRep(data: tiff),
              let cgImage = rep.cgImage else {
            throw ScoutProcessError.invalidImage(sourceURL.lastPathComponent)
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
        CGImageDestinationAddImage(destination, cgImage, properties as CFDictionary)
        CGImageDestinationFinalize(destination)
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
        let name = sanitize(url.deletingPathExtension().lastPathComponent)
        let ext = url.pathExtension
        return ext.isEmpty ? name : "\(name).\(ext)"
    }

    private struct PipelineResult {
        let processedFolder: URL
    }

    private struct FileObservation {
        let lastSize: Int
        let lastModifiedAt: Date
        let stableSince: Date
    }

    private static let folderStampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        return formatter
    }()

    private static let fileDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        return formatter
    }()

    private static let overlayDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "MM-dd-yyyy hh:mm:ss a"
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
