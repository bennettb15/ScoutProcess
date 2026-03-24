//
//  ScoutProcessModel.swift
//  ScoutProcess
//

import AppKit
import Foundation
import Observation
import SwiftUI

@MainActor
@Observable
final class ScoutProcessModel {
    enum AppState {
        case idle
        case processing
        case error

        var label: String {
            switch self {
            case .idle: "Idle"
            case .processing: "Processing"
            case .error: "Error"
            }
        }

        var symbolName: String {
            switch self {
            case .idle: "pause.circle"
            case .processing: "gearshape.2"
            case .error: "exclamationmark.triangle"
            }
        }

        var color: Color {
            switch self {
            case .idle: .secondary
            case .processing: .blue
            case .error: .red
            }
        }
    }

    enum DirectoryKind {
        case input
        case duplicate
        case archive
        case deliverables
        case failed
    }

    struct QueueItem: Identifiable {
        let id: UUID
        let fileName: String
        var status: QueueStatus
        var detail: String
        var updatedAt: Date
        var destinationURL: URL?
    }

    enum QueueStatus {
        case pending
        case processing
        case done
        case failed

        var label: String {
            switch self {
            case .pending: "Pending"
            case .processing: "Processing"
            case .done: "Done"
            case .failed: "Failed"
            }
        }

        var color: Color {
            switch self {
            case .pending: .secondary
            case .processing: .blue
            case .done: .green
            case .failed: .red
            }
        }
    }

    var directories: ScoutProcessDirectories
    var state: AppState = .idle
    var queueItems: [QueueItem] = []
    var queueRefreshID = UUID()
    var logLines: [String] = []
    var lastProcessedAt: Date?
    var isDropTargeted = false
    var inputItemCount = 0
    var duplicateItemCount = 0
    var failedItemCount = 0
    var isWatcherActive = false
    var selectedRegenSessionFolderURL: URL?
    var regenStatusMessage = "Choose an archived session to regenerate deliverables from Scout Archive."
    var isRegenerating = false

    private var controller: ScoutProcessController
    private let regenerationService = ArchiveRegenerationService()
    private let maxLogLines = 250
    private let inputMonitorQueue = DispatchQueue(label: "com.bennett.scoutprocess.input-monitor", qos: .utility)
    private var inputDirectoryFileDescriptor: CInt = -1
    private var inputDirectoryMonitor: DispatchSourceFileSystemObject?
    private let duplicateMonitorQueue = DispatchQueue(label: "com.bennett.scoutprocess.duplicate-monitor", qos: .utility)
    private var duplicateDirectoryFileDescriptor: CInt = -1
    private var duplicateDirectoryMonitor: DispatchSourceFileSystemObject?
    private let failedMonitorQueue = DispatchQueue(label: "com.bennett.scoutprocess.failed-monitor", qos: .utility)
    private var failedDirectoryFileDescriptor: CInt = -1
    private var failedDirectoryMonitor: DispatchSourceFileSystemObject?
    private var memoryLogTimer: DispatchSourceTimer?

    init() {
        let directories = ScoutProcessDirectories.defaultDirectories()
        self.directories = directories
        self.controller = ScoutProcessController(directories: directories)
        configureControllerCallbacks()
    }

    var archiveLocationText: String {
        directories.archiveRoot.tildePath
    }

    var deliverablesLocationText: String {
        directories.deliverablesRoot.tildePath
    }

    private func configureControllerCallbacks() {
        controller.onLog = { [weak self] line in
            Task { @MainActor [weak self] in
                self?.appendLog(line)
            }
        }

        controller.onStateChange = { [weak self] newState in
            Task { @MainActor [weak self] in
                self?.state = newState
            }
        }

        controller.onQueueUpdate = { [weak self] update in
            Task { @MainActor [weak self] in
                self?.applyQueueUpdate(update)
                self?.refreshInputItemCount()
                self?.refreshDuplicateItemCount()
                self?.refreshFailedItemCount()
            }
        }

        controller.onLastProcessed = { [weak self] date in
            Task { @MainActor [weak self] in
                self?.lastProcessedAt = date
            }
        }
    }

    func start() {
        controller.start()
        isWatcherActive = true
        refreshInputItemCount()
        refreshDuplicateItemCount()
        refreshFailedItemCount()
#if DEBUG
        logLaunchDiagnostics()
        startMemoryDiagnostics()
#endif
        startInputDirectoryMonitor()
        startDuplicateDirectoryMonitor()
        startFailedDirectoryMonitor()
    }

    func stop() {
        controller.stop()
        isWatcherActive = false
#if DEBUG
        stopMemoryDiagnostics()
#endif
        stopInputDirectoryMonitor()
        stopDuplicateDirectoryMonitor()
        stopFailedDirectoryMonitor()
    }

    func openDirectory(_ kind: DirectoryKind) {
        let url: URL = switch kind {
        case .input: directories.input
        case .duplicate: directories.duplicate
        case .archive: directories.archiveRoot
        case .deliverables: directories.deliverablesRoot
        case .failed: directories.failed
        }

        NSWorkspace.shared.open(url)

        if kind == .input {
            refreshInputItemCount()
        }

        if kind == .duplicate {
            refreshDuplicateItemCount()
        }

        if kind == .failed {
            refreshFailedItemCount()
        }
    }

    func openQueueItemDestination(_ item: QueueItem) {
        guard let destinationURL = item.destinationURL else { return }
        NSWorkspace.shared.open(destinationURL)
    }

    func chooseArchiveLocation() {
        guard let selectedURL = chooseDirectory(
            title: "Choose Scout Archive Location",
            initialDirectory: directories.archiveRoot
        ) else {
            return
        }

        ScoutProcessLocationSettings.setArchiveRootURL(selectedURL)
        updateDirectories()
        appendLog("Scout Archive location updated: \(selectedURL.tildePath)")
    }

    func chooseDeliverablesLocation() {
        guard let selectedURL = chooseDirectory(
            title: "Choose Scout Deliverables Location",
            initialDirectory: directories.deliverablesRoot
        ) else {
            return
        }

        ScoutProcessLocationSettings.setDeliverablesRootURL(selectedURL)
        updateDirectories()
        appendLog("Scout Deliverables location updated: \(selectedURL.tildePath)")
    }

    func chooseArchiveSessionForRegeneration() {
        guard let selectedURL = chooseDirectory(
            title: "Choose Archived Session Folder",
            initialDirectory: directories.archiveClientsRoot
        ) else {
            return
        }

        selectedRegenSessionFolderURL = selectedURL
        regenStatusMessage = "Selected archive session: \(selectedURL.lastPathComponent)"
    }

    func regenerateArchiveSession(selection: RegenerationSelection) {
        guard let selectedRegenSessionFolderURL else {
            regenStatusMessage = "Choose an archived session folder first."
            return
        }

        isRegenerating = true
        regenStatusMessage = "Regenerating selected deliverables from \(selectedRegenSessionFolderURL.lastPathComponent)..."

        Task {
            do {
                let result = try regenerationService.regenerate(
                    archiveSessionFolderURL: selectedRegenSessionFolderURL,
                    selection: selection
                )

                guard let destinationURL = chooseDirectory(
                    title: "Choose Destination for Regenerated Deliverables",
                    initialDirectory: directories.deliverablesRoot
                ) else {
                    try? FileManager.default.removeItem(at: result.generatedSessionFolder.deletingLastPathComponent())
                    await MainActor.run {
                        self.isRegenerating = false
                        self.regenStatusMessage = "Regeneration finished, but destination selection was canceled."
                    }
                    return
                }

                let exportedURL = try regenerationService.export(
                    result: result,
                    selection: selection,
                    destinationRootURL: destinationURL
                )

                await MainActor.run {
                    self.isRegenerating = false
                    self.regenStatusMessage = "Regenerated deliverables saved to \(exportedURL.tildePath)"
                }
            } catch {
                await MainActor.run {
                    self.isRegenerating = false
                    self.regenStatusMessage = "Regeneration failed: \(error.localizedDescription)"
                }
            }
        }
    }

    func removeQueueItem(_ item: QueueItem) {
        queueItems.removeAll { $0.id == item.id }
        queueRefreshID = UUID()
    }

    func clearCompletedQueueItems() {
        queueItems.removeAll { $0.status == .done }
        queueRefreshID = UUID()
    }

    func importDroppedZip(_ sourceURL: URL) {
        guard sourceURL.pathExtension.lowercased() == "zip" else {
            appendLog("Ignored dropped file: \(sourceURL.lastPathComponent) is not a .zip")
            return
        }

        do {
            try FileManager.default.createDirectory(at: directories.input, withIntermediateDirectories: true)

            if sourceURL.deletingLastPathComponent() == directories.input {
                appendLog("Dropped ZIP already in Input: \(sourceURL.lastPathComponent)")
                controller.enqueueImportedZip(sourceURL)
                return
            }

            let destinationURL = uniqueImportDestination(for: sourceURL.lastPathComponent)
            try FileManager.default.copyItem(at: sourceURL, to: destinationURL)
            appendLog("Imported dropped ZIP to Input: \(destinationURL.lastPathComponent)")
            controller.enqueueImportedZip(destinationURL)
        } catch {
            appendLog("Failed to import dropped ZIP \(sourceURL.lastPathComponent): \(error.localizedDescription)")
            state = .error
        }
    }

    var lastProcessedText: String {
        guard let lastProcessedAt else { return "Never" }
        return lastProcessedAt.formatted(date: .abbreviated, time: .standard)
    }

    private func appendLog(_ line: String) {
        logLines.append(line)
        if logLines.count > maxLogLines {
            logLines.removeFirst(logLines.count - maxLogLines)
        }
    }

    private func chooseDirectory(title: String, initialDirectory: URL) -> URL? {
        let panel = NSOpenPanel()
        panel.title = title
        panel.prompt = "Choose"
        panel.directoryURL = initialDirectory
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = true
        return panel.runModal() == .OK ? panel.urls.first : nil
    }

    private func updateDirectories() {
        let wasWatcherActive = isWatcherActive
        if wasWatcherActive {
            stop()
        }

        directories = ScoutProcessDirectories.defaultDirectories()
        controller = ScoutProcessController(directories: directories)
        configureControllerCallbacks()

        if wasWatcherActive {
            start()
        } else {
            refreshInputItemCount()
            refreshDuplicateItemCount()
            refreshFailedItemCount()
        }

        queueRefreshID = UUID()
    }

    private func uniqueImportDestination(for fileName: String) -> URL {
        let candidate = directories.input.appending(path: fileName)
        guard !FileManager.default.fileExists(atPath: candidate.path) else {
            let base = candidate.deletingPathExtension().lastPathComponent
            let ext = candidate.pathExtension

            for index in 1...999 {
                let nextName = ext.isEmpty ? "\(base)_\(index)" : "\(base)_\(index).\(ext)"
                let nextURL = directories.input.appending(path: nextName)
                if !FileManager.default.fileExists(atPath: nextURL.path) {
                    return nextURL
                }
            }

            return directories.input.appending(path: UUID().uuidString + ".zip")
        }

        return candidate
    }

    private func applyQueueUpdate(_ update: ScoutProcessController.QueueUpdate) {
        if let index = queueItems.firstIndex(where: { $0.fileName == update.fileName }) {
            queueItems[index].status = update.status
            queueItems[index].detail = update.detail
            queueItems[index].updatedAt = update.updatedAt
            queueItems[index].destinationURL = update.destinationURL
            return
        }

        queueItems.append(
            QueueItem(
                id: UUID(),
                fileName: update.fileName,
                status: update.status,
                detail: update.detail,
                updatedAt: update.updatedAt,
                destinationURL: update.destinationURL
            )
        )
        queueItems.sort { $0.updatedAt > $1.updatedAt }
    }

    private func refreshFailedItemCount() {
        let fileManager = FileManager.default

        guard let contents = try? fileManager.contentsOfDirectory(
            at: directories.failed,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        ) else {
            failedItemCount = 0
            return
        }

        let directoriesOnly = contents.filter {
            (try? $0.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true
        }
        let failedSessionFolders = directoriesOnly.filter { directoryURL in
            fileManager.fileExists(atPath: directoryURL.appending(path: "error.txt").path)
        }
        let zipFiles = contents.filter { $0.pathExtension.lowercased() == "zip" }

        // Treat the failed session folder as the canonical failed item.
        // Only count standalone ZIPs when no failed session folder exists.
        failedItemCount = failedSessionFolders.isEmpty ? zipFiles.count : failedSessionFolders.count
    }

    private func refreshDuplicateItemCount() {
        let fileManager = FileManager.default

        guard let contents = try? fileManager.contentsOfDirectory(
            at: directories.duplicate,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            duplicateItemCount = 0
            return
        }

        duplicateItemCount = contents.filter { url in
            guard url.pathExtension.lowercased() == "zip" else { return false }
            return (try? url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true
        }.count
    }

    private func refreshInputItemCount() {
        let fileManager = FileManager.default

        guard let contents = try? fileManager.contentsOfDirectory(
            at: directories.input,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            inputItemCount = 0
            return
        }

        inputItemCount = contents.filter { url in
            guard url.pathExtension.lowercased() == "zip" else { return false }
            return (try? url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true
        }.count
    }

    private func startInputDirectoryMonitor() {
        stopInputDirectoryMonitor()

        inputDirectoryFileDescriptor = open(directories.input.path, O_EVTONLY)
        guard inputDirectoryFileDescriptor >= 0 else { return }

        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: inputDirectoryFileDescriptor,
            eventMask: [.write, .rename, .delete, .extend, .attrib, .link],
            queue: inputMonitorQueue
        )

        source.setEventHandler { [weak self] in
            Task { @MainActor [weak self] in
                self?.refreshInputItemCount()
            }
        }

        source.setCancelHandler { [weak self] in
            guard let self else { return }
            if self.inputDirectoryFileDescriptor >= 0 {
                close(self.inputDirectoryFileDescriptor)
                self.inputDirectoryFileDescriptor = -1
            }
        }

        inputDirectoryMonitor = source
        source.resume()
    }

    private func stopInputDirectoryMonitor() {
        if let inputDirectoryMonitor {
            inputDirectoryMonitor.cancel()
            self.inputDirectoryMonitor = nil
        } else if inputDirectoryFileDescriptor >= 0 {
            close(inputDirectoryFileDescriptor)
            inputDirectoryFileDescriptor = -1
        }
    }

    private func startDuplicateDirectoryMonitor() {
        stopDuplicateDirectoryMonitor()

        duplicateDirectoryFileDescriptor = open(directories.duplicate.path, O_EVTONLY)
        guard duplicateDirectoryFileDescriptor >= 0 else { return }

        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: duplicateDirectoryFileDescriptor,
            eventMask: [.write, .rename, .delete, .extend, .attrib, .link],
            queue: duplicateMonitorQueue
        )

        source.setEventHandler { [weak self] in
            Task { @MainActor [weak self] in
                self?.refreshDuplicateItemCount()
            }
        }

        source.setCancelHandler { [weak self] in
            guard let self else { return }
            if self.duplicateDirectoryFileDescriptor >= 0 {
                close(self.duplicateDirectoryFileDescriptor)
                self.duplicateDirectoryFileDescriptor = -1
            }
        }

        duplicateDirectoryMonitor = source
        source.resume()
    }

    private func stopDuplicateDirectoryMonitor() {
        if let duplicateDirectoryMonitor {
            duplicateDirectoryMonitor.cancel()
            self.duplicateDirectoryMonitor = nil
        } else if duplicateDirectoryFileDescriptor >= 0 {
            close(duplicateDirectoryFileDescriptor)
            duplicateDirectoryFileDescriptor = -1
        }
    }

    private func startFailedDirectoryMonitor() {
        stopFailedDirectoryMonitor()

        failedDirectoryFileDescriptor = open(directories.failed.path, O_EVTONLY)
        guard failedDirectoryFileDescriptor >= 0 else { return }

        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: failedDirectoryFileDescriptor,
            eventMask: [.write, .rename, .delete, .extend, .attrib, .link],
            queue: failedMonitorQueue
        )

        source.setEventHandler { [weak self] in
            Task { @MainActor [weak self] in
                self?.refreshFailedItemCount()
            }
        }

        source.setCancelHandler { [weak self] in
            guard let self else { return }
            if self.failedDirectoryFileDescriptor >= 0 {
                close(self.failedDirectoryFileDescriptor)
                self.failedDirectoryFileDescriptor = -1
            }
        }

        failedDirectoryMonitor = source
        source.resume()
    }

    private func stopFailedDirectoryMonitor() {
        if let failedDirectoryMonitor {
            failedDirectoryMonitor.cancel()
            self.failedDirectoryMonitor = nil
        } else if failedDirectoryFileDescriptor >= 0 {
            close(failedDirectoryFileDescriptor)
            failedDirectoryFileDescriptor = -1
        }
    }

    #if DEBUG
    private func startMemoryDiagnostics() {
        stopMemoryDiagnostics()

        let timer = DispatchSource.makeTimerSource(queue: .global(qos: .utility))
        timer.schedule(deadline: .now(), repeating: .seconds(5))
        timer.setEventHandler { [weak self] in
            guard let self else { return }
            let residentBytes = MemoryDiagnostics.currentResidentSizeBytes()
            print("[Diagnostics] resident=\(MemoryDiagnostics.formatBytes(residentBytes)) (\(residentBytes) bytes)")
            Task { @MainActor [weak self] in
                self?.logRuntimeCounts()
            }
        }
        memoryLogTimer = timer
        timer.resume()
    }

    private func stopMemoryDiagnostics() {
        memoryLogTimer?.cancel()
        memoryLogTimer = nil
    }

    private func logLaunchDiagnostics() {
        let inputCount = countItems(in: directories.input)
        let workingCount = countItems(in: directories.working)
        let processedCount = countItems(in: directories.base.appending(path: "Processed", directoryHint: .isDirectory))
        let failedCount = countItems(in: directories.failed)
        let sessionCount = countDetectedSessions()
        let residentBytes = MemoryDiagnostics.currentResidentSizeBytes()

        print("[Diagnostics] launch folders input=\(inputCount) working=\(workingCount) processed=\(processedCount) failed=\(failedCount)")
        print("[Diagnostics] launch sessions detected=\(sessionCount)")
        print("[Diagnostics] launch arrays queueItems=\(queueItems.count) logLines=\(logLines.count)")

        let snapshot = controller.diagnosticsSnapshot()
        print("[Diagnostics] launch controller queuedZips=\(snapshot.queueCount) queuedPaths=\(snapshot.queuedPathsCount) observations=\(snapshot.observationsCount) processedThisRun=\(snapshot.processedThisRunCount)")
        print("[Diagnostics] launch resident=\(MemoryDiagnostics.formatBytes(residentBytes)) (\(residentBytes) bytes)")
    }

    private func logRuntimeCounts() {
        let snapshot = controller.diagnosticsSnapshot()
        print("[Diagnostics] runtime queueItems=\(queueItems.count) logLines=\(logLines.count) queuedZips=\(snapshot.queueCount) queuedPaths=\(snapshot.queuedPathsCount) observations=\(snapshot.observationsCount) processedThisRun=\(snapshot.processedThisRunCount)")
    }

    private func countItems(in directory: URL) -> Int {
        guard let contents = try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            return 0
        }

        return contents.count
    }

    private func countDetectedSessions() -> Int {
        guard let enumerator = FileManager.default.enumerator(
            at: directories.archiveRoot,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        ) else {
            return 0
        }

        var count = 0
        for case let url as URL in enumerator {
            guard url.lastPathComponent == "session.json" else { continue }
            count += 1
        }
        return count
    }
    #endif

    static var preview: ScoutProcessModel {
        let model = ScoutProcessModel()
        model.state = .processing
        model.lastProcessedAt = .now
        model.queueItems = [
            QueueItem(id: UUID(), fileName: "Session_A.zip", status: .processing, detail: "Generating stamped JPGs", updatedAt: .now, destinationURL: nil),
            QueueItem(id: UUID(), fileName: "Session_B.zip", status: .done, detail: "Archived", updatedAt: .now.addingTimeInterval(-3600), destinationURL: model.directories.deliverablesClientsRoot.appending(path: "ABC Property Management/Buffalo Wild Wings - 12345 Polaris Pkwy/2026-03-02_01")),
            QueueItem(id: UUID(), fileName: "Session_C.zip", status: .failed, detail: "session.json missing", updatedAt: .now.addingTimeInterval(-7200), destinationURL: nil)
        ]
        model.logLines = [
            "15:14:01 Watcher started. Watching: ~/Documents/ScoutProcess/Input/",
            "15:14:04 Detected: Session_A.zip (event: created)",
            "15:14:06 Unzipped into Working/Session_A_20260302_151406",
            "15:14:08 Rendering stamp for 18 shots"
        ]
        return model
    }
}
