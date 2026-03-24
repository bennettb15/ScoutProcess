//
//  ContentView.swift
//  ScoutProcess
//
//  Created by Brian Bennett on 2/28/26.
//

import SwiftUI
import AppKit

struct ContentView: View {
    @Environment(ScoutProcessModel.self) private var model
    @Environment(\.colorScheme) private var colorScheme
    @State private var selectedTab: DashboardTab = .pipeline
    @State private var isLogVisible = false
    @State private var regenStampedImagery = true
    @State private var regenPropertyReport = true
    @State private var regenFlaggedItems = false
    @State private var regenPriorityItems = false

    private var canRunRegeneration: Bool {
        model.selectedRegenSessionFolderURL != nil && (
            regenStampedImagery ||
            regenPropertyReport ||
            regenFlaggedItems ||
            regenPriorityItems
        )
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            header
            tabSelector
            if selectedTab == .pipeline {
                queueSection
                logSection
            } else if selectedTab == .regen {
                regenSection
            } else {
                PunchListView()
            }
            footer
        }
        .padding(20)
        .frame(minWidth: 860, minHeight: 620)
    }

    private enum DashboardTab: String, CaseIterable, Identifiable {
        case pipeline
        case regen
        case punchList

        var id: String { rawValue }

        var label: String {
            switch self {
            case .pipeline: return "Pipeline"
            case .regen: return "Regen"
            case .punchList: return "Punch List"
            }
        }
    }

    private var header: some View {
        HStack(alignment: .top) {
            VStack(alignment: .leading, spacing: 8) {
                Image(colorScheme == .dark ? "ScoutProcessLogoWhite" : "ScoutProcessLogoBlue")
                    .resizable()
                    .scaledToFit()
                    .frame(height: 52)
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 8) {
                Text("Last processed")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Text(model.lastProcessedText)
                    .font(.headline)
                HStack(spacing: 10) {
                    Button("Scout Archive") {
                        model.openDirectory(.archive)
                    }
                    Button("Scout Deliverables") {
                        model.openDirectory(.deliverables)
                    }
                    Button {
                        model.openDirectory(.input)
                    } label: {
                        HStack(spacing: 8) {
                            Text("Input")

                            if model.inputItemCount > 0 {
                                Text("\(model.inputItemCount)")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 7)
                                    .padding(.vertical, 2)
                                    .background(Color.orange, in: Capsule())
                                    .foregroundStyle(.white)
                            }
                        }
                    }
                    Button {
                        model.openDirectory(.duplicate)
                    } label: {
                        HStack(spacing: 8) {
                            Text("Duplicate")

                            if model.duplicateItemCount > 0 {
                                Text("\(model.duplicateItemCount)")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 7)
                                    .padding(.vertical, 2)
                                    .background(Color.yellow, in: Capsule())
                                    .foregroundStyle(.black)
                            }
                        }
                    }
                    Button {
                        model.openDirectory(.failed)
                    } label: {
                        HStack(spacing: 8) {
                            Text("Failed")

                            if model.failedItemCount > 0 {
                                Text("\(model.failedItemCount)")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 7)
                                    .padding(.vertical, 2)
                                    .background(Color.red, in: Capsule())
                                    .foregroundStyle(.white)
                            }
                        }
                    }
                }

                Toggle("Show Log", isOn: $isLogVisible)
                    .toggleStyle(.checkbox)
            }
        }
    }

    private var tabSelector: some View {
        Picker("Mode", selection: $selectedTab) {
            ForEach(DashboardTab.allCases) { tab in
                Text(tab.label).tag(tab)
            }
        }
        .pickerStyle(.segmented)
        .frame(maxWidth: 320)
    }

    private var queueSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Queue")
                    .font(.title3.weight(.semibold))

                Spacer()

                if model.queueItems.contains(where: { $0.status == .done }) {
                    Button("Clear Completed") {
                        model.clearCompletedQueueItems()
                    }
                    .buttonStyle(.plain)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 7)
                    .background(Color.blue, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                    .foregroundStyle(.white)
                }
            }

            ZStack {
                Table(model.queueItems) {
                    TableColumn("ZIP") { item in
                        Text(item.fileName)
                    }
                    TableColumn("Status") { item in
                        HStack(spacing: 8) {
                            if item.status == .processing {
                                ProgressView()
                                    .controlSize(.small)
                            }
                            Text(item.status.label)
                                .foregroundStyle(item.status.color)
                        }
                    }
                    TableColumn("Updated") { item in
                        Text(item.updatedAt.formatted(date: .abbreviated, time: .standard))
                            .foregroundStyle(.secondary)
                    }
                    TableColumn("Details") { item in
                        Text(item.detail)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                    }
                    TableColumn("Action") { item in
                        HStack(spacing: 8) {
                            if item.status == .done, item.destinationURL != nil {
                                Button("View Files") {
                                    model.openQueueItemDestination(item)
                                }
                                .buttonStyle(.plain)
                                .padding(.horizontal, 10)
                                .padding(.vertical, 6)
                                .background(Color.blue, in: RoundedRectangle(cornerRadius: 8))
                                .foregroundStyle(.white)
                            }

                            if item.status != .processing {
                                Button {
                                    model.removeQueueItem(item)
                                } label: {
                                    Image(systemName: "xmark")
                                        .font(.system(size: 11, weight: .bold))
                                        .frame(width: 24, height: 24)
                                }
                                .buttonStyle(.plain)
                                .background(Color.secondary.opacity(0.16), in: Circle())
                                .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
                .id(model.queueRefreshID)
                .frame(minHeight: 250)

                RoundedRectangle(cornerRadius: 14)
                    .strokeBorder(model.isDropTargeted ? .blue : .clear, style: StrokeStyle(lineWidth: 3, dash: [10, 8]))
                    .background(
                        RoundedRectangle(cornerRadius: 14)
                            .fill(model.isDropTargeted ? Color.blue.opacity(0.10) : .clear)
                    )

                if model.isDropTargeted {
                    VStack(spacing: 8) {
                        Image(systemName: "tray.and.arrow.down.fill")
                            .font(.system(size: 28, weight: .semibold))
                            .foregroundStyle(.blue)
                        Text("Drop ZIP Here")
                            .font(.headline)
                        Text("The file will be copied to ~/Documents/ScoutProcess/Input/")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .padding(24)
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14))
                }
            }
            .dropDestination(for: URL.self) { items, _ in
                var importedAny = false
                for url in items {
                    guard url.pathExtension.lowercased() == "zip" else { continue }
                    model.importDroppedZip(url)
                    importedAny = true
                }
                model.isDropTargeted = false
                return importedAny
            } isTargeted: { isTargeted in
                model.isDropTargeted = isTargeted
            }
        }
    }

    private var logSection: some View {
        Group {
            if isLogVisible {
                VStack(alignment: .leading, spacing: 10) {
                    HStack {
                        Text("Log")
                            .font(.title3.weight(.semibold))

                        Spacer()

                        Button("Copy Log") {
                            let logText = model.logLines.joined(separator: "\n")
                            NSPasteboard.general.clearContents()
                            NSPasteboard.general.setString(logText, forType: .string)
                        }
                        .disabled(model.logLines.isEmpty)
                    }

                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 6) {
                        ForEach(Array(model.logLines.enumerated()), id: \.offset) { _, line in
                            Text(line)
                                .font(.system(.caption, design: .monospaced))
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                    }
                    .textSelection(.enabled)
                }
                .padding(12)
                .background(.quaternary.opacity(0.45), in: RoundedRectangle(cornerRadius: 12))
                .frame(maxHeight: .infinity)
                }
            }
        }
    }

    private var regenSection: some View {
        VStack(alignment: .leading, spacing: 16) {
            VStack(alignment: .leading, spacing: 8) {
                Text("Regenerate Deliverables")
                    .font(.title3.weight(.semibold))
                Text("Rebuild selected stamped imagery and PDFs using data stored in Scout Archive, then choose where to save the regenerated outputs.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            VStack(alignment: .leading, spacing: 14) {
                HStack(alignment: .top) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text("Archive Session")
                            .font(.headline)
                        Text(model.selectedRegenSessionFolderURL?.tildePath ?? "No archive session selected")
                            .font(.caption.monospaced())
                            .foregroundStyle(.secondary)
                            .textSelection(.enabled)
                    }

                    Spacer()

                    Button("Choose Session...") {
                        model.chooseArchiveSessionForRegeneration()
                    }
                }

                Divider()

                VStack(alignment: .leading, spacing: 10) {
                    Text("Deliverables")
                        .font(.headline)

                    Toggle("Stamped Imagery", isOn: $regenStampedImagery)
                    Toggle("Property Report PDF", isOn: $regenPropertyReport)
                    Toggle("Flagged Comparison Report PDF", isOn: $regenFlaggedItems)
                    Toggle("Priority Report PDF", isOn: $regenPriorityItems)
                }

                Divider()

                HStack {
                    Text(model.regenStatusMessage)
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    Spacer()

                    if model.isRegenerating {
                        ProgressView()
                            .controlSize(.small)
                    }

                    Button("Regen Selected") {
                        model.regenerateArchiveSession(
                            selection: RegenerationSelection(
                                stampedImagery: regenStampedImagery,
                                propertyReport: regenPropertyReport,
                                flaggedItems: regenFlaggedItems,
                                priorityItems: regenPriorityItems
                            )
                        )
                    }
                    .buttonStyle(.plain)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 7)
                    .background(canRunRegeneration ? Color.blue : Color.secondary.opacity(0.18), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                    .foregroundStyle(canRunRegeneration ? .white : .secondary)
                    .disabled(model.isRegenerating || !canRunRegeneration)
                }
            }
            .padding(16)
            .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 14))

            Spacer()
        }
    }

    private var footer: some View {
        HStack {
            Text("Files are processed one at a time after their size is stable.")
                .font(.caption)
                .foregroundStyle(.secondary)

            Spacer()

            HStack(spacing: 8) {
                Circle()
                    .fill(model.isWatcherActive ? Color.green : Color.orange)
                    .frame(width: 8, height: 8)
                Text("Watcher")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
            }
        }
    }
}

#Preview {
    ContentView()
        .environment(ScoutProcessModel.preview)
}
