//
//  PunchListView.swift
//  ScoutProcess
//

import AppKit
import SwiftUI
import UniformTypeIdentifiers

struct PunchListView: View {
    private let controlButtonWidth: CGFloat = 112
    private let rowMediaControlSize: CGFloat = 84
    private let rowControlLabelWidth: CGFloat = 64
    private let neutralButtonGray = Color(red: 0.24, green: 0.28, blue: 0.34)
    @State private var selectedStatus: PunchListStatus? = nil
    @State private var selectedPriority: PunchListPriority? = nil
    @State private var selectedAssigneeFilter: String? = nil
    @State private var selectedTradeFilter: PunchListTrade? = nil
    @State private var selectedPropertyFilter: String? = nil
    @State private var selectedOrganizationFilter: String? = nil
    @State private var pastDueOnly = false
    @State private var allItems: [PunchListItemSummary] = []
    @State private var items: [PunchListItemSummary] = []
    @State private var selectedItemID: Int64?
    @State private var isLoading = false
    @State private var loadError: String?
    @State private var assignedToDraft = ""
    @State private var dueDateDraft = ""
    @State private var resolutionNoteDraft = ""
    @State private var statusDraft: PunchListStatus = .active
    @State private var priorityDraft: PunchListPriority = .medium
    @State private var tradeDraft: PunchListTrade = .general
    @State private var isSaving = false
    @State private var relatedShots: [PunchListRelatedShot] = []
    @State private var relatedShotPreviews: [String: NSImage] = [:]
    @State private var isUploadingResolution = false
    @State private var uploadMessage: String?
    @State private var showingGallery = false
    @State private var galleryStartIndex = 0
    @State private var showingClearDevDataAlert = false
    @State private var isClearingDevData = false
    @State private var updatingRowItemIDs: Set<Int64> = []
    @State private var priorityPopoverItemID: Int64?
    @State private var tradePopoverItemID: Int64?
    @State private var assigneePopoverItemID: Int64?
    @State private var statusPopoverItemID: Int64?
    @State private var dueDatePopoverItemID: Int64?
    @State private var rowThumbnailImages: [Int64: NSImage] = [:]
    @State private var loadingThumbnailItemIDs: Set<Int64> = []
    @State private var resolutionPromptItem: PunchListItemSummary?
    @State private var resolutionPromptNote = ""
    @State private var activeTopFilterPopover: TopFilterPopover?
    @State private var showFiltersOverlay = false
    @State private var selectedPunchListPage: PunchListPage = .open
    @AppStorage("PunchListCustomAssigneesJSON") private var customAssigneesStorage = "[]"
    @AppStorage("PunchListCustomTradesJSON") private var customTradesStorage = "[]"
    @AppStorage("PunchListTradeOrderJSON") private var tradeOrderStorage = "[]"
    @AppStorage("PunchListAssigneeOrderJSON") private var assigneeOrderStorage = "[]"
    @State private var newAssigneeInput = ""
    @State private var newTradeInput = ""
    @State private var showManageAssigneesSheet = false
    @State private var showManageTradesSheet = false
    private static let unassignedAssigneeFilterValue = "__UNASSIGNED__"
    
    private enum TopFilterPopover: Hashable {
        case status
        case priority
        case organization
        case property
        case assignee
        case trade
    }

    private enum PunchListPage: String, CaseIterable, Identifiable {
        case open = "Open"
        case resolved = "Resolved"

        var id: String { rawValue }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            filterBar
            itemList
                .frame(maxHeight: .infinity)
        }
        .task { await reloadItems() }
        .onChange(of: selectedStatus) { _, _ in Task { await reloadItems() } }
        .onChange(of: selectedPriority) { _, _ in applyCurrentFilters() }
        .onChange(of: selectedPropertyFilter) { _, _ in applyCurrentFilters() }
        .onChange(of: selectedOrganizationFilter) { _, _ in applyCurrentFilters() }
        .onChange(of: selectedAssigneeFilter) { _, _ in applyCurrentFilters() }
        .onChange(of: selectedTradeFilter) { _, _ in applyCurrentFilters() }
        .onChange(of: pastDueOnly) { _, _ in applyCurrentFilters() }
        .onChange(of: selectedPunchListPage) { _, _ in applyCurrentFilters() }
        .onChange(of: selectedItemID) { _, _ in syncDraftsFromSelection() }
        .sheet(isPresented: $showingGallery) {
            ShotGallerySheet(
                shots: relatedShots,
                previews: relatedShotPreviews,
                initialIndex: galleryStartIndex
            )
        }
        .sheet(item: $resolutionPromptItem) { item in
            ResolutionNotePromptSheet(
                title: identityLine(for: item),
                note: $resolutionPromptNote,
                onCancel: { resolutionPromptItem = nil },
                onSave: {
                    let note = resolutionPromptNote.trimmingCharacters(in: .whitespacesAndNewlines)
                    guard note.isEmpty == false else { return }
                    resolutionPromptItem = nil
                    Task { await resolveItem(item, withNote: note) }
                }
            )
        }
        .sheet(isPresented: $showManageTradesSheet) {
            ManageSimpleOptionsSheet(
                title: "Manage Trades",
                options: rowTradeOptions.map(displayTrade),
                onRename: { oldValue, newValue in
                    renameCustomTrade(from: oldValue, to: newValue)
                },
                onDelete: { removeCustomTrade($0) },
                onReorder: { writeTradeOrder($0) }
            )
        }
        .sheet(isPresented: $showManageAssigneesSheet) {
            ManageSimpleOptionsSheet(
                title: "Manage Assignees",
                options: rowAssigneeOptions,
                onRename: { oldValue, newValue in
                    renameCustomAssignee(from: oldValue, to: newValue)
                },
                onDelete: { removeCustomAssignee($0) },
                onReorder: { writeAssigneeOrder($0) }
            )
        }
        .alert("Clear Issue Data?", isPresented: $showingClearDevDataAlert) {
            Button("Cancel", role: .cancel) {}
            Button("Clear", role: .destructive) {
                Task { await clearIssueDataForDevelopment() }
            }
        } message: {
            Text("This deletes issues, issue history, punch list items, and evidence, and clears shot issue flags. Use for development only.")
        }
    }

    private var filterBar: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                Spacer()

                if isLoading {
                    ProgressView()
                        .controlSize(.small)
                }

#if DEBUG
                Button("Clear Issues (Dev)") {
                    showingClearDevDataAlert = true
                }
                .foregroundStyle(.red)
                .disabled(isLoading || isClearingDevData)
#endif
            }
        }
        .padding(.bottom, 2)
    }

    private var filterOverlayContent: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("Filters")
                    .font(.headline)
                Spacer()
                Button("Clear All") {
                    selectedStatus = nil
                    selectedPriority = nil
                    selectedOrganizationFilter = nil
                    selectedPropertyFilter = nil
                    selectedAssigneeFilter = nil
                    selectedTradeFilter = nil
                    pastDueOnly = false
                    activeTopFilterPopover = nil
                }
                .buttonStyle(.plain)
                .foregroundStyle(.blue)
                .disabled(anyFiltersActive == false)
            }

            Divider()

            VStack(alignment: .leading, spacing: 6) {
                topFilterButton(title: "Status", value: selectedStatus?.label ?? "All", isActive: selectedStatus != nil, width: 250) {
                    activeTopFilterPopover = .status
                }
                .popover(isPresented: topFilterPopoverBinding(.status), arrowEdge: .trailing) {
                    VStack(alignment: .leading, spacing: 6) {
                        topFilterOptionRow(label: "All", isSelected: selectedStatus == nil) {
                            selectedStatus = nil
                            activeTopFilterPopover = nil
                        }
                        Divider()
                        ForEach(PunchListStatus.allCases) { status in
                            topFilterOptionRow(
                                label: status.label,
                                isSelected: selectedStatus == status,
                                markerColor: statusColor(status.rawValue)
                            ) {
                                selectedStatus = status
                                activeTopFilterPopover = nil
                            }
                        }
                    }
                    .padding(10)
                }

                topFilterButton(title: "Priority", value: selectedPriority?.label ?? "All", isActive: selectedPriority != nil, width: 250) {
                    activeTopFilterPopover = .priority
                }
                .popover(isPresented: topFilterPopoverBinding(.priority), arrowEdge: .trailing) {
                    VStack(alignment: .leading, spacing: 6) {
                        topFilterOptionRow(label: "All Priorities", isSelected: selectedPriority == nil) {
                            selectedPriority = nil
                            activeTopFilterPopover = nil
                        }
                        Divider()
                        ForEach(PunchListPriority.allCases) { priority in
                            topFilterOptionRow(
                                label: priority.label,
                                isSelected: selectedPriority == priority,
                                markerColor: priorityColor(priority.rawValue)
                            ) {
                                selectedPriority = priority
                                activeTopFilterPopover = nil
                            }
                        }
                    }
                    .padding(10)
                }

                topFilterButton(title: "Organization", value: selectedOrganizationFilter ?? "All", isActive: selectedOrganizationFilter != nil, width: 250) {
                    activeTopFilterPopover = .organization
                }
                .popover(isPresented: topFilterPopoverBinding(.organization), arrowEdge: .trailing) {
                    VStack(alignment: .leading, spacing: 6) {
                        topFilterOptionRow(label: "All Organizations", isSelected: selectedOrganizationFilter == nil) {
                            selectedOrganizationFilter = nil
                            activeTopFilterPopover = nil
                        }
                        if organizationOptions.isEmpty == false {
                            Divider()
                            ForEach(organizationOptions, id: \.self) { org in
                                topFilterOptionRow(label: org, isSelected: selectedOrganizationFilter == org) {
                                    selectedOrganizationFilter = org
                                    activeTopFilterPopover = nil
                                }
                            }
                        }
                    }
                    .padding(10)
                }

                topFilterButton(title: "Property", value: selectedPropertyFilter ?? "All", isActive: selectedPropertyFilter != nil, width: 250) {
                    activeTopFilterPopover = .property
                }
                .popover(isPresented: topFilterPopoverBinding(.property), arrowEdge: .trailing) {
                    VStack(alignment: .leading, spacing: 6) {
                        topFilterOptionRow(label: "All Properties", isSelected: selectedPropertyFilter == nil) {
                            selectedPropertyFilter = nil
                            activeTopFilterPopover = nil
                        }
                        if propertyOptions.isEmpty == false {
                            Divider()
                            ForEach(propertyOptions, id: \.self) { property in
                                topFilterOptionRow(label: property, isSelected: selectedPropertyFilter == property) {
                                    selectedPropertyFilter = property
                                    activeTopFilterPopover = nil
                                }
                            }
                        }
                    }
                    .padding(10)
                }

                topFilterButton(title: "Assignee", value: selectedAssigneeFilterDisplay, isActive: selectedAssigneeFilter != nil, width: 250) {
                    activeTopFilterPopover = .assignee
                }
                .popover(isPresented: topFilterPopoverBinding(.assignee), arrowEdge: .trailing) {
                    VStack(alignment: .leading, spacing: 6) {
                        topFilterOptionRow(label: "All Assignees", isSelected: selectedAssigneeFilter == nil) {
                            selectedAssigneeFilter = nil
                            activeTopFilterPopover = nil
                        }
                        topFilterOptionRow(label: "Unassigned", isSelected: selectedAssigneeFilter == Self.unassignedAssigneeFilterValue) {
                            selectedAssigneeFilter = Self.unassignedAssigneeFilterValue
                            activeTopFilterPopover = nil
                        }
                        if assigneeOptions.isEmpty == false {
                            Divider()
                            ForEach(assigneeOptions, id: \.self) { assignee in
                                topFilterOptionRow(label: assignee, isSelected: selectedAssigneeFilter == assignee) {
                                    selectedAssigneeFilter = assignee
                                    activeTopFilterPopover = nil
                                }
                            }
                        }
                    }
                    .padding(10)
                }

                topFilterButton(title: "Trade", value: selectedTradeFilter?.label ?? "All", isActive: selectedTradeFilter != nil, width: 250) {
                    activeTopFilterPopover = .trade
                }
                .popover(isPresented: topFilterPopoverBinding(.trade), arrowEdge: .trailing) {
                    VStack(alignment: .leading, spacing: 6) {
                        topFilterOptionRow(label: "All Trades", isSelected: selectedTradeFilter == nil) {
                            selectedTradeFilter = nil
                            activeTopFilterPopover = nil
                        }
                        Divider()
                        ForEach(PunchListTrade.allCases) { trade in
                            topFilterOptionRow(label: trade.label, isSelected: selectedTradeFilter == trade) {
                                selectedTradeFilter = trade
                                activeTopFilterPopover = nil
                            }
                        }
                    }
                    .padding(10)
                }

                Button {
                    pastDueOnly.toggle()
                } label: {
                    HStack(spacing: 6) {
                        Image(systemName: pastDueOnly ? "checkmark.square.fill" : "square")
                        Text("Past Due Only")
                    }
                    .font(.body)
                    .foregroundStyle(pastDueOnly ? .white : .primary)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .frame(width: 250, alignment: .leading)
                    .background(
                        (pastDueOnly ? Color.blue : Color.white.opacity(0.10)),
                        in: RoundedRectangle(cornerRadius: 7, style: .continuous)
                    )
                }
                .buttonStyle(.plain)
            }

            HStack {
                Spacer()
                Button("Done") {
                    showFiltersOverlay = false
                }
                .buttonStyle(.plain)
                .foregroundStyle(.blue)
            }
        }
        .padding(12)
        .frame(width: 300)
    }

    private var filtersButton: some View {
        Button {
            showFiltersOverlay = true
        } label: {
            HStack(spacing: 6) {
                Image(systemName: "line.3.horizontal.decrease.circle")
                Text("Filters")
            }
            .font(.body.weight(.semibold))
            .foregroundStyle(anyFiltersActive ? .white : .primary)
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(
                (anyFiltersActive ? Color.blue : Color.white.opacity(0.10)),
                in: RoundedRectangle(cornerRadius: 7, style: .continuous)
            )
        }
        .buttonStyle(.plain)
        .popover(
            isPresented: $showFiltersOverlay,
            attachmentAnchor: .point(.bottom),
            arrowEdge: .bottom
        ) {
            filterOverlayContent
        }
    }

    private var itemList: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                Text("Punch List")
                    .font(.title3.weight(.semibold))
                Picker("", selection: $selectedPunchListPage) {
                    ForEach(PunchListPage.allCases) { page in
                        Text(page.rawValue).tag(page)
                    }
                }
                .labelsHidden()
                .pickerStyle(.segmented)
                .frame(width: 220)
                filtersButton
                if groupedFiltersActive {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 6) {
                            if let selectedTradeFilter {
                                activeFilterChip("Trade: \(selectedTradeFilter.label)") {
                                    self.selectedTradeFilter = nil
                                }
                            }
                            if let selectedAssigneeFilter {
                                let assigneeLabel = selectedAssigneeFilter == Self.unassignedAssigneeFilterValue ? "Unassigned" : selectedAssigneeFilter
                                activeFilterChip("Assignee: \(assigneeLabel)") {
                                    self.selectedAssigneeFilter = nil
                                }
                            }
                            if let selectedPropertyFilter {
                                activeFilterChip("Property: \(selectedPropertyFilter)") {
                                    self.selectedPropertyFilter = nil
                                }
                            }
                            if let selectedOrganizationFilter {
                                activeFilterChip("Organization: \(selectedOrganizationFilter)") {
                                    self.selectedOrganizationFilter = nil
                                }
                            }
                        }
                    }
                }
                Spacer(minLength: 0)
            }

            if let loadError {
                Text(loadError)
                    .font(.caption)
                    .foregroundStyle(.red)
            }

            List(selection: $selectedItemID) {
                ForEach(items) { item in
                    HStack(alignment: .top, spacing: 10) {
                        VStack(spacing: 6) {
                            priorityPickerColumn(for: item)
                            rowThumbnailButton(for: item)
                        }
                        .frame(width: rowMediaControlSize, alignment: .topLeading)

                        VStack(alignment: .leading, spacing: 4) {
                            HStack(spacing: 6) {
                                Text(identityLine(for: item))
                                    .font(.headline)
                                    .lineLimit(1)
                                rowDetailsMenu(for: item)
                            }

                            HStack(spacing: 6) {
                                Image(systemName: "flag.fill")
                                    .font(.caption.weight(.bold))
                                    .foregroundStyle(.red)
                                Text(item.flaggedReason?.isEmpty == false ? item.flaggedReason! : "Flagged")
                                    .font(.subheadline.weight(.bold))
                                    .foregroundStyle(.white)
                                    .lineLimit(1)
                            }

                            HStack(spacing: 6) {
                                Text(item.propertyName ?? "Unknown Property")
                                    .font(.subheadline.weight(.semibold))
                                if let org = normalizedOrgName(item.orgName) {
                                    Text("|")
                                        .font(.subheadline.weight(.semibold))
                                        .foregroundStyle(.secondary)
                                    Text(org)
                                        .font(.subheadline.weight(.semibold))
                                }
                            }
                            .foregroundStyle(.primary)

                            if let captured = formattedDate(item.capturedAtUTC) {
                                Text(captured)
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }

                            if PunchListStatus(rawValue: item.status) == .resolved {
                                HStack(spacing: 6) {
                                    Text("Resolution:")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(.secondary)
                                    Text(item.resolutionNote?.isEmpty == false ? item.resolutionNote! : "No note")
                                        .font(.caption)
                                        .lineLimit(2)
                                    Button("Edit") {
                                        resolutionPromptNote = item.resolutionNote ?? ""
                                        resolutionPromptItem = item
                                    }
                                    .buttonStyle(.plain)
                                        .font(.caption.weight(.semibold))
                                }
                            }
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)

                        rightControls(for: item)
                    }
                    .padding(.trailing, 12)
                    .tag(item.id)
                }
            }
        }
        .frame(minWidth: 420)
    }

    private var detailPane: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 10) {
                if let selectedItem {
                    HStack {
                        Text("Item Details")
                            .font(.title3.weight(.semibold))
                        Spacer()
                    Menu {
                        if let property = selectedItem.propertyName {
                            Button("Copy Property: \(property)") {
                                copyToClipboard(property)
                            }
                        }
                        Button("Copy Photo: \(displayPhotoName(for: selectedItem))") {
                            copyToClipboard(displayPhotoName(for: selectedItem))
                        }
                        Button("Copy Session: \(selectedItem.sessionID)") {
                            copyToClipboard(selectedItem.sessionID)
                        }
                        Button("Copy Issue ID: \(selectedItem.issueID ?? "N/A")") {
                            copyToClipboard(selectedItem.issueID ?? "N/A")
                        }
                        Button("Copy Reason: \(selectedItem.flaggedReason?.isEmpty == false ? selectedItem.flaggedReason! : "Flagged")") {
                            copyToClipboard(selectedItem.flaggedReason?.isEmpty == false ? selectedItem.flaggedReason! : "Flagged")
                        }
                    } label: {
                        Image(systemName: "ellipsis.circle")
                            .font(.title3)
                        }
                        .menuStyle(.borderlessButton)
                        .help("More details")
                    }

                    Text("Related Shots (\(relatedShots.count))")
                        .font(.headline)

                    primaryPreviewCard

                    HStack(spacing: 10) {
                        Button("Review Past Shots (\(max(relatedShots.count - 1, 0)))") {
                            galleryStartIndex = 1
                            showingGallery = true
                        }
                        .disabled(relatedShots.count < 2)

                        Button("Open Full Gallery") {
                            galleryStartIndex = 0
                            showingGallery = true
                        }
                        .disabled(relatedShots.isEmpty)
                    }

                    Picker("Status", selection: $statusDraft) {
                        ForEach(PunchListStatus.allCases) { status in
                            Text(status.label).tag(status)
                        }
                    }
                    .pickerStyle(.menu)

                    TextField("Assigned to", text: $assignedToDraft)
                        .textFieldStyle(.roundedBorder)

                    TextField(
                        "Due date (MM/DD/YYYY)",
                        text: Binding(
                            get: { dueDateDraft },
                            set: { dueDateDraft = formattedDueDateInput($0) }
                        )
                    )
                        .textFieldStyle(.roundedBorder)

                    TextField("Resolution note", text: $resolutionNoteDraft, axis: .vertical)
                        .lineLimit(3...5)
                        .textFieldStyle(.roundedBorder)

                    HStack {
                        Button("Upload Resolution Photo") {
                            uploadResolutionPhoto(for: selectedItem)
                        }
                        .disabled(isSaving || isUploadingResolution)

                        Button("Save") {
                            Task { await saveSelectedItem() }
                        }
                        .disabled(isSaving || isUploadingResolution)

                        if isSaving || isUploadingResolution {
                            ProgressView()
                                .controlSize(.small)
                        }
                    }

                    if let uploadMessage {
                        Text(uploadMessage)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                } else {
                    Text("Select a punch list item")
                        .foregroundStyle(.secondary)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.trailing, 8)
        }
        .frame(minWidth: 360, maxHeight: .infinity, alignment: .topLeading)
        .padding(.leading, 10)
    }

    private var selectedItem: PunchListItemSummary? {
        guard let selectedItemID else { return nil }
        return items.first(where: { $0.id == selectedItemID })
    }

    private func reloadItems() async {
        isLoading = true
        loadError = nil
        do {
            let loaded = try PunchListService.shared.fetchItems(
                status: selectedStatus,
                propertyFilter: nil
            )
            allItems = loaded
            applyCurrentFilters()
            if let selectedItemID, items.contains(where: { $0.id == selectedItemID }) == false {
                self.selectedItemID = nil
            } else if self.selectedItemID == nil {
                self.selectedItemID = items.first?.id
            }
            syncDraftsFromSelection()
        } catch {
            loadError = error.localizedDescription
        }
        isLoading = false
    }

    private func syncDraftsFromSelection() {
        guard let selectedItem else {
            assignedToDraft = ""
            dueDateDraft = ""
            resolutionNoteDraft = ""
            statusDraft = .active
            priorityDraft = .medium
            tradeDraft = .general
            relatedShots = []
            relatedShotPreviews = [:]
            uploadMessage = nil
            return
        }

        assignedToDraft = selectedItem.assignedTo ?? ""
        dueDateDraft = selectedItem.dueDate ?? ""
        resolutionNoteDraft = selectedItem.resolutionNote ?? ""
        statusDraft = PunchListStatus(rawValue: selectedItem.status) ?? .active
        priorityDraft = PunchListPriority(rawValue: selectedItem.priority) ?? .medium
        tradeDraft = PunchListTrade(rawValue: selectedItem.trade) ?? .general
        uploadMessage = nil
        Task { await loadRelatedShots(for: selectedItem) }
    }

    private func saveSelectedItem() async {
        guard let selectedItem else { return }
        isSaving = true
        defer { isSaving = false }

        do {
            try PunchListService.shared.updateItem(
                id: selectedItem.id,
                status: statusDraft,
                priority: priorityDraft,
                trade: tradeDraft.rawValue,
                assignedTo: assignedToDraft,
                dueDate: dueDateDraft,
                resolutionNote: resolutionNoteDraft
            )
            await reloadItems()
            selectedItemID = selectedItem.id
            if let refreshedItem = items.first(where: { $0.id == selectedItem.id }) {
                await loadRelatedShots(for: refreshedItem)
            }
        } catch {
            loadError = "Save failed: \(error.localizedDescription)"
        }
    }

    private func loadRelatedShots(for item: PunchListItemSummary) async {
        do {
            let loaded = try PunchListService.shared.fetchRelatedShots(for: item)
            relatedShots = loaded

            var previews: [String: NSImage] = [:]
            for shot in loaded {
                guard let filename = shot.stampedJpegFilename,
                      let imageURL = PunchListService.shared.resolveArchivedImageURL(
                        filename: filename,
                        preferredSessionID: shot.sessionID,
                        preferredPropertyID: shot.propertyID,
                        preferredLogicalShotIdentity: shot.logicalShotIdentity,
                        preferredShotKey: shot.shotKey,
                        preferredCapturedAtUTC: shot.capturedAtUTC
                      ),
                      let image = NSImage(contentsOf: imageURL) else {
                    continue
                }
                previews[shot.id] = image
            }
            relatedShotPreviews = previews
            if let latest = loaded.first, let latestImage = previews[latest.id] {
                rowThumbnailImages[item.id] = latestImage
            }
        } catch {
            relatedShots = []
            relatedShotPreviews = [:]
            loadError = "Failed loading related shots: \(error.localizedDescription)"
        }
    }

    private func uploadResolutionPhoto(for item: PunchListItemSummary) {
        let panel = NSOpenPanel()
        panel.title = "Select Resolution Photo"
        panel.prompt = "Upload"
        panel.allowedContentTypes = [.jpeg, .png, .heic, .tiff, .image]
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.canChooseFiles = true

        guard panel.runModal() == .OK, let selectedURL = panel.url else { return }

        isUploadingResolution = true
        uploadMessage = nil

        Task {
            defer { isUploadingResolution = false }

            do {
                let destination = try PunchListService.shared.attachResolutionEvidence(
                    itemID: item.id,
                    sourceImageURL: selectedURL,
                    uploader: NSFullUserName()
                )
                uploadMessage = "Uploaded to \(destination.lastPathComponent) and marked resolved."
                await reloadItems()
                if let refreshedItem = items.first(where: { $0.id == item.id }) {
                    selectedItemID = refreshedItem.id
                    await loadRelatedShots(for: refreshedItem)
                    statusDraft = PunchListStatus(rawValue: refreshedItem.status) ?? .resolved
                }
            } catch {
                loadError = "Resolution upload failed: \(error.localizedDescription)"
            }
        }
    }

    private func clearIssueDataForDevelopment() async {
        isClearingDevData = true
        defer { isClearingDevData = false }

        do {
            try PunchListService.shared.clearIssueAndPunchListDataForDevelopment()
            await reloadItems()
            loadError = nil
            uploadMessage = "Development issue data cleared."
        } catch {
            loadError = "Failed clearing issue data: \(error.localizedDescription)"
        }
    }

    private func updatePriority(for item: PunchListItemSummary, to priority: PunchListPriority) async {
        guard updatingRowItemIDs.contains(item.id) == false else { return }
        updatingRowItemIDs.insert(item.id)
        defer { updatingRowItemIDs.remove(item.id) }

        let previousPriority = item.priority
        updatePriorityLocally(itemID: item.id, priority: priority.rawValue)

        do {
            try PunchListService.shared.updateItem(
                id: item.id,
                status: PunchListStatus(rawValue: item.status) ?? .active,
                priority: priority,
                trade: item.trade,
                assignedTo: item.assignedTo,
                dueDate: item.dueDate,
                resolutionNote: item.resolutionNote
            )
        } catch {
            updatePriorityLocally(itemID: item.id, priority: previousPriority)
            loadError = "Priority update failed: \(error.localizedDescription)"
        }
    }

    private func updatePriorityLocally(itemID: Int64, priority: String) {
        allItems = allItems.map { existing in
            guard existing.id == itemID else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: priority,
                trade: existing.trade,
                assignedTo: existing.assignedTo,
                dueDate: existing.dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
    }

    private func updateTrade(for item: PunchListItemSummary, to trade: String) async {
        guard updatingRowItemIDs.contains(item.id) == false else { return }
        updatingRowItemIDs.insert(item.id)
        defer { updatingRowItemIDs.remove(item.id) }

        let previous = item.trade
        updateTradeLocally(itemID: item.id, trade: trade)

        do {
            try PunchListService.shared.updateItem(
                id: item.id,
                status: PunchListStatus(rawValue: item.status) ?? .active,
                priority: PunchListPriority(rawValue: item.priority) ?? .medium,
                trade: trade,
                assignedTo: item.assignedTo,
                dueDate: item.dueDate,
                resolutionNote: item.resolutionNote
            )
        } catch {
            updateTradeLocally(itemID: item.id, trade: previous)
            loadError = "Trade update failed: \(error.localizedDescription)"
        }
    }

    private func updateTradeLocally(itemID: Int64, trade: String) {
        allItems = allItems.map { existing in
            guard existing.id == itemID else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: existing.priority,
                trade: trade,
                assignedTo: existing.assignedTo,
                dueDate: existing.dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
    }

    private func handleStatusSelection(for item: PunchListItemSummary, status: PunchListStatus) async {
        if status == .resolved {
            await MainActor.run {
                resolutionPromptNote = item.resolutionNote ?? ""
                resolutionPromptItem = item
            }
            return
        }
        await updateStatus(for: item, to: status, resolutionNote: nil)
    }

    private func resolveItem(_ item: PunchListItemSummary, withNote note: String) async {
        await updateStatus(for: item, to: .resolved, resolutionNote: note)
    }

    private func updateStatus(for item: PunchListItemSummary, to status: PunchListStatus, resolutionNote: String?) async {
        guard updatingRowItemIDs.contains(item.id) == false else { return }
        updatingRowItemIDs.insert(item.id)
        defer { updatingRowItemIDs.remove(item.id) }

        let previous = item.status
        let previousNote = item.resolutionNote
        let noteToPersist: String?
        if status == .resolved {
            noteToPersist = resolutionNote
        } else {
            noteToPersist = nil
        }
        updateStatusLocally(itemID: item.id, status: status.rawValue, resolutionNote: noteToPersist)

        do {
            try PunchListService.shared.updateItem(
                id: item.id,
                status: status,
                priority: PunchListPriority(rawValue: item.priority) ?? .medium,
                trade: item.trade,
                assignedTo: item.assignedTo,
                dueDate: item.dueDate,
                resolutionNote: noteToPersist
            )
        } catch {
            updateStatusLocally(itemID: item.id, status: previous, resolutionNote: previousNote)
            loadError = "Status update failed: \(error.localizedDescription)"
        }
    }

    private func updateStatusLocally(itemID: Int64, status: String, resolutionNote: String?) {
        allItems = allItems.map { existing in
            guard existing.id == itemID else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: status,
                priority: existing.priority,
                trade: existing.trade,
                assignedTo: existing.assignedTo,
                dueDate: existing.dueDate,
                resolutionNote: resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
    }

    private func updateAssignee(for item: PunchListItemSummary, to assignee: String?) async {
        guard updatingRowItemIDs.contains(item.id) == false else { return }
        updatingRowItemIDs.insert(item.id)
        defer { updatingRowItemIDs.remove(item.id) }

        let previous = item.assignedTo
        updateAssigneeLocally(itemID: item.id, assignedTo: assignee)

        do {
            try PunchListService.shared.updateItem(
                id: item.id,
                status: PunchListStatus(rawValue: item.status) ?? .active,
                priority: PunchListPriority(rawValue: item.priority) ?? .medium,
                trade: item.trade,
                assignedTo: assignee,
                dueDate: item.dueDate,
                resolutionNote: item.resolutionNote
            )
        } catch {
            updateAssigneeLocally(itemID: item.id, assignedTo: previous)
            loadError = "Assignee update failed: \(error.localizedDescription)"
        }
    }

    private func updateAssigneeLocally(itemID: Int64, assignedTo: String?) {
        allItems = allItems.map { existing in
            guard existing.id == itemID else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: existing.priority,
                trade: existing.trade,
                assignedTo: assignedTo,
                dueDate: existing.dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
    }

    private func updateDueDate(for item: PunchListItemSummary, to dueDate: String?) async {
        guard updatingRowItemIDs.contains(item.id) == false else { return }
        updatingRowItemIDs.insert(item.id)
        defer { updatingRowItemIDs.remove(item.id) }

        let previous = item.dueDate
        updateDueDateLocally(itemID: item.id, dueDate: dueDate)

        do {
            try PunchListService.shared.updateItem(
                id: item.id,
                status: PunchListStatus(rawValue: item.status) ?? .active,
                priority: PunchListPriority(rawValue: item.priority) ?? .medium,
                trade: item.trade,
                assignedTo: item.assignedTo,
                dueDate: dueDate,
                resolutionNote: item.resolutionNote
            )
        } catch {
            updateDueDateLocally(itemID: item.id, dueDate: previous)
            loadError = "Due date update failed: \(error.localizedDescription)"
        }
    }

    private func updateDueDateLocally(itemID: Int64, dueDate: String?) {
        allItems = allItems.map { existing in
            guard existing.id == itemID else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: existing.priority,
                trade: existing.trade,
                assignedTo: existing.assignedTo,
                dueDate: dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
    }

    private func identityLine(for item: PunchListItemSummary) -> String {
        [
            (item.building ?? "Building").uppercased(),
            (item.elevation ?? "Elevation").uppercased(),
            (item.detailType ?? "Detail").uppercased(),
            ("A\(item.angleIndex ?? 0)").uppercased(),
        ].joined(separator: " | ")
    }

    private func displayStatus(_ rawStatus: String) -> String {
        PunchListStatus(rawValue: rawStatus)?.label ?? rawStatus
    }

    private func normalizedAssignee(_ rawAssigned: String?) -> String? {
        let assigned = rawAssigned?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return assigned.isEmpty ? nil : assigned
    }

    private func normalizedOrgName(_ rawOrg: String?) -> String? {
        let org = rawOrg?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return org.isEmpty ? nil : org
    }

    private func displayPriority(_ rawPriority: String) -> String {
        PunchListPriority(rawValue: rawPriority)?.label ?? rawPriority.capitalized
    }

    private func displayTrade(_ rawTrade: String) -> String {
        PunchListTrade(rawValue: rawTrade)?.label ?? rawTrade
    }

    private var customTrades: [String] {
        decodedCustomList(from: customTradesStorage)
    }

    private var customAssignees: [String] {
        decodedCustomList(from: customAssigneesStorage)
    }

    private var preferredTradeOrder: [String] {
        decodedOrderList(from: tradeOrderStorage)
    }

    private var preferredAssigneeOrder: [String] {
        decodedOrderList(from: assigneeOrderStorage)
    }

    private var rowTradeOptions: [String] {
        var seen: Set<String> = []
        var options: [String] = []
        for trade in PunchListTrade.allCases.map(\.rawValue) {
            let key = trade.lowercased()
            if seen.insert(key).inserted {
                options.append(trade)
            }
        }
        for trade in customTrades {
            let key = trade.lowercased()
            if seen.insert(key).inserted {
                options.append(trade)
            }
        }
        for trade in allItems.compactMap({ value -> String? in
            let trimmed = value.trade.trimmingCharacters(in: .whitespacesAndNewlines)
            return trimmed.isEmpty ? nil : trimmed
        }) {
            let key = trade.lowercased()
            if seen.insert(key).inserted {
                options.append(trade)
            }
        }
        var sorted = options.sorted {
            displayTrade($0).localizedCaseInsensitiveCompare(displayTrade($1)) == .orderedAscending
        }
        if let index = sorted.firstIndex(where: { $0.caseInsensitiveCompare(PunchListTrade.general.rawValue) == .orderedSame }) {
            let general = sorted.remove(at: index)
            sorted.insert(general, at: 0)
        }
        return applyPreferredOrder(
            sorted,
            preferredOrder: preferredTradeOrder,
            label: { displayTrade($0) },
            keepGeneralTop: true
        )
    }

    private var rowAssigneeOptions: [String] {
        var seen: Set<String> = []
        var options: [String] = []
        for assignee in assigneeOptions {
            let key = assignee.lowercased()
            if seen.insert(key).inserted {
                options.append(assignee)
            }
        }
        for assignee in customAssignees {
            let key = assignee.lowercased()
            if seen.insert(key).inserted {
                options.append(assignee)
            }
        }
        let sorted = options.sorted {
            $0.localizedCaseInsensitiveCompare($1) == .orderedAscending
        }
        return applyPreferredOrder(
            sorted,
            preferredOrder: preferredAssigneeOrder,
            label: { $0 },
            keepGeneralTop: false
        )
    }

    private func decodedCustomList(from json: String) -> [String] {
        guard let data = json.data(using: .utf8),
              let decoded = try? JSONDecoder().decode([String].self, from: data) else {
            return []
        }
        return sanitizedCustomList(decoded)
    }

    private func decodedOrderList(from json: String) -> [String] {
        guard let data = json.data(using: .utf8),
              let decoded = try? JSONDecoder().decode([String].self, from: data) else {
            return []
        }
        return dedupedKeepingOrder(decoded)
    }

    private func writeCustomTrades(_ values: [String]) {
        let normalized = sanitizedCustomList(values)
        guard let data = try? JSONEncoder().encode(normalized),
              let encoded = String(data: data, encoding: .utf8) else {
            return
        }
        customTradesStorage = encoded
    }

    private func writeCustomAssignees(_ values: [String]) {
        let normalized = sanitizedCustomList(values)
        guard let data = try? JSONEncoder().encode(normalized),
              let encoded = String(data: data, encoding: .utf8) else {
            return
        }
        customAssigneesStorage = encoded
    }

    private func writeTradeOrder(_ values: [String]) {
        let normalized = dedupedKeepingOrder(values)
        guard let data = try? JSONEncoder().encode(normalized),
              let encoded = String(data: data, encoding: .utf8) else {
            return
        }
        tradeOrderStorage = encoded
    }

    private func writeAssigneeOrder(_ values: [String]) {
        let normalized = dedupedKeepingOrder(values)
        guard let data = try? JSONEncoder().encode(normalized),
              let encoded = String(data: data, encoding: .utf8) else {
            return
        }
        assigneeOrderStorage = encoded
    }

    private func applyPreferredOrder(
        _ values: [String],
        preferredOrder: [String],
        label: (String) -> String,
        keepGeneralTop: Bool
    ) -> [String] {
        guard preferredOrder.isEmpty == false else { return values }
        let rank: [String: Int] = Dictionary(
            uniqueKeysWithValues: preferredOrder.enumerated().map { ($0.element.lowercased(), $0.offset) }
        )
        let fallbackRank: [String: Int] = Dictionary(
            uniqueKeysWithValues: values.enumerated().map { (label($0.element).lowercased(), $0.offset + 10_000) }
        )
        var reordered = values.sorted { lhs, rhs in
            let lhsKey = label(lhs).lowercased()
            let rhsKey = label(rhs).lowercased()
            let lhsRank = rank[lhsKey] ?? fallbackRank[lhsKey] ?? Int.max
            let rhsRank = rank[rhsKey] ?? fallbackRank[rhsKey] ?? Int.max
            if lhsRank != rhsRank { return lhsRank < rhsRank }
            return label(lhs).localizedCaseInsensitiveCompare(label(rhs)) == .orderedAscending
        }
        if keepGeneralTop,
           let index = reordered.firstIndex(where: { $0.caseInsensitiveCompare(PunchListTrade.general.rawValue) == .orderedSame }) {
            let general = reordered.remove(at: index)
            reordered.insert(general, at: 0)
        }
        return reordered
    }

    private func sanitizedCustomList(_ values: [String]) -> [String] {
        var seen: Set<String> = []
        var output: [String] = []
        for value in values {
            let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmed.isEmpty == false else { continue }
            let key = trimmed.lowercased()
            if seen.insert(key).inserted {
                output.append(trimmed)
            }
        }
        return output.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    private func dedupedKeepingOrder(_ values: [String]) -> [String] {
        var seen: Set<String> = []
        var output: [String] = []
        for value in values {
            let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard trimmed.isEmpty == false else { continue }
            let key = trimmed.lowercased()
            if seen.insert(key).inserted {
                output.append(trimmed)
            }
        }
        return output
    }

    private func addCustomTrade(_ value: String) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty == false else { return }
        writeCustomTrades(customTrades + [trimmed])
    }

    private func addCustomAssignee(_ value: String) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty == false else { return }
        writeCustomAssignees(customAssignees + [trimmed])
    }

    private func removeCustomTrade(_ value: String) {
        writeCustomTrades(customTrades.filter { displayTrade($0).caseInsensitiveCompare(value) != .orderedSame })
    }

    private func removeCustomAssignee(_ value: String) {
        writeCustomAssignees(customAssignees.filter { $0.caseInsensitiveCompare(value) != .orderedSame })
        allItems = allItems.map { existing in
            guard normalizedAssignee(existing.assignedTo)?.caseInsensitiveCompare(value) == .orderedSame else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: existing.priority,
                trade: existing.trade,
                assignedTo: nil,
                dueDate: existing.dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
        Task { try? PunchListService.shared.bulkRenameAssignee(from: value, to: nil) }
    }

    private func renameCustomTrade(from oldValue: String, to newValue: String) {
        let trimmed = newValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty == false else { return }
        var updated = customTrades
        if let index = updated.firstIndex(where: { displayTrade($0).caseInsensitiveCompare(oldValue) == .orderedSame }) {
            updated[index] = trimmed
        } else {
            updated.append(trimmed)
        }
        writeCustomTrades(updated)
        allItems = allItems.map { existing in
            guard displayTrade(existing.trade).caseInsensitiveCompare(oldValue) == .orderedSame else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: existing.priority,
                trade: trimmed,
                assignedTo: existing.assignedTo,
                dueDate: existing.dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
        Task { try? PunchListService.shared.bulkRenameTrade(from: oldValue, to: trimmed) }
    }

    private func renameCustomAssignee(from oldValue: String, to newValue: String) {
        let trimmed = newValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty == false else { return }
        var updated = customAssignees
        if let index = updated.firstIndex(where: { $0.caseInsensitiveCompare(oldValue) == .orderedSame }) {
            updated[index] = trimmed
        } else {
            updated.append(trimmed)
        }
        writeCustomAssignees(updated)
        allItems = allItems.map { existing in
            guard normalizedAssignee(existing.assignedTo)?.caseInsensitiveCompare(oldValue) == .orderedSame else { return existing }
            return PunchListItemSummary(
                id: existing.id,
                sessionID: existing.sessionID,
                shotID: existing.shotID,
                issueID: existing.issueID,
                logicalShotIdentity: existing.logicalShotIdentity,
                propertyID: existing.propertyID,
                propertyName: existing.propertyName,
                orgName: existing.orgName,
                building: existing.building,
                elevation: existing.elevation,
                detailType: existing.detailType,
                angleIndex: existing.angleIndex,
                shotKey: existing.shotKey,
                capturedAtUTC: existing.capturedAtUTC,
                flaggedReason: existing.flaggedReason,
                stampedJpegFilename: existing.stampedJpegFilename,
                status: existing.status,
                priority: existing.priority,
                trade: existing.trade,
                assignedTo: trimmed,
                dueDate: existing.dueDate,
                resolutionNote: existing.resolutionNote,
                resolvedAtUTC: existing.resolvedAtUTC,
                relatedShotCount: existing.relatedShotCount
            )
        }
        applyCurrentFilters()
        Task { try? PunchListService.shared.bulkRenameAssignee(from: oldValue, to: trimmed) }
    }

    private func priorityColor(_ rawPriority: String) -> Color {
        switch PunchListPriority(rawValue: rawPriority) {
        case .low: return .secondary
        case .medium: return Color(red: 0.24, green: 0.28, blue: 0.34)
        case .high: return .orange
        case .critical: return .red
        case .none: return .secondary
        }
    }

    @ViewBuilder
    private func priorityPickerColumn(for item: PunchListItemSummary) -> some View {
        Button {
            priorityPopoverItemID = item.id
        } label: {
            HStack(spacing: 4) {
                Text(displayPriority(item.priority))
                    .font(.caption2.weight(.semibold))
                    .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.system(size: 9, weight: .semibold))
            }
            .foregroundStyle(.white)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: rowMediaControlSize, alignment: .center)
            .background(priorityColor(item.priority), in: RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .popover(
            isPresented: Binding(
                get: { priorityPopoverItemID == item.id },
                set: { isPresented in
                    if isPresented == false && priorityPopoverItemID == item.id {
                        priorityPopoverItemID = nil
                    }
                }
            ),
            arrowEdge: .bottom
        ) {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(PunchListPriority.allCases) { priority in
                    Button {
                        priorityPopoverItemID = nil
                        Task { await updatePriority(for: item, to: priority) }
                    } label: {
                        HStack(spacing: 8) {
                            Circle()
                                .fill(priorityColor(priority.rawValue))
                                .frame(width: 8, height: 8)
                            Text(priority.label)
                            Spacer()
                            if item.priority == priority.rawValue {
                                Image(systemName: "checkmark")
                                    .font(.caption.weight(.semibold))
                            }
                        }
                        .frame(width: 120, alignment: .leading)
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(10)
        }
        .disabled(updatingRowItemIDs.contains(item.id))
    }

    @ViewBuilder
    private func tradePickerColumn(for item: PunchListItemSummary) -> some View {
        Button {
            tradePopoverItemID = item.id
        } label: {
            HStack(spacing: 4) {
                Text(displayTrade(item.trade))
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.system(size: 9, weight: .semibold))
            }
            .foregroundStyle(.white)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: controlButtonWidth, alignment: .center)
            .background(neutralButtonGray, in: RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .popover(
            isPresented: Binding(
                get: { tradePopoverItemID == item.id },
                set: { isPresented in
                    if isPresented == false && tradePopoverItemID == item.id {
                        tradePopoverItemID = nil
                    }
                }
            ),
            arrowEdge: .bottom
        ) {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(rowTradeOptions, id: \.self) { trade in
                    Button {
                        tradePopoverItemID = nil
                        Task { await updateTrade(for: item, to: trade) }
                    } label: {
                        HStack(spacing: 8) {
                            Text(displayTrade(trade))
                            Spacer()
                            if item.trade.caseInsensitiveCompare(trade) == .orderedSame {
                                Image(systemName: "checkmark")
                                    .font(.caption.weight(.semibold))
                            }
                        }
                        .frame(width: 150, alignment: .leading)
                    }
                    .buttonStyle(.plain)
                }

                Divider()

                HStack(spacing: 6) {
                    TextField("Add new trade", text: $newTradeInput)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 150)

                    Button("Add") {
                        let value = newTradeInput.trimmingCharacters(in: .whitespacesAndNewlines)
                        guard value.isEmpty == false else { return }
                        addCustomTrade(value)
                        newTradeInput = ""
                        tradePopoverItemID = nil
                        Task { await updateTrade(for: item, to: value) }
                    }
                    .buttonStyle(.plain)
                }

                Button("Manage Trades...") {
                    tradePopoverItemID = nil
                    showManageTradesSheet = true
                }
                .buttonStyle(.plain)
            }
            .padding(10)
        }
        .disabled(updatingRowItemIDs.contains(item.id))
    }

    @ViewBuilder
    private func assigneePickerColumn(for item: PunchListItemSummary) -> some View {
        Button {
            assigneePopoverItemID = item.id
        } label: {
            HStack(spacing: 4) {
                Text(normalizedAssignee(item.assignedTo) ?? "Unassigned")
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.system(size: 9, weight: .semibold))
            }
            .foregroundStyle(.white)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: controlButtonWidth, alignment: .center)
            .background(neutralButtonGray, in: RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .popover(
            isPresented: Binding(
                get: { assigneePopoverItemID == item.id },
                set: { isPresented in
                    if isPresented == false && assigneePopoverItemID == item.id {
                        assigneePopoverItemID = nil
                    }
                }
            ),
            arrowEdge: .bottom
        ) {
            VStack(alignment: .leading, spacing: 6) {
                Button {
                    assigneePopoverItemID = nil
                    Task { await updateAssignee(for: item, to: nil) }
                } label: {
                    HStack {
                        Text("Unassigned")
                        Spacer()
                        if normalizedAssignee(item.assignedTo) == nil {
                            Image(systemName: "checkmark")
                                .font(.caption.weight(.semibold))
                        }
                    }
                    .frame(width: 160, alignment: .leading)
                }
                .buttonStyle(.plain)

                if rowAssigneeOptions.isEmpty == false {
                    Divider()
                    ForEach(rowAssigneeOptions, id: \.self) { assignee in
                        Button {
                            assigneePopoverItemID = nil
                            Task { await updateAssignee(for: item, to: assignee) }
                        } label: {
                            HStack {
                                Text(assignee)
                                Spacer()
                                if normalizedAssignee(item.assignedTo) == assignee {
                                    Image(systemName: "checkmark")
                                        .font(.caption.weight(.semibold))
                                }
                            }
                            .frame(width: 160, alignment: .leading)
                        }
                        .buttonStyle(.plain)
                    }
                }

                Divider()

                HStack(spacing: 6) {
                    TextField("Add new assignee", text: $newAssigneeInput)
                        .textFieldStyle(.roundedBorder)
                        .frame(width: 160)

                    Button("Add") {
                        let value = newAssigneeInput.trimmingCharacters(in: .whitespacesAndNewlines)
                        guard value.isEmpty == false else { return }
                        addCustomAssignee(value)
                        newAssigneeInput = ""
                        assigneePopoverItemID = nil
                        Task { await updateAssignee(for: item, to: value) }
                    }
                    .buttonStyle(.plain)
                }

                Button("Manage Assignees...") {
                    assigneePopoverItemID = nil
                    showManageAssigneesSheet = true
                }
                .buttonStyle(.plain)
            }
            .padding(10)
        }
        .disabled(updatingRowItemIDs.contains(item.id))
    }

    @ViewBuilder
    private func dueDatePickerColumn(for item: PunchListItemSummary) -> some View {
        Button {
            dueDatePopoverItemID = item.id
        } label: {
            HStack(spacing: 4) {
                Text(item.dueDate?.isEmpty == false ? item.dueDate! : "None")
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                Image(systemName: "calendar")
                    .font(.system(size: 11, weight: .semibold))
            }
            .foregroundStyle(isPastDue(item) ? .red : .white)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: controlButtonWidth, alignment: .center)
            .background(neutralButtonGray, in: RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .popover(
            isPresented: Binding(
                get: { dueDatePopoverItemID == item.id },
                set: { isPresented in
                    if isPresented == false && dueDatePopoverItemID == item.id {
                        dueDatePopoverItemID = nil
                    }
                }
            ),
            arrowEdge: .bottom
        ) {
            VStack(alignment: .center, spacing: 8) {
                DatePicker(
                    "",
                    selection: Binding(
                        get: { parsedDueDate(item.dueDate) ?? Date() },
                        set: { newValue in
                            Task { await updateDueDate(for: item, to: Self.dueDateFormatter.string(from: newValue)) }
                        }
                    ),
                    displayedComponents: [.date]
                )
                .labelsHidden()
                .datePickerStyle(.graphical)
                .frame(maxWidth: .infinity, alignment: .center)

                Divider()

                Button("Clear Due Date") {
                    dueDatePopoverItemID = nil
                    Task { await updateDueDate(for: item, to: nil) }
                }
                .frame(maxWidth: .infinity, alignment: .center)
            }
            .padding(10)
            .frame(width: 280)
        }
        .disabled(updatingRowItemIDs.contains(item.id))
    }

    @ViewBuilder
    private func rightControls(for item: PunchListItemSummary) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            labeledControlRow("Status") {
                statusPickerColumn(for: item)
            }
            labeledControlRow(isPastDue(item) ? "Past Due" : "Due Date") {
                dueDatePickerColumn(for: item)
            }
            labeledControlRow("Trade") {
                tradePickerColumn(for: item)
            }
            labeledControlRow("Assignee") {
                assigneePickerColumn(for: item)
            }
        }
    }

    @ViewBuilder
    private func labeledControlRow<Content: View>(
        _ label: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        HStack(spacing: 8) {
            Text(label)
                .font(.caption.weight(.semibold))
                .foregroundStyle(label == "Past Due" ? .red : .secondary)
                .frame(width: rowControlLabelWidth, alignment: .leading)
            content()
        }
    }

    @ViewBuilder
    private func statusPickerColumn(for item: PunchListItemSummary) -> some View {
        Button {
            statusPopoverItemID = item.id
        } label: {
            HStack(spacing: 4) {
                Text(displayStatus(item.status))
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.system(size: 9, weight: .semibold))
            }
            .foregroundStyle(.white)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .frame(width: controlButtonWidth, alignment: .center)
            .background(statusColor(item.status), in: RoundedRectangle(cornerRadius: 7, style: .continuous))
        }
        .buttonStyle(.plain)
        .popover(
            isPresented: Binding(
                get: { statusPopoverItemID == item.id },
                set: { isPresented in
                    if isPresented == false && statusPopoverItemID == item.id {
                        statusPopoverItemID = nil
                    }
                }
            ),
            arrowEdge: .bottom
        ) {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(PunchListStatus.allCases) { status in
                    Button {
                        statusPopoverItemID = nil
                        Task { await handleStatusSelection(for: item, status: status) }
                    } label: {
                        HStack(spacing: 8) {
                            Circle()
                                .fill(statusColor(status.rawValue))
                                .frame(width: 8, height: 8)
                            Text(status.label)
                            Spacer()
                            if item.status == status.rawValue {
                                Image(systemName: "checkmark")
                                    .font(.caption.weight(.semibold))
                            }
                        }
                        .frame(width: 210, alignment: .leading)
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(10)
        }
        .disabled(updatingRowItemIDs.contains(item.id))
    }

    private var selectedAssigneeFilterDisplay: String {
        if selectedAssigneeFilter == Self.unassignedAssigneeFilterValue {
            return "Unassigned"
        }
        return selectedAssigneeFilter ?? "All"
    }

    private func topFilterPopoverBinding(_ filter: TopFilterPopover) -> Binding<Bool> {
        Binding(
            get: { activeTopFilterPopover == filter },
            set: { isPresented in
                if isPresented {
                    activeTopFilterPopover = filter
                } else if activeTopFilterPopover == filter {
                    activeTopFilterPopover = nil
                }
            }
        )
    }

    @ViewBuilder
    private func topFilterButton(
        title: String,
        value: String,
        isActive: Bool,
        width: CGFloat,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            HStack(spacing: 6) {
                Text(title)
                    .font(.body)
                Text(value)
                    .font(.body.weight(.semibold))
                    .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.system(size: 10, weight: .semibold))
            }
            .foregroundStyle(isActive ? .white : .primary)
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .frame(width: width, alignment: .leading)
            .background(
                (isActive ? Color.blue : Color.white.opacity(0.10)),
                in: RoundedRectangle(cornerRadius: 7, style: .continuous)
            )
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private func topFilterOptionRow(
        label: String,
        isSelected: Bool,
        markerColor: Color? = nil,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            HStack(spacing: 8) {
                if let markerColor {
                    Circle()
                        .fill(markerColor)
                        .frame(width: 8, height: 8)
                }
                Text(label)
                    .lineLimit(1)
                Spacer()
                if isSelected {
                    Image(systemName: "checkmark")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.blue)
                }
            }
            .frame(width: 220, alignment: .leading)
        }
        .buttonStyle(.plain)
    }

    private var assigneeOptions: [String] {
        let values = allItems.compactMap { normalizedAssignee($0.assignedTo) }
        return Array(Set(values)).sorted()
    }

    private var propertyOptions: [String] {
        let values = allItems.compactMap { value -> String? in
            let raw = value.propertyName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            return raw.isEmpty ? nil : raw
        }
        return Array(Set(values)).sorted()
    }

    private var organizationOptions: [String] {
        let values = allItems.compactMap { normalizedOrgName($0.orgName) }
        return Array(Set(values)).sorted()
    }

    private var groupedFiltersActive: Bool {
        selectedOrganizationFilter != nil
            || selectedPropertyFilter != nil
            || selectedAssigneeFilter != nil
            || selectedTradeFilter != nil
    }

    private var anyFiltersActive: Bool {
        groupedFiltersActive
            || selectedStatus != nil
            || selectedPriority != nil
            || pastDueOnly
    }

    @ViewBuilder
    private func activeFilterChip(_ title: String, onClear: @escaping () -> Void) -> some View {
        HStack(spacing: 6) {
            Text(title)
                .font(.caption.weight(.semibold))
                .lineLimit(1)
            Button {
                onClear()
            } label: {
                Image(systemName: "xmark.circle.fill")
                    .font(.caption.weight(.semibold))
            }
            .buttonStyle(.plain)
        }
        .foregroundStyle(.white)
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .background(Color.blue.opacity(0.8), in: Capsule())
    }

    private func statusColor(_ rawStatus: String) -> Color {
        switch PunchListStatus(rawValue: rawStatus) {
        case .active: return .red
        case .resolvedPendingVerification: return .orange
        case .resolved: return .green
        case .none: return .secondary
        }
    }

    private func applyLocalFilters(_ source: [PunchListItemSummary]) -> [PunchListItemSummary] {
        return source.filter { item in
            let isResolved = PunchListStatus(rawValue: item.status) == .resolved
            if selectedPunchListPage == .resolved {
                if isResolved == false {
                    return false
                }
            } else if isResolved {
                return false
            }
            if let selectedPriority, item.priority != selectedPriority.rawValue {
                return false
            }
            if let selectedTradeFilter, item.trade != selectedTradeFilter.rawValue {
                return false
            }
            if let selectedPropertyFilter {
                let propertyName = item.propertyName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                if propertyName != selectedPropertyFilter {
                    return false
                }
            }
            if let selectedOrganizationFilter {
                if normalizedOrgName(item.orgName) != selectedOrganizationFilter {
                    return false
                }
            }
            if let selectedAssigneeFilter {
                if selectedAssigneeFilter == Self.unassignedAssigneeFilterValue {
                    if normalizedAssignee(item.assignedTo) != nil {
                        return false
                    }
                } else if normalizedAssignee(item.assignedTo) != selectedAssigneeFilter {
                    return false
                }
            }
            if pastDueOnly && isPastDue(item) == false {
                return false
            }
            return true
        }
    }

    private func applyCurrentFilters() {
        items = applyLocalFilters(allItems)
        if let selectedItemID, items.contains(where: { $0.id == selectedItemID }) == false {
            self.selectedItemID = items.first?.id
        } else if self.selectedItemID == nil {
            self.selectedItemID = items.first?.id
        }
    }

    private func isPastDue(_ item: PunchListItemSummary) -> Bool {
        guard let dueDate = item.dueDate?.trimmingCharacters(in: .whitespacesAndNewlines),
              dueDate.isEmpty == false else {
            return false
        }
        guard let parsed = Self.dueDateFormatter.date(from: dueDate) else {
            return false
        }
        let today = Calendar.current.startOfDay(for: Date())
        let dueDay = Calendar.current.startOfDay(for: parsed)
        return dueDay < today && PunchListStatus(rawValue: item.status) != .resolved
    }

    private func formattedDate(_ rawUTC: String?) -> String? {
        guard let rawUTC else { return nil }
        guard let date = Self.iso8601Formatter.date(from: rawUTC) ?? Self.iso8601Fallback.date(from: rawUTC) else {
            return rawUTC
        }
        return Self.displayFormatter.string(from: date)
    }

    private func displayPhotoName(for item: PunchListItemSummary) -> String {
        if let stamped = item.stampedJpegFilename?.trimmingCharacters(in: .whitespacesAndNewlines),
           stamped.isEmpty == false {
            return stamped
        }
        if let shotKey = item.shotKey?.trimmingCharacters(in: .whitespacesAndNewlines),
           shotKey.isEmpty == false {
            return shotKey
        }
        return item.shotID ?? "Unknown"
    }

    @ViewBuilder
    private func rowThumbnailButton(for item: PunchListItemSummary) -> some View {
        Button {
            openGallery(for: item)
        } label: {
            if let image = thumbnailImage(for: item) {
                Image(nsImage: image)
                    .resizable()
                    .scaledToFill()
                    .frame(width: rowMediaControlSize, height: rowMediaControlSize)
                    .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
            } else {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .fill(Color.secondary.opacity(0.12))
                    .overlay {
                        Image(systemName: "photo")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .frame(width: rowMediaControlSize, height: rowMediaControlSize)
            }
        }
        .buttonStyle(.plain)
        .task(id: item.id) {
            await preloadThumbnailIfNeeded(for: item)
        }
    }

    private func thumbnailImage(for item: PunchListItemSummary) -> NSImage? {
        return rowThumbnailImages[item.id]
    }

    private static func loadRowThumbnail(for item: PunchListItemSummary) -> NSImage? {
        guard let filename = item.stampedJpegFilename?.trimmingCharacters(in: .whitespacesAndNewlines),
              filename.isEmpty == false,
              let imageURL = PunchListService.shared.resolveArchivedImageURL(
                filename: filename,
                preferredSessionID: item.sessionID,
                preferredPropertyID: item.propertyID,
                preferredLogicalShotIdentity: item.logicalShotIdentity,
                preferredShotKey: item.shotKey,
                preferredCapturedAtUTC: item.capturedAtUTC
              ),
              let image = NSImage(contentsOf: imageURL) else {
            return nil
        }
        return image
    }

    private func preloadThumbnailIfNeeded(for item: PunchListItemSummary) async {
        if rowThumbnailImages[item.id] != nil || loadingThumbnailItemIDs.contains(item.id) {
            return
        }
        loadingThumbnailItemIDs.insert(item.id)
        defer { loadingThumbnailItemIDs.remove(item.id) }

        let image = await Task.detached(priority: .utility) {
            Self.loadRowThumbnail(for: item)
        }.value

        if let image {
            rowThumbnailImages[item.id] = image
        }
    }

    private func openGallery(for item: PunchListItemSummary) {
        Task {
            selectedItemID = item.id
            await loadRelatedShots(for: item)
            galleryStartIndex = 0
            showingGallery = true
        }
    }

    private func formattedDueDateInput(_ input: String) -> String {
        let digits = input.filter(\.isNumber)
        let limited = String(digits.prefix(8))

        if limited.count <= 2 {
            return limited
        } else if limited.count <= 4 {
            let month = limited.prefix(2)
            let day = limited.dropFirst(2)
            return "\(month)/\(day)"
        } else {
            let month = limited.prefix(2)
            let day = limited.dropFirst(2).prefix(2)
            let year = limited.dropFirst(4)
            return "\(month)/\(day)/\(year)"
        }
    }

    private func parsedDueDate(_ value: String?) -> Date? {
        guard let value = value?.trimmingCharacters(in: .whitespacesAndNewlines),
              value.isEmpty == false else {
            return nil
        }
        return Self.dueDateFormatter.date(from: value)
    }

    private func copyToClipboard(_ text: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(text, forType: .string)
    }

    @ViewBuilder
    private func rowDetailsMenu(for item: PunchListItemSummary) -> some View {
        Menu {
            if let property = item.propertyName {
                Button("Copy Property: \(property)") {
                    copyToClipboard(property)
                }
            }
            Button("Copy Photo: \(displayPhotoName(for: item))") {
                copyToClipboard(displayPhotoName(for: item))
            }
            Button("Copy Session: \(item.sessionID)") {
                copyToClipboard(item.sessionID)
            }
            Button("Copy Issue ID: \(item.issueID ?? "N/A")") {
                copyToClipboard(item.issueID ?? "N/A")
            }
            Button("Copy Reason: \(item.flaggedReason?.isEmpty == false ? item.flaggedReason! : "Flagged")") {
                copyToClipboard(item.flaggedReason?.isEmpty == false ? item.flaggedReason! : "Flagged")
            }
            Divider()
            Button("Open Full Gallery") {
                openGallery(for: item)
            }
        } label: {
            Image(systemName: "ellipsis.circle")
                .font(.subheadline.weight(.semibold))
        }
        .menuStyle(.borderlessButton)
        .help("Item details")
    }

    private var primaryPreviewCard: some View {
        Group {
            if let latestShot = relatedShots.first, let image = relatedShotPreviews[latestShot.id] {
                Button {
                    galleryStartIndex = 0
                    showingGallery = true
                } label: {
                    ZStack {
                        RoundedRectangle(cornerRadius: 10, style: .continuous)
                            .fill(Color.secondary.opacity(0.08))
                        Image(nsImage: image)
                            .resizable()
                            .scaledToFit()
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .padding(6)
                    }
                    .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                }
                .buttonStyle(.plain)
                .frame(height: 260)

                HStack(spacing: 10) {
                    Text("Most Recent:")
                        .font(.caption.weight(.semibold))
                    Text(formattedDate(latestShot.capturedAtUTC) ?? "Unknown")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            } else {
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(Color.secondary.opacity(0.12))
                    .overlay {
                        Text("No recent stamped preview found")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .frame(height: 260)
            }
        }
    }

    private static let iso8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    private static let iso8601Fallback: ISO8601DateFormatter = {
        ISO8601DateFormatter()
    }()

    private static let displayFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York") ?? .current
        formatter.dateFormat = "MM/dd/yyyy h:mm a"
        return formatter
    }()

    private static let dueDateFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York") ?? .current
        formatter.dateFormat = "MM/dd/yyyy"
        return formatter
    }()
}

private struct RowPreviewContext: Identifiable {
    let id: Int64
    let title: String
    let image: NSImage
    let item: PunchListItemSummary
}

private struct ShotGallerySheet: View {
    let shots: [PunchListRelatedShot]
    let previews: [String: NSImage]
    let initialIndex: Int

    @Environment(\.dismiss) private var dismiss
    @State private var selection: Int = 0

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Shot Gallery")
                    .font(.title3.weight(.semibold))
                Spacer()
                Button("Done") { dismiss() }
            }

            if shots.isEmpty {
                Spacer()
                Text("No shots available")
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                let selectedShot = shots[min(max(selection, 0), shots.count - 1)]
                VStack(alignment: .leading, spacing: 10) {
                    if let image = previews[selectedShot.id] {
                        Image(nsImage: image)
                            .resizable()
                            .scaledToFit()
                            .frame(maxHeight: 460)
                            .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                    } else {
                        RoundedRectangle(cornerRadius: 10, style: .continuous)
                            .fill(Color.secondary.opacity(0.12))
                            .overlay {
                                Text("No preview available")
                                    .foregroundStyle(.secondary)
                            }
                            .frame(height: 320)
                    }

                    HStack(spacing: 10) {
                        Text(selectedShot.sessionID)
                        Text(selectedShot.capturedAtUTC ?? "Unknown capture")
                    }
                    .font(.caption)
                    .foregroundStyle(.secondary)

                    ScrollView(.horizontal) {
                        HStack(spacing: 10) {
                            ForEach(Array(shots.enumerated()), id: \.offset) { index, shot in
                                Button {
                                    selection = index
                                } label: {
                                    ZStack {
                                        if let thumb = previews[shot.id] {
                                            Image(nsImage: thumb)
                                                .resizable()
                                                .scaledToFill()
                                                .frame(width: 130, height: 84)
                                                .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                                        } else {
                                            RoundedRectangle(cornerRadius: 8, style: .continuous)
                                                .fill(Color.secondary.opacity(0.12))
                                                .overlay {
                                                    Text("No Preview")
                                                        .font(.caption2)
                                                        .foregroundStyle(.secondary)
                                                }
                                                .frame(width: 130, height: 84)
                                        }
                                    }
                                    .overlay(
                                        RoundedRectangle(cornerRadius: 8, style: .continuous)
                                            .stroke(selection == index ? Color.accentColor : Color.clear, lineWidth: 2)
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.vertical, 2)
                    }
                }
            }
        }
        .padding(14)
        .frame(minWidth: 760, minHeight: 620)
        .onAppear {
            let bounded = min(max(initialIndex, 0), max(shots.count - 1, 0))
            selection = bounded
        }
    }
}

private struct RowThumbnailPreviewSheet: View {
    let image: NSImage
    let title: String
    let openGallery: () -> Void

    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(title)
                    .font(.headline.weight(.semibold))
                    .lineLimit(1)
                Spacer()
                Button("Done") { dismiss() }
            }

            Image(nsImage: image)
                .resizable()
                .scaledToFit()
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

            HStack {
                Spacer()
                Button("Open Full Gallery") {
                    dismiss()
                    openGallery()
                }
            }
        }
        .padding(14)
        .frame(minWidth: 700, minHeight: 520)
    }
}

private struct ResolutionNotePromptSheet: View {
    let title: String
    @Binding var note: String
    let onCancel: () -> Void
    let onSave: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Resolution Note")
                .font(.title3.weight(.semibold))
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)

            TextField("Enter resolution note", text: $note, axis: .vertical)
                .lineLimit(3...5)
                .textFieldStyle(.roundedBorder)

            HStack {
                Spacer()
                Button("Cancel") { onCancel() }
                Button("Save") { onSave() }
                    .disabled(note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(14)
        .frame(minWidth: 460)
    }
}

private struct ManageSimpleOptionsSheet: View {
    private enum SortMode: String, CaseIterable, Identifiable {
        case nameAscending = "Name (A-Z)"
        case nameDescending = "Name (Z-A)"
        case manual = "Manual"

        var id: String { rawValue }
    }

    let title: String
    let options: [String]
    let onRename: (String, String) -> Void
    let onDelete: (String) -> Void
    let onReorder: ([String]) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var editingOption: String?
    @State private var editingDraft = ""
    @State private var sortMode: SortMode = .nameAscending
    @State private var manualOrder: [String] = []

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(title)
                    .font(.title3.weight(.semibold))
                Spacer()
                Menu {
                    ForEach(SortMode.allCases) { mode in
                        Button {
                            sortMode = mode
                        } label: {
                            HStack {
                                Text(mode.rawValue)
                                if sortMode == mode {
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 6) {
                        Text("Sort By")
                        Text(sortMode.rawValue)
                            .fontWeight(.semibold)
                    }
                    .font(.subheadline)
                }
                Button("Done") { dismiss() }
            }

            if displayedOptions.isEmpty {
                Text("No custom items yet.")
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                List(displayedOptions, id: \.self) { option in
                    HStack {
                        if editingOption == option {
                            TextField("Name", text: $editingDraft)
                                .textFieldStyle(.roundedBorder)
                                .frame(minWidth: 180)
                        } else {
                            Text(option)
                                .lineLimit(1)
                        }
                        Spacer()
                        if editingOption == option {
                            Button("Cancel") {
                                editingOption = nil
                                editingDraft = ""
                            }
                            .buttonStyle(.plain)

                            Button("Save") {
                                let trimmed = editingDraft.trimmingCharacters(in: .whitespacesAndNewlines)
                                guard trimmed.isEmpty == false else { return }
                                onRename(option, trimmed)
                                editingOption = nil
                                editingDraft = ""
                            }
                            .buttonStyle(.plain)
                            .disabled(editingDraft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        } else {
                            if sortMode == .manual {
                                HStack(spacing: 6) {
                                    Button {
                                        move(option: option, delta: -1)
                                    } label: {
                                        Image(systemName: "chevron.up")
                                    }
                                    .buttonStyle(.plain)
                                    .disabled(isFirstInManualOrder(option))

                                    Button {
                                        move(option: option, delta: 1)
                                    } label: {
                                        Image(systemName: "chevron.down")
                                    }
                                    .buttonStyle(.plain)
                                    .disabled(isLastInManualOrder(option))
                                }
                            }

                            Button("Rename") {
                                editingOption = option
                                editingDraft = option
                            }
                            .buttonStyle(.plain)

                            Button(role: .destructive) {
                                onDelete(option)
                            } label: {
                                Text("Delete")
                            }
                        }
                    }
                }
                .frame(minHeight: 220)
            }
        }
        .padding(14)
        .frame(minWidth: 360, minHeight: 280)
        .onAppear {
            manualOrder = options
        }
        .onChange(of: options) { _, newValue in
            var merged = manualOrder.filter { current in
                newValue.contains(where: { $0.caseInsensitiveCompare(current) == .orderedSame })
            }
            for value in newValue where merged.contains(where: { $0.caseInsensitiveCompare(value) == .orderedSame }) == false {
                merged.append(value)
            }
            manualOrder = merged
        }
    }

    private var displayedOptions: [String] {
        switch sortMode {
        case .nameAscending:
            return options.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
        case .nameDescending:
            return options.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedDescending }
        case .manual:
            var merged = manualOrder.filter { current in
                options.contains(where: { $0.caseInsensitiveCompare(current) == .orderedSame })
            }
            for value in options where merged.contains(where: { $0.caseInsensitiveCompare(value) == .orderedSame }) == false {
                merged.append(value)
            }
            return merged
        }
    }

    private func isFirstInManualOrder(_ option: String) -> Bool {
        guard let index = manualOrder.firstIndex(where: { $0.caseInsensitiveCompare(option) == .orderedSame }) else {
            return true
        }
        return index == 0
    }

    private func isLastInManualOrder(_ option: String) -> Bool {
        guard let index = manualOrder.firstIndex(where: { $0.caseInsensitiveCompare(option) == .orderedSame }) else {
            return true
        }
        return index >= manualOrder.count - 1
    }

    private func move(option: String, delta: Int) {
        guard let index = manualOrder.firstIndex(where: { $0.caseInsensitiveCompare(option) == .orderedSame }) else { return }
        let destination = index + delta
        guard destination >= 0, destination < manualOrder.count else { return }
        let value = manualOrder.remove(at: index)
        manualOrder.insert(value, at: destination)
        onReorder(manualOrder)
    }
}
