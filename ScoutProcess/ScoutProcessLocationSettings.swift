//
//  ScoutProcessLocationSettings.swift
//  ScoutProcess
//

import Foundation

enum ScoutProcessLocationSettings {
    private static let archiveRootKey = "ScoutProcessArchiveRootPath"
    private static let deliverablesRootKey = "ScoutProcessDeliverablesRootPath"

    static func archiveRootURL(fileManager: FileManager = .default) -> URL {
        if let customPath = UserDefaults.standard.string(forKey: archiveRootKey),
           customPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return URL(fileURLWithPath: customPath, isDirectory: true)
        }

        let docs = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first!
        return docs.appending(path: "ScoutArchive", directoryHint: .isDirectory)
    }

    static func deliverablesRootURL(fileManager: FileManager = .default) -> URL {
        if let customPath = UserDefaults.standard.string(forKey: deliverablesRootKey),
           customPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false {
            return URL(fileURLWithPath: customPath, isDirectory: true)
        }

        let googleDriveMyDrive = fileManager.homeDirectoryForCurrentUser
            .appending(path: "Library", directoryHint: .isDirectory)
            .appending(path: "CloudStorage", directoryHint: .isDirectory)
            .appending(path: "GoogleDrive-brian@scoutclear.com", directoryHint: .isDirectory)
            .appending(path: "My Drive", directoryHint: .isDirectory)
        if fileManager.fileExists(atPath: googleDriveMyDrive.path) {
            return googleDriveMyDrive.appending(path: "Scout Deliverables", directoryHint: .isDirectory)
        }

        let docs = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first!
        return docs.appending(path: "Scout Deliverables", directoryHint: .isDirectory)
    }

    static func setArchiveRootURL(_ url: URL) {
        UserDefaults.standard.set(url.path(percentEncoded: false), forKey: archiveRootKey)
    }

    static func setDeliverablesRootURL(_ url: URL) {
        UserDefaults.standard.set(url.path(percentEncoded: false), forKey: deliverablesRootKey)
    }
}
