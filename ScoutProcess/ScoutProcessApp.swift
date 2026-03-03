//
//  ScoutProcessApp.swift
//  ScoutProcess
//
//  Created by Brian Bennett on 2/28/26.
//

import SwiftUI

@main
struct ScoutProcessApp: App {
    @State private var model = ScoutProcessModel()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environment(model)
                .task {
                    model.start()
                }
                .onDisappear {
                    model.stop()
                }
        }
    }
}
