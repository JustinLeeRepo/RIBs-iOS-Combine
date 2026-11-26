//
//  Copyright (c) 2017. Uber Technologies
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

import Combine
import Foundation

/// A thread-safe group of cancellables that are cancelled together.
///
/// Cancellables inserted after the group has been cancelled are cancelled immediately.
final class CompositeCancellable: Cancellable {

    /// The number of cancellables currently held by this group.
    var count: Int {
        lock.lock()
        defer {
            lock.unlock()
        }
        return cancellables.count
    }

    /// Adds the given cancellable to this group, retaining it until the group is cancelled.
    ///
    /// - parameter cancellable: The cancellable to add.
    func insert(_ cancellable: Cancellable) {
        lock.lock()
        guard !isCancelled else {
            lock.unlock()
            cancellable.cancel()
            return
        }
        cancellables.append(cancellable)
        lock.unlock()
    }

    /// Cancels and releases all the cancellables held by this group.
    func cancel() {
        lock.lock()
        let cancellables = self.cancellables
        self.cancellables = []
        isCancelled = true
        lock.unlock()

        for cancellable in cancellables {
            cancellable.cancel()
        }
    }

    // MARK: - Private

    private let lock = NSLock()
    private var cancellables = [Cancellable]()
    private var isCancelled = false
}
