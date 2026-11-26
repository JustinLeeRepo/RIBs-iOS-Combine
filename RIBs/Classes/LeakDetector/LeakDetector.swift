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
import UIKit

/// Leak detection status.
public enum LeakDetectionStatus {

    /// Leak detection is in progress.
    case InProgress

    /// Leak detection has completed.
    case DidComplete
}

/// The default time values used for leak detection expectations.
public struct LeakDefaultExpectationTime {

    /// The object deallocation time.
    public static let deallocation = 1.0

    /// The view disappear time.
    public static let viewDisappear = 5.0
}

/// The handle for a scheduled leak detection.
public protocol LeakDetectionHandle {

    /// Cancel the scheduled detection.
    func cancel()
}

/// An expectation based leak detector, that allows an object's owner to set an expectation that an owned object to be
/// deallocated within a time frame.
///
/// A `Router` that owns an `Interactor` might for example expect its `Interactor` be deallocated when the `Router`
/// itself is deallocated. If the interactor does not deallocate in time, a runtime assert is triggered, along with
/// critical logging.
public class LeakDetector {

    /// The singleton instance.
    public static let instance = LeakDetector()

    /// The status of leak detection.
    ///
    /// The status changes between InProgress and DidComplete as units register for new detections, cancel existing
    /// detections, and existing detections complete.
    public var status: AnyPublisher<LeakDetectionStatus, Never> {
        return expectationCount
            .map { expectationCount in
                expectationCount > 0 ? LeakDetectionStatus.InProgress : LeakDetectionStatus.DidComplete
            }
            .removeDuplicates()
            .eraseToAnyPublisher()
    }

    /// Sets up an expectation for the given object to be deallocated within the given time.
    ///
    /// - parameter object: The object to track for deallocation.
    /// - parameter inTime: The time the given object is expected to be deallocated within.
    /// - returns: The handle that can be used to cancel the expectation.
    @discardableResult
    public func expectDeallocate(object: AnyObject, inTime time: TimeInterval = LeakDefaultExpectationTime.deallocation) -> LeakDetectionHandle {
        expectationCount.send(expectationCount.value + 1)

        let objectDescription = String(describing: object)
        let objectId = String(ObjectIdentifier(object).hashValue)
        // Each expectation tracks its object under a key of its own. Keyed by the object's address alone, an object
        // allocated at a freed object's address would take over its entry and be reported as the freed one leaking.
        let key = "\(objectId)-\(UUID().uuidString)" as NSString
        trackingObjects.setObject(object, forKey: key)

        let handle = LeakDetectionHandleImpl {
            self.expectationCount.send(self.expectationCount.value - 1)
        }

        Executor.execute(withDelay: time) {
            // The expectation is over either way, so its object is no longer expected to be deallocated.
            defer {
                self.trackingObjects.removeObject(forKey: key)
            }

            // Retain the handle so we can check for the cancelled status. Also cannot use the cancellable
            // concurrency API since the returned handle must be retained to ensure closure is executed.
            if !handle.cancelled {
                let didDeallocate = (self.trackingObjects.object(forKey: key) == nil)
                let message = "<\(objectDescription): \(objectId)> has leaked. Objects are expected to be deallocated at this time: \(self.trackingObjects)"

                if let leakReporter = self.leakReporter {
                    if !didDeallocate {
                        leakReporter(message)
                    }
                } else if self.disableLeakDetector {
                    if !didDeallocate {
                        print("Leak detection is disabled. This should only be used for debugging purposes.")
                        print(message)
                    }
                } else {
                    assert(didDeallocate, message)
                }
            }

            // A cancelled expectation was already taken off the count when it was cancelled.
            if handle.complete() {
                self.expectationCount.send(self.expectationCount.value - 1)
            }
        }

        return handle
    }

    /// Sets up an expectation for the given view controller to disappear within the given time.
    ///
    /// - parameter viewController: The `UIViewController` expected to disappear.
    /// - parameter inTime: The time the given view controller is expected to disappear.
    /// - returns: The handle that can be used to cancel the expectation.
    @discardableResult
    public func expectViewControllerDisappear(viewController: UIViewController, inTime time: TimeInterval = LeakDefaultExpectationTime.viewDisappear) -> LeakDetectionHandle {
        expectationCount.send(expectationCount.value + 1)

        let handle = LeakDetectionHandleImpl {
            self.expectationCount.send(self.expectationCount.value - 1)
        }

        Executor.execute(withDelay: time) { [weak viewController] in
            // Retain the handle so we can check for the cancelled status. Also cannot use the cancellable
            // concurrency API since the returned handle must be retained to ensure closure is executed.
            if let viewController = viewController, !handle.cancelled {
                let viewDidDisappear = (!viewController.isViewLoaded || viewController.view.window == nil)
                let message = "\(viewController) appearance has leaked. Either its parent router who does not own a view controller was detached, but failed to dismiss the leaked view controller; or the view controller is reused and re-added to window, yet the router is not re-attached but re-created. Objects are expected to be deallocated at this time: \(self.trackingObjects)"

                if self.disableLeakDetector {
                    if !viewDidDisappear {
                        print("Leak detection is disabled. This should only be used for debugging purposes.")
                        print(message)
                    }
                } else {
                    assert(viewDidDisappear, message)
                }
            }

            // A cancelled expectation was already taken off the count when it was cancelled.
            if handle.complete() {
                self.expectationCount.send(self.expectationCount.value - 1)
            }
        }

        return handle
    }

    // MARK: - Internal Interface

    // Test override for leak detectors.
    static var disableLeakDetectorOverride: Bool = false

    /// Test override: receives the message of each leaked object, in place of the assert.
    var leakReporter: ((String) -> Void)?

    #if DEBUG
        /// Reset the state of Leak Detector, internal for UI test only.
        func reset() {
            trackingObjects.removeAllObjects()
            expectationCount.send(0)
        }
    #endif

    // MARK: - Private Interface

    private let trackingObjects = NSMapTable<AnyObject, AnyObject>.strongToWeakObjects()
    private let expectationCount = OrderedCurrentValueSubject<Int, Never>(0)

    lazy var disableLeakDetector: Bool = {
        if let environmentValue = ProcessInfo().environment["DISABLE_LEAK_DETECTION"] {
            let lowercase = environmentValue.lowercased()
            return lowercase == "yes" || lowercase == "true"
        }
        return LeakDetector.disableLeakDetectorOverride
    }()

    private init() {}
}

fileprivate class LeakDetectionHandleImpl: LeakDetectionHandle {

    var cancelled: Bool {
        return cancelledSubject.value
    }

    let cancelledSubject = CurrentValueSubject<Bool, Never>(false)
    let cancelClosure: (() -> ())?

    init(cancelClosure: (() -> ())? = nil) {
        self.cancelClosure = cancelClosure
    }

    func cancel() {
        // An expectation ends once, either when it is cancelled or when its time is up.
        guard end(cancelling: true) else {
            return
        }
        cancelClosure?()
    }

    /// Ends the expectation because its time is up.
    ///
    /// - returns: `false` if the expectation was cancelled before its time was up.
    func complete() -> Bool {
        return end(cancelling: false)
    }

    // MARK: - Private

    private let lock = NSLock()
    private var didEnd = false

    private func end(cancelling: Bool) -> Bool {
        lock.lock()
        defer {
            lock.unlock()
        }
        guard !didEnd else {
            return false
        }
        didEnd = true
        if cancelling {
            cancelledSubject.send(true)
        }
        return true
    }
}
