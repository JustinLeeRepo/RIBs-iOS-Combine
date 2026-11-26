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
import XCTest
@testable import RIBs

final class LeakDetectorTests: XCTestCase {

    private var detector: LeakDetector!
    private var statuses: [LeakDetectionStatus]!
    private var statusCancellable: AnyCancellable!

    // MARK: - Setup

    override func setUp() {
        super.setUp()

        detector = LeakDetector.instance
        detector.reset()
        statuses = []
        statusCancellable = detector.status.sink { [weak self] status in
            self?.statuses.append(status)
        }
    }

    override func tearDown() {
        statusCancellable.cancel()
        detector.reset()

        super.tearDown()
    }

    // MARK: - Tests

    func test_status_cancelledExpectation_isTakenOffTheCountOnce() {
        let object = NSObject()
        let handle = detector.expectDeallocate(object: object, inTime: 0.05)

        XCTAssertEqual(statuses, [.DidComplete, .InProgress])

        handle.cancel()
        handle.cancel()

        XCTAssertEqual(statuses, [.DidComplete, .InProgress, .DidComplete])

        // Let the time of the cancelled expectation run out.
        waitForExecutor(withDelay: 0.1)

        // If the cancelled expectation was taken off the count a second time, the status would miss this expectation.
        let otherObject = NSObject()
        let otherHandle = detector.expectDeallocate(object: otherObject, inTime: 10)

        XCTAssertEqual(statuses, [.DidComplete, .InProgress, .DidComplete, .InProgress])

        otherHandle.cancel()

        XCTAssertEqual(statuses, [.DidComplete, .InProgress, .DidComplete, .InProgress, .DidComplete])
        withExtendedLifetime((object, otherObject)) {}
    }

    func test_status_cancelAfterExpectationCompleted_isTakenOffTheCountOnce() {
        var object: NSObject? = NSObject()
        let handle = detector.expectDeallocate(object: object!, inTime: 0.05)
        object = nil

        waitForExecutor(withDelay: 0.1)

        XCTAssertEqual(statuses, [.DidComplete, .InProgress, .DidComplete])

        handle.cancel()

        let otherObject = NSObject()
        let otherHandle = detector.expectDeallocate(object: otherObject, inTime: 10)

        XCTAssertEqual(statuses, [.DidComplete, .InProgress, .DidComplete, .InProgress])

        otherHandle.cancel()
        withExtendedLifetime(otherObject) {}
    }

    func test_expectDeallocate_objectAllocatedAtAFreedObjectsAddress_doesNotReportTheFreedObject() throws {
        var leaks: [String] = []
        detector.leakReporter = { leaks.append($0) }
        defer { detector.leakReporter = nil }

        var freed: NSObject? = NSObject()
        let freedAddress = ObjectIdentifier(freed!)
        detector.expectDeallocate(object: freed!, inTime: 0.05)
        freed = nil

        // The allocator hands a freed address out again; find the object that got it.
        var candidates: [NSObject] = []
        var reused: NSObject?
        for _ in 0..<10_000 where reused == nil {
            let candidate = NSObject()
            if ObjectIdentifier(candidate) == freedAddress {
                reused = candidate
            } else {
                candidates.append(candidate)
            }
        }
        candidates.removeAll()
        guard let reused else {
            throw XCTSkip("No new object was allocated at the freed object's address.")
        }

        // Still alive when the freed object's expectation runs out.
        detector.expectDeallocate(object: reused, inTime: 10).cancel()
        waitForExecutor(withDelay: 0.1)

        XCTAssertEqual(leaks, [])
        withExtendedLifetime(reused) {}
    }

    func test_expectDeallocate_objectStillAlive_isReported() {
        var leaks: [String] = []
        detector.leakReporter = { leaks.append($0) }
        defer { detector.leakReporter = nil }

        let object = NSObject()
        detector.expectDeallocate(object: object, inTime: 0.05)
        waitForExecutor(withDelay: 0.1)

        XCTAssertEqual(leaks.count, 1)
        withExtendedLifetime(object) {}
    }

    // MARK: - Private

    /// Waits for an executor that is scheduled after, and with a longer delay than, the ones under test. It
    /// therefore runs after they did.
    private func waitForExecutor(withDelay delay: TimeInterval) {
        let didExecute = expectation(description: "Executor did execute.")
        Executor.execute(withDelay: delay) {
            didExecute.fulfill()
        }
        wait(for: [didExecute], timeout: 5)
    }
}
