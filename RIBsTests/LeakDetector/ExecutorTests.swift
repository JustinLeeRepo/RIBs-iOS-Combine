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

import XCTest
@testable import RIBs

final class ExecutorTests: XCTestCase {

    func test_execute_runsLogicOnceAfterDelay() {
        let didExecute = expectation(description: "Executor did execute the logic.")
        let delay: TimeInterval = 0.05
        let startTime = Date()
        var executeCount = 0
        var executeTime: Date?

        Executor.execute(withDelay: delay) {
            executeCount += 1
            executeTime = Date()
            didExecute.fulfill()
        }

        XCTAssertEqual(executeCount, 0)

        wait(for: [didExecute], timeout: 5)

        // The executor only counts time that elapsed, so the logic can never run before the delay is over.
        XCTAssertGreaterThanOrEqual(executeTime?.timeIntervalSince(startTime) ?? 0, delay)

        // Give the executor time to fire again, to verify it does not.
        let didSettle = expectation(description: "Executor did settle.")
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
            didSettle.fulfill()
        }
        wait(for: [didSettle], timeout: 5)

        XCTAssertEqual(executeCount, 1)
    }
}
