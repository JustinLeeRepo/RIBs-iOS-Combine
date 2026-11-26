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

final class InteractorTests: XCTestCase {

    private var interactor: Interactor!

    // MARK: - Setup

    override func setUp() {
        super.setUp()

        interactor = Interactor()
    }

    // MARK: - Tests

    func test_isActiveStream_emitsActivenessAndCompletesOnDeinit() {
        var values = [Bool]()
        var didComplete = false
        let cancellable = interactor.isActiveStream
            .sink(receiveCompletion: { _ in
                didComplete = true
            }, receiveValue: { isActive in
                values.append(isActive)
            })

        XCTAssertEqual(values, [false])

        interactor.activate()
        interactor.activate()

        XCTAssertTrue(interactor.isActive)
        XCTAssertEqual(values, [false, true])

        interactor.deactivate()

        XCTAssertFalse(interactor.isActive)
        XCTAssertEqual(values, [false, true, false])
        XCTAssertFalse(didComplete)

        interactor = nil

        XCTAssertTrue(didComplete)

        cancellable.cancel()
    }

    func test_isActiveStream_notifiesSubscribersInSubscriptionOrder() {
        // Repeated with new interactors, since notifying in no particular order would only fail some of the time.
        for _ in 0..<8 {
            let interactor = Interactor()
            var order = [Int]()
            let cancellables = (0..<5).map { index in
                interactor.isActiveStream
                    .dropFirst()
                    .sink { _ in order.append(index) }
            }

            interactor.activate()

            XCTAssertEqual(order, [0, 1, 2, 3, 4])
            withExtendedLifetime(cancellables) {}
        }
    }

    func test_cancelOnDeactivate_cancelsSubscriptionWhenInteractorDeactivates() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()

        interactor.activate()

        // The returned cancellable is discarded on purpose, the interactor is expected to retain the subscription.
        subject
            .sink { values.append($0) }
            .cancelOnDeactivate(interactor: interactor)

        subject.send(1)

        XCTAssertEqual(values, [1])

        interactor.deactivate()
        subject.send(2)

        XCTAssertEqual(values, [1])
    }

    func test_cancelOnDeactivate_inactiveInteractor_cancelsSubscriptionImmediately() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()

        let cancellable = subject
            .sink { values.append($0) }
            .cancelOnDeactivate(interactor: interactor)

        withExtendedLifetime(cancellable) {
            subject.send(1)
        }

        XCTAssertEqual(values, [])
    }

    func test_confineTo_emitsLatestValueOnlyWhileInteractorIsActive() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()
        let cancellable = subject
            .confineTo(interactor)
            .sink { values.append($0) }

        subject.send(1)

        XCTAssertEqual(values, [])

        interactor.activate()

        XCTAssertEqual(values, [1])

        subject.send(2)

        XCTAssertEqual(values, [1, 2])

        interactor.deactivate()
        subject.send(3)

        XCTAssertEqual(values, [1, 2])

        interactor.activate()

        XCTAssertEqual(values, [1, 2, 3])

        cancellable.cancel()
    }

    func test_confineTo_valueSentWhileHandlingAValue_isEmitted() {
        let subject = CurrentValueSubject<Int, Never>(0)
        var values = [Int]()

        interactor.activate()

        let cancellable = subject
            .confineTo(interactor)
            .sink { value in
                values.append(value)
                if value < 3 {
                    subject.send(value + 1)
                }
            }

        XCTAssertEqual(values, [0, 1, 2, 3])

        cancellable.cancel()
    }

    func test_confineTo_interactorReactivatedWhileHandlingAValue_emitsLatestValueAgain() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()

        interactor.activate()

        let cancellable = subject
            .confineTo(interactor)
            .sink { [interactor] value in
                values.append(value)
                if values.count == 1 {
                    interactor?.deactivate()
                    interactor?.activate()
                }
            }

        subject.send(1)

        XCTAssertEqual(values, [1, 1])

        cancellable.cancel()
    }

    func test_confineTo_scopeThatDoesNotReplayItsActiveness_emitsWhileActive() {
        let interactorScope = InteractableMock()
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()
        let cancellable = subject
            .confineTo(interactorScope)
            .sink { values.append($0) }

        subject.send(1)

        XCTAssertEqual(values, [])

        interactorScope.isActiveStreamSubject.send(true)

        XCTAssertEqual(values, [1])

        subject.send(2)

        XCTAssertEqual(values, [1, 2])

        interactorScope.isActiveStreamSubject.send(false)
        subject.send(3)

        XCTAssertEqual(values, [1, 2])

        interactorScope.isActiveStreamSubject.send(true)
        subject.send(4)

        XCTAssertEqual(values, [1, 2, 3, 4])

        cancellable.cancel()
    }

    func test_confineTo_completesWithPublisherAfterInteractorIsDeallocated() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()
        var didComplete = false
        let cancellable = subject
            .confineTo(interactor)
            .sink(receiveCompletion: { _ in
                didComplete = true
            }, receiveValue: { value in
                values.append(value)
            })

        interactor.activate()
        subject.send(1)
        interactor = nil
        subject.send(2)

        XCTAssertEqual(values, [1])
        XCTAssertFalse(didComplete)

        subject.send(completion: .finished)

        XCTAssertTrue(didComplete)

        cancellable.cancel()
    }
}
