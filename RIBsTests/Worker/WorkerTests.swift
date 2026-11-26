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

final class WorkerTests: XCTestCase {

    private var worker: TestWorker!
    private var interactor: InteractorMock!
    private var cancellables: Set<AnyCancellable>!

    // MARK: - Setup

    override func setUp() {
        super.setUp()

        cancellables = Set<AnyCancellable>()

        worker = TestWorker()
        interactor = InteractorMock()
    }

    // MARK: - Tests

    func test_didStart_onceOnly_boundToInteractor() {
        XCTAssertEqual(worker.didStartCallCount, 0)
        XCTAssertEqual(worker.didStopCallCount, 0)

        worker.start(interactor)

        XCTAssertTrue(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 0)
        XCTAssertEqual(worker.didStopCallCount, 0)

        interactor.activate()

        XCTAssertTrue(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 1)
        XCTAssertEqual(worker.didStopCallCount, 0)

        interactor.deactivate()

        XCTAssertTrue(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 1)
        XCTAssertEqual(worker.didStopCallCount, 1)

        worker.start(interactor)

        XCTAssertTrue(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 1)
        XCTAssertEqual(worker.didStopCallCount, 1)

        interactor.activate()

        XCTAssertTrue(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 2)
        XCTAssertEqual(worker.didStopCallCount, 1)

        worker.stop()

        XCTAssertFalse(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 2)
        XCTAssertEqual(worker.didStopCallCount, 2)

        worker.stop()

        XCTAssertFalse(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 2)
        XCTAssertEqual(worker.didStopCallCount, 2)
    }

    func test_start_stop_lifecycle() {
        // Collect the values, so the test fails if the stream does not emit at all.
        var values = [Bool]()

        worker.isStartedStream
            .first()
            .sink { values.append($0) }
            .store(in: &cancellables)

        XCTAssertEqual(values, [false])

        interactor.activate()
        worker.start(interactor)

        worker.isStartedStream
            .first()
            .sink { values.append($0) }
            .store(in: &cancellables)

        XCTAssertEqual(values, [false, true])

        worker.stop()

        worker.isStartedStream
            .first()
            .sink { values.append($0) }
            .store(in: &cancellables)

        XCTAssertEqual(values, [false, true, false])
    }

    func test_start_fromDidStart_bindsToTheNewInteractor() {
        let otherInteractor = InteractorMock()
        interactor.activate()
        otherInteractor.activate()

        worker.didStartHandler = { [worker] in
            // Move the worker to the other interactor the first time it starts.
            worker?.didStartHandler = nil
            worker?.stop()
            worker?.start(otherInteractor)
        }
        worker.start(interactor)

        XCTAssertTrue(worker.isStarted)
        XCTAssertEqual(worker.didStartCallCount, 2)
        XCTAssertEqual(worker.didStopCallCount, 1)

        interactor.deactivate()

        XCTAssertEqual(worker.didStopCallCount, 1)

        otherInteractor.deactivate()

        XCTAssertEqual(worker.didStopCallCount, 2)

        otherInteractor.activate()

        XCTAssertEqual(worker.didStartCallCount, 3)
    }

    func test_workers_stopAndStartAgainInTheOrderTheyWereStarted() {
        // Repeated with new interactors, since notifying in no particular order would only fail some of the time.
        for _ in 0..<8 {
            let interactor = Interactor()
            var events = [String]()
            let workers = (0..<4).map { index -> LifecycleWorker in
                let worker = LifecycleWorker()
                worker.didStartHandler = { events.append("start \(index)") }
                worker.didStopHandler = { events.append("stop \(index)") }
                return worker
            }

            interactor.activate()
            workers.forEach { $0.start(interactor) }

            XCTAssertEqual(events, ["start 0", "start 1", "start 2", "start 3"])
            events.removeAll()

            interactor.deactivate()

            XCTAssertEqual(events, ["stop 0", "stop 1", "stop 2", "stop 3"])
            events.removeAll()

            interactor.activate()

            XCTAssertEqual(events, ["start 0", "start 1", "start 2", "start 3"])
        }
    }

    func test_cancelOnStop_cancelsSubscriptionWhenWorkerStops() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()

        interactor.activate()
        worker.start(interactor)

        // The returned cancellable is discarded on purpose, the worker is expected to retain the subscription.
        subject
            .sink { values.append($0) }
            .cancelOnStop(worker)

        subject.send(1)

        XCTAssertEqual(values, [1])

        worker.stop()
        subject.send(2)

        XCTAssertEqual(values, [1])
    }

    func test_cancelOnStop_stoppedWorker_cancelsSubscriptionImmediately() {
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()

        let cancellable = subject
            .sink { values.append($0) }
            .cancelOnStop(worker)

        withExtendedLifetime(cancellable) {
            subject.send(1)
        }

        XCTAssertEqual(values, [])
    }
}

private final class TestWorker: Worker {

    private(set) var didStartCallCount: Int = 0
    private(set) var didStopCallCount: Int = 0
    var didStartHandler: (() -> ())?

    // MARK: - Overrides

    override func didStart(_ interactorScope: InteractorScope) {
        super.didStart(interactorScope)

        didStartCallCount += 1
        didStartHandler?()
    }

    override func didStop() {
        super.didStop()

        didStopCallCount += 1
    }
}
