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

/// A subscriber with explicit demand, recording everything it receives.
private final class RecordingSubscriber<Input>: Subscriber {
    typealias Failure = Never

    private(set) var events = [String]()
    private var subscription: Subscription?
    private let initialDemand: Int

    init(initialDemand: Int) {
        self.initialDemand = initialDemand
    }

    func receive(subscription: Subscription) {
        self.subscription = subscription
        events.append("subscribed")
        if initialDemand > 0 {
            subscription.request(.max(initialDemand))
        }
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        events.append("\(input)")
        return .none
    }

    func receive(completion: Subscribers.Completion<Never>) {
        events.append("finished")
    }

    func request(_ count: Int) {
        subscription?.request(.max(count))
    }

    func cancel() {
        subscription?.cancel()
        events.append("cancelled")
    }
}

/// A tiny deterministic generator, so failures are reproducible.
private struct Generator {
    var state: UInt64
    mutating func next(_ bound: Int) -> Int {
        state = state &* 6364136223846793005 &+ 1442695040888963407
        return Int((state >> 33) % UInt64(bound))
    }
}

/// Holds an object weakly, to check whether it has been released.
private final class WeakReference<Object: AnyObject> {
    weak var object: Object?

    init(_ object: Object?) {
        self.object = object
    }
}

final class OrderedSubjectTests: XCTestCase {

    // MARK: - Ordering

    func test_send_notifiesSubscribersInSubscriptionOrder() {
        for _ in 0..<20 {
            let subject = OrderedSubject<Int, Never>()
            var order = [Int]()
            let cancellables = (0..<8).map { index in
                subject.sink { _ in order.append(index) }
            }
            subject.send(1)
            XCTAssertEqual(order, Array(0..<8))
            withExtendedLifetime(cancellables) {}
        }
    }

    func test_sendCompletion_notifiesSubscribersInSubscriptionOrder() {
        let subject = OrderedCurrentValueSubject<Int, Never>(0)
        var order = [Int]()
        let cancellables = (0..<8).map { index in
            subject.sink(receiveCompletion: { _ in order.append(index) }, receiveValue: { _ in })
        }
        subject.send(completion: .finished)
        XCTAssertEqual(order, Array(0..<8))
        withExtendedLifetime(cancellables) {}
    }

    func test_reentrantSend_isDeliveredNested() {
        let subject = OrderedCurrentValueSubject<Int, Never>(0)
        var log = [String]()
        let first = subject.sink { value in
            log.append("A\(value)")
            if value == 1 {
                subject.send(2)
            }
        }
        let second = subject.sink { value in
            log.append("B\(value)")
        }
        subject.send(1)
        XCTAssertEqual(log, ["A0", "B0", "A1", "A2", "B2", "B1"])
        XCTAssertEqual(subject.value, 2)
        withExtendedLifetime((first, second)) {}
    }

    // MARK: - Replay

    func test_currentValueSubject_replaysLatestValueToNewSubscribers() {
        let subject = OrderedCurrentValueSubject<Int, Never>(1)
        var early = [Int]()
        var late = [Int]()
        let earlyCancellable = subject.sink { early.append($0) }
        subject.send(2)
        let lateCancellable = subject.sink { late.append($0) }
        subject.send(3)
        XCTAssertEqual(early, [1, 2, 3])
        XCTAssertEqual(late, [2, 3])
        XCTAssertEqual(subject.value, 3)
        withExtendedLifetime((earlyCancellable, lateCancellable)) {}
    }

    func test_subject_doesNotReplay() {
        let subject = OrderedSubject<Int, Never>()
        var values = [Int]()
        subject.send(1)
        let cancellable = subject.sink { values.append($0) }
        subject.send(2)
        XCTAssertEqual(values, [2])
        withExtendedLifetime(cancellable) {}
    }

    // MARK: - Completion and cancellation

    func test_sendCompletion_lateSubscribersOnlyReceiveCompletion() {
        let subject = OrderedCurrentValueSubject<Int, Never>(1)
        subject.send(completion: .finished)
        subject.send(2)
        var events = [String]()
        let cancellable = subject.sink(receiveCompletion: { _ in events.append("finished") }, receiveValue: { events.append("\($0)") })
        XCTAssertEqual(events, ["finished"])
        XCTAssertEqual(subject.value, 1)
        withExtendedLifetime(cancellable) {}
    }

    func test_cancel_stopsDeliveryAndReleasesSubscriber() {
        final class Token {}
        let subject = OrderedSubject<Int, Never>()
        var values = [Int]()
        var token: Token? = Token()
        let weakToken = WeakReference(token)
        let cancellable = subject.sink { [token] value in
            _ = token
            values.append(value)
        }
        token = nil
        subject.send(1)
        XCTAssertNotNil(weakToken.object)
        cancellable.cancel()
        subject.send(2)
        XCTAssertEqual(values, [1])
        XCTAssertNil(weakToken.object)
    }

    func test_cancelDuringDelivery_skipsCancelledSubscriber() {
        let subject = OrderedSubject<Int, Never>()
        var log = [String]()
        var second: AnyCancellable?
        let first = subject.sink { value in
            log.append("A\(value)")
            second?.cancel()
        }
        second = subject.sink { value in
            log.append("B\(value)")
        }
        subject.send(1)
        subject.send(2)
        XCTAssertEqual(log, ["A1", "A2"])
        withExtendedLifetime(first) {}
    }

    func test_subscription_keepsSubjectAliveUntilCancelled() {
        var subject: OrderedSubject<Int, Never>? = OrderedSubject<Int, Never>()
        let weakSubject = WeakReference(subject)
        let cancellable = subject?.sink { _ in }
        subject = nil
        XCTAssertNotNil(weakSubject.object)
        cancellable?.cancel()
        XCTAssertNil(weakSubject.object)
    }

    // MARK: - Use as the subject of a multicast

    func test_multicast_sharesUpstreamAndNotifiesInSubscriptionOrder() {
        var subscribeCount = 0
        let upstream = PassthroughSubject<Int, Never>()
        let shared = upstream
            .handleEvents(receiveSubscription: { _ in subscribeCount += 1 })
            .multicast(subject: OrderedSubject<Int, Never>())
            .autoconnect()
        var log = [String]()
        let cancellables = (0..<5).map { index in
            shared.sink(receiveCompletion: { _ in log.append("\(index):finished") }, receiveValue: { log.append("\(index):\($0)") })
        }
        upstream.send(7)
        upstream.send(completion: .finished)
        XCTAssertEqual(subscribeCount, 1)
        XCTAssertEqual(log, ["0:7", "1:7", "2:7", "3:7", "4:7", "0:finished", "1:finished", "2:finished", "3:finished", "4:finished"])
        withExtendedLifetime(cancellables) {}
    }

    // MARK: - Same per-subscriber behavior as Combine's subjects

    func test_matchesCurrentValueSubject_forRandomOperationSequences() {
        for seed in 0..<400 {
            let stock = CurrentValueSubject<Int, Never>(-1)
            let ordered = OrderedCurrentValueSubject<Int, Never>(-1)
            let (stockLog, orderedLog) = runRandomOperations(seed: UInt64(seed), stock: stock, ordered: ordered)
            XCTAssertEqual(stockLog, orderedLog, "seed \(seed)")
            XCTAssertEqual(stock.value, ordered.value, "seed \(seed)")
        }
    }

    func test_matchesPassthroughSubject_forRandomOperationSequences() {
        for seed in 0..<400 {
            let stock = PassthroughSubject<Int, Never>()
            let ordered = OrderedSubject<Int, Never>()
            let (stockLog, orderedLog) = runRandomOperations(seed: UInt64(seed), stock: stock, ordered: ordered)
            XCTAssertEqual(stockLog, orderedLog, "seed \(seed)")
        }
    }

    private func runRandomOperations<Stock: Subject, Ordered: Subject>(seed: UInt64, stock: Stock, ordered: Ordered) -> ([[String]], [[String]]) where Stock.Output == Int, Stock.Failure == Never, Ordered.Output == Int, Ordered.Failure == Never {
        var generator = Generator(state: seed &* 2654435761 &+ 12345)
        var stockSubscribers = [RecordingSubscriber<Int>]()
        var orderedSubscribers = [RecordingSubscriber<Int>]()
        var nextValue = 0
        let operationCount = 10 + generator.next(40)
        for _ in 0..<operationCount {
            switch generator.next(10) {
            case 0, 1:
                let initialDemand = generator.next(3)
                let stockSubscriber = RecordingSubscriber<Int>(initialDemand: initialDemand)
                let orderedSubscriber = RecordingSubscriber<Int>(initialDemand: initialDemand)
                stockSubscribers.append(stockSubscriber)
                orderedSubscribers.append(orderedSubscriber)
                stock.subscribe(stockSubscriber)
                ordered.subscribe(orderedSubscriber)
            case 2, 3, 4, 5:
                stock.send(nextValue)
                ordered.send(nextValue)
                nextValue += 1
            case 6, 7:
                guard !stockSubscribers.isEmpty else { continue }
                let index = generator.next(stockSubscribers.count)
                let count = 1 + generator.next(3)
                stockSubscribers[index].request(count)
                orderedSubscribers[index].request(count)
            case 8:
                guard !stockSubscribers.isEmpty else { continue }
                let index = generator.next(stockSubscribers.count)
                stockSubscribers[index].cancel()
                orderedSubscribers[index].cancel()
            default:
                guard generator.next(6) == 0 else { continue }
                stock.send(completion: .finished)
                ordered.send(completion: .finished)
            }
        }
        return (stockSubscribers.map { $0.events }, orderedSubscribers.map { $0.events })
    }

    // MARK: - Thread safety

    func test_concurrentUse_deliversEveryValueToEverySubscriber() {
        let subject = OrderedCurrentValueSubject<Int, Never>(0)
        let lock = NSLock()
        var received = 0
        let steady = (0..<4).map { _ in
            subject.dropFirst().sink { _ in
                lock.lock()
                received += 1
                lock.unlock()
            }
        }
        DispatchQueue.concurrentPerform(iterations: 400) { index in
            if index % 4 == 0 {
                // Subscribers coming and going while values are sent.
                let transient = subject.sink { _ in }
                transient.cancel()
            } else {
                subject.send(index)
            }
            _ = subject.value
        }
        lock.lock()
        let total = received
        lock.unlock()
        XCTAssertEqual(total, 4 * 300)
        withExtendedLifetime(steady) {}
    }
}
