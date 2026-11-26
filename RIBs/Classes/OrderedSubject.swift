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

/// A subject that notifies its subscribers in the order they subscribed, and only delivers values sent after
/// subscription, like `PassthroughSubject`.
///
/// Combine's own subjects notify multiple subscribers in an unspecified order. The lifecycle streams of RIBs have
/// several subscribers at once (workers, child routers, workflow steps), whose callbacks are expected to run in a
/// predictable sequence, so these streams are backed by this subject instead.
class OrderedSubject<Output, Failure: Error>: Subject {

    /// Initializer.
    init() {}

    /// Sends the given value to the subscribers, in the order they subscribed.
    ///
    /// - parameter value: The value to send.
    final func send(_ value: Output) {
        lock.lock()
        guard completion == nil else {
            lock.unlock()
            return
        }
        lockedStore(value)
        let conduits = self.conduits
        lock.unlock()

        for conduit in conduits {
            conduit.offer(value)
        }
    }

    /// Sends the given completion to the subscribers, in the order they subscribed. Subscribers attached after this
    /// immediately receive the same completion.
    ///
    /// - parameter completion: The completion to send.
    final func send(completion: Subscribers.Completion<Failure>) {
        lock.lock()
        guard self.completion == nil else {
            lock.unlock()
            return
        }
        self.completion = completion
        let conduits = self.conduits
        self.conduits = []
        lock.unlock()

        for conduit in conduits {
            conduit.finish(with: completion)
        }
    }

    /// Requests unlimited values from the given upstream subscription, when this subject is used as a subscriber.
    ///
    /// - parameter subscription: The upstream subscription.
    final func send(subscription: Subscription) {
        subscription.request(.unlimited)
    }

    final func receive<S: Subscriber>(subscriber: S) where S.Input == Output, S.Failure == Failure {
        lock.lock()
        if let completion = completion {
            lock.unlock()
            subscriber.receive(subscription: Subscriptions.empty)
            subscriber.receive(completion: completion)
            return
        }
        let conduit = Conduit(subject: self, downstream: AnySubscriber(subscriber))
        conduits.append(conduit)
        lock.unlock()

        subscriber.receive(subscription: conduit)
    }

    // MARK: - Fileprivate

    fileprivate let lock = NSLock()

    /// Stores the given value as the latest value. Invoked with the lock held.
    fileprivate func lockedStore(_ value: Output) {
        // No-op
    }

    /// The latest value to replay to subscribers, `nil` if this subject does not replay. Invoked with the lock held.
    fileprivate func lockedLatestValue() -> Output? {
        return nil
    }

    // MARK: - Private

    private var conduits = [Conduit]()
    private var completion: Subscribers.Completion<Failure>?

    private func latestValue() -> Output? {
        lock.lock()
        defer {
            lock.unlock()
        }
        return lockedLatestValue()
    }

    private func remove(_ conduit: Conduit) {
        lock.lock()
        conduits.removeAll { $0 === conduit }
        lock.unlock()
    }

    /// The subscription of a single subscriber.
    private final class Conduit: Subscription {

        init(subject: OrderedSubject, downstream: AnySubscriber<Output, Failure>) {
            self.subject = subject
            self.downstream = downstream
        }

        func offer(_ value: Output) {
            lock.lock()
            guard let downstream = downstream else {
                lock.unlock()
                return
            }
            guard demand > 0 else {
                // The subscriber is not ready for this value. A replaying subject delivers its latest value once
                // the subscriber asks for more.
                needsLatestValue = true
                lock.unlock()
                return
            }
            demand -= 1
            needsLatestValue = false
            lock.unlock()

            deliver(value, to: downstream)
        }

        func finish(with completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard let downstream = downstream else {
                lock.unlock()
                return
            }
            self.downstream = nil
            subject = nil
            lock.unlock()

            downstreamLock.lock()
            downstream.receive(completion: completion)
            downstreamLock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard let downstream = downstream else {
                lock.unlock()
                return
            }
            self.demand += demand
            guard needsLatestValue, self.demand > 0 else {
                lock.unlock()
                return
            }
            needsLatestValue = false
            guard let value = subject?.latestValue() else {
                lock.unlock()
                return
            }
            self.demand -= 1
            lock.unlock()

            deliver(value, to: downstream)
        }

        func cancel() {
            lock.lock()
            guard downstream != nil else {
                lock.unlock()
                return
            }
            downstream = nil
            let subject = self.subject
            self.subject = nil
            lock.unlock()

            subject?.remove(self)
        }

        // MARK: - Private

        private let lock = NSLock()
        // Recursive, since a subscriber may send to the subject again while it is receiving a value.
        private let downstreamLock = NSRecursiveLock()
        private var subject: OrderedSubject?
        private var downstream: AnySubscriber<Output, Failure>?
        private var demand = Subscribers.Demand.none
        private var needsLatestValue = true

        private func deliver(_ value: Output, to downstream: AnySubscriber<Output, Failure>) {
            downstreamLock.lock()
            let additionalDemand = downstream.receive(value)
            downstreamLock.unlock()

            guard additionalDemand > 0 else {
                return
            }
            lock.lock()
            demand += additionalDemand
            lock.unlock()
        }
    }
}

/// A subject that notifies its subscribers in the order they subscribed, and replays its latest value to new
/// subscribers, like `CurrentValueSubject`.
final class OrderedCurrentValueSubject<Output, Failure: Error>: OrderedSubject<Output, Failure> {

    /// The latest value of this subject.
    var value: Output {
        lock.lock()
        defer {
            lock.unlock()
        }
        return latest
    }

    /// Initializer.
    ///
    /// - parameter value: The initial value of this subject.
    init(_ value: Output) {
        latest = value
    }

    // MARK: - Fileprivate

    fileprivate override func lockedStore(_ value: Output) {
        latest = value
    }

    fileprivate override func lockedLatestValue() -> Output? {
        return latest
    }

    // MARK: - Private

    private var latest: Output
}
