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

final class RouterTests: XCTestCase {

    private var router: Router<Interactable>!
    private var lifecycleCancellable: AnyCancellable!

    // MARK: - Setup

    override func setUp() {
        super.setUp()

        router = Router(interactor: InteractableMock())
    }

    override func tearDown() {
        super.tearDown()

        lifecycleCancellable?.cancel()
    }

    // MARK: - Tests

    func test_load_verifyLifecyclePublisher() {
        var currentLifecycle: RouterLifecycle?
        var didComplete = false
        lifecycleCancellable = router
            .lifecycle
            .sink(receiveCompletion: { _ in
                currentLifecycle = nil
                didComplete = true
            }, receiveValue: { lifecycle in
                currentLifecycle = lifecycle
            })

        XCTAssertNil(currentLifecycle)
        XCTAssertFalse(didComplete)

        router.load()

        XCTAssertEqual(currentLifecycle, RouterLifecycle.didLoad)
        XCTAssertFalse(didComplete)

        router = nil

        XCTAssertNil(currentLifecycle)
        XCTAssertTrue(didComplete)
    }

    func test_attachChild_childActivenessFollowsParentInteractor() {
        let parentInteractor = Interactor()
        let childInteractor = Interactor()
        let grandchildInteractor = Interactor()
        let parentRouter = Router<Interactable>(interactor: parentInteractor)
        let childRouter = Router<Interactable>(interactor: childInteractor)
        let grandchildRouter = Router<Interactable>(interactor: grandchildInteractor)

        parentInteractor.activate()
        parentRouter.load()
        parentRouter.attachChild(childRouter)
        childRouter.attachChild(grandchildRouter)

        XCTAssertTrue(childInteractor.isActive)
        XCTAssertTrue(grandchildInteractor.isActive)

        parentInteractor.deactivate()

        XCTAssertFalse(childInteractor.isActive)
        XCTAssertFalse(grandchildInteractor.isActive)

        parentInteractor.activate()

        XCTAssertTrue(childInteractor.isActive)
        XCTAssertTrue(grandchildInteractor.isActive)

        parentRouter.detachChild(childRouter)

        XCTAssertFalse(childInteractor.isActive)
        XCTAssertFalse(grandchildInteractor.isActive)
    }

    func test_detachChild_thenAttachAgain_notifiesWorkersBeforeChildren() {
        // Repeated with new routers, since notifying in no particular order would only fail some of the time.
        for _ in 0..<8 {
            var events = [String]()
            let rootInteractor = Interactor()
            let rootRouter = Router<Interactable>(interactor: rootInteractor)
            rootInteractor.activate()
            rootRouter.load()

            let parentInteractor = LifecycleInteractor()
            let parentRouter = Router<Interactable>(interactor: parentInteractor)
            let childInteractor = LifecycleInteractor()
            let childRouter = Router<Interactable>(interactor: childInteractor)
            let worker = LifecycleWorker()
            worker.didStartHandler = { events.append("worker did start") }
            worker.didStopHandler = { events.append("worker did stop") }
            parentInteractor.didBecomeActiveHandler = { [weak parentInteractor] in
                events.append("parent did become active")
                if let parentInteractor = parentInteractor {
                    worker.start(parentInteractor)
                }
            }
            parentInteractor.willResignActiveHandler = { events.append("parent will resign active") }
            childInteractor.didBecomeActiveHandler = { events.append("child did become active") }
            childInteractor.willResignActiveHandler = { events.append("child will resign active") }

            rootRouter.attachChild(parentRouter)
            parentRouter.attachChild(childRouter)

            XCTAssertEqual(events, ["parent did become active", "worker did start", "child did become active"])
            events.removeAll()

            rootRouter.detachChild(parentRouter)

            XCTAssertEqual(events, ["parent will resign active", "worker did stop", "child will resign active"])
            events.removeAll()

            rootRouter.attachChild(parentRouter)

            XCTAssertEqual(events, ["worker did start", "child did become active", "parent did become active"])

            rootRouter.detachChild(parentRouter)
        }
    }

    func test_attachChild_detachedAgainWhileItsChildrenActivate_leavesItsWorkerStopped() {
        // Repeated with new routers, since notifying in no particular order would only fail some of the time.
        for _ in 0..<8 {
            let rootInteractor = Interactor()
            let rootRouter = Router<Interactable>(interactor: rootInteractor)
            rootInteractor.activate()
            rootRouter.load()

            let parentInteractor = LifecycleInteractor()
            let parentRouter = Router<Interactable>(interactor: parentInteractor)
            let childInteractor = LifecycleInteractor()
            let childRouter = Router<Interactable>(interactor: childInteractor)
            let worker = LifecycleWorker()
            parentInteractor.didBecomeActiveHandler = { [weak parentInteractor] in
                if let parentInteractor = parentInteractor {
                    worker.start(parentInteractor)
                }
            }

            rootRouter.attachChild(parentRouter)
            parentRouter.attachChild(childRouter)
            rootRouter.detachChild(parentRouter)

            XCTAssertEqual(worker.didStartCallCount, 1)
            XCTAssertEqual(worker.didStopCallCount, 1)

            // The next time the child becomes active, which is when its parent is attached again, it has the parent
            // detached straight away. The worker of the parent must not be left running on the detached parent.
            childInteractor.didBecomeActiveHandler = { [weak rootRouter, weak parentRouter] in
                if let rootRouter = rootRouter, let parentRouter = parentRouter {
                    rootRouter.detachChild(parentRouter)
                }
            }
            rootRouter.attachChild(parentRouter)

            XCTAssertFalse(parentInteractor.isActive)
            XCTAssertFalse(childInteractor.isActive)
            XCTAssertEqual(worker.didStartCallCount, 2)
            XCTAssertEqual(worker.didStopCallCount, 2)
        }
    }
}
