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

final class WorkerflowTests: XCTestCase {

    func test_nestedStepsDoNotRepeat() {
        var outerStep1RunCount = 0
        var outerStep2RunCount = 0
        var outerStep3RunCount = 0

        var innerStep1RunCount = 0
        var innerStep2RunCount = 0
        var innerStep3RunCount = 0

        let emptyPublisher = makeEmptyPublisher()

        let workflow = Workflow<String>()
        _ = workflow
            .onStep { (mock) -> AnyPublisher<((), ()), Error> in
                outerStep1RunCount += 1

                return emptyPublisher
            }
            .onStep { (_, _) -> AnyPublisher<((), ()), Error> in
                outerStep2RunCount += 1

                return emptyPublisher
            }
            .onStep { (_, _) -> AnyPublisher<((), ()), Error> in
                outerStep3RunCount += 1

                let innerStep: Step<String, (), ()> = emptyPublisher.fork(workflow)

                innerStep
                    .onStep({ (_, _) -> AnyPublisher<((), ()), Error> in
                        innerStep1RunCount += 1
                        return emptyPublisher
                    })
                    .onStep({ (_, _) -> AnyPublisher<((), ()), Error> in
                        innerStep2RunCount += 1
                        return emptyPublisher
                    })
                    .onStep({ (_, _) -> AnyPublisher<((), ()), Error> in
                        innerStep3RunCount += 1
                        return emptyPublisher
                    })
                    .commit()

                return emptyPublisher
            }
            .commit()
            .subscribe("Test Actionable Item")

        XCTAssertEqual(outerStep1RunCount, 1, "Outer step 1 should not have been run more than once")
        XCTAssertEqual(outerStep2RunCount, 1, "Outer step 2 should not have been run more than once")
        XCTAssertEqual(outerStep3RunCount, 1, "Outer step 3 should not have been run more than once")

        XCTAssertEqual(innerStep1RunCount, 1, "Inner step 1 should not have been run more than once")
        XCTAssertEqual(innerStep2RunCount, 1, "Inner step 2 should not have been run more than once")
        XCTAssertEqual(innerStep3RunCount, 1, "Inner step 3 should not have been run more than once")
    }

    func test_workflowReceivesError() {
        let workflow = TestWorkflow()

        let emptyPublisher = makeEmptyPublisher()
        _ = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return Fail(error: WorkflowTestError.error).eraseToAnyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(0, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.forkCallCount)
        XCTAssertEqual(1, workflow.errorCallCount)
    }

    func test_workflowDidComplete() {
        let workflow = TestWorkflow()

        let emptyPublisher = makeEmptyPublisher()
        _ = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.forkCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_workflowDidFork() {
        let workflow = TestWorkflow()

        let emptyPublisher = makeEmptyPublisher()
        _ = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                return emptyPublisher
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                let forkedStep: Step<(), (), ()> = emptyPublisher.fork(workflow)
                forkedStep
                    .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                        return emptyPublisher
                    }
                    .commit()
                return emptyPublisher
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(1, workflow.forkCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_fork_verifySingleInvocationAtRoot() {
        let workflow = TestWorkflow()

        var rootCallCount = 0
        let emptyPublisher = makeEmptyPublisher()
        let rootStep = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                rootCallCount += 1
                return emptyPublisher
        }

        let firstFork: Step<(), (), ()> = rootStep.asPublisher().fork(workflow)
        _ = firstFork
            .onStep { (_, _) -> AnyPublisher<((), ()), Error> in
                return makeEmptyPublisher()
            }
            .commit()

        let secondFork: Step<(), (), ()> = rootStep.asPublisher().fork(workflow)
        _ = secondFork
            .onStep { (_, _) -> AnyPublisher<((), ()), Error> in
                return makeEmptyPublisher()
            }
            .commit()

        XCTAssertEqual(0, rootCallCount)

        _ = workflow.subscribe(())

        XCTAssertEqual(1, rootCallCount)
    }

    func test_fork_fromSharedStep_runsEachBranchOnce() {
        let workflow = TestWorkflow()

        var rootCallCount = 0
        var mainBranchCallCount = 0
        var forkedBranchCallCount = 0
        let rootStep = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                rootCallCount += 1
                return makeEmptyPublisher()
            }
        rootStep
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                mainBranchCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()

        let forkedStep: Step<(), (), ()> = rootStep.asPublisher().fork(workflow)
        forkedStep
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                forkedBranchCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()

        _ = workflow.subscribe(())

        XCTAssertEqual(1, rootCallCount)
        XCTAssertEqual(1, mainBranchCallCount)
        XCTAssertEqual(1, forkedBranchCallCount)
        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(1, workflow.forkCallCount)
    }

    func test_fork_branchesRunInTheOrderTheyWereCommitted() {
        // Repeated with new workflows, since notifying in no particular order would only fail some of the time.
        for _ in 0..<8 {
            let workflow = TestWorkflow()
            var order = [Int]()
            let rootStep = workflow
                .onStep { _ -> AnyPublisher<((), ()), Error> in
                    return makeEmptyPublisher()
                }
            for index in 0..<4 {
                let forkedStep: Step<(), (), ()> = rootStep.asPublisher().fork(workflow)
                forkedStep
                    .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                        order.append(index)
                        return makeEmptyPublisher()
                    }
                    .commit()
            }

            _ = workflow.subscribe(())

            XCTAssertEqual(order, [0, 1, 2, 3])
            XCTAssertEqual(1, workflow.completeCallCount)
        }
    }

    func test_fork_fromStepThatAlreadyEmitted_doesNotKeepWorkflowAlive() {
        var workflow: TestWorkflow? = TestWorkflow()
        var didDeinit = false
        workflow?.deinitHandler = {
            didDeinit = true
        }

        var forkedStepCallCount = 0
        if let workflow = workflow {
            let rootStep = workflow
                .onStep { _ -> AnyPublisher<((), ()), Error> in
                    return makeEmptyPublisher()
                }
            rootStep
                .onStep { [weak workflow] _, _ -> AnyPublisher<((), ()), Error> in
                    if let workflow = workflow {
                        // The root step has already emitted its value at this point, so the forked step never runs.
                        let forkedStep: Step<(), (), ()> = rootStep.asPublisher().fork(workflow)
                        forkedStep
                            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                                forkedStepCallCount += 1
                                return makeEmptyPublisher()
                            }
                            .commit()
                    }
                    return makeEmptyPublisher()
                }
                .commit()
            _ = workflow.subscribe(())
        }

        XCTAssertEqual(0, forkedStepCallCount)
        XCTAssertEqual(1, workflow?.completeCallCount)

        workflow = nil

        XCTAssertTrue(didDeinit)
    }

    func test_step_isCancelledAfterTheFollowingStepsRan() {
        let workflow = TestWorkflow()
        let firstStepSubject = PassthroughSubject<((), ()), Error>()
        var events = [String]()

        _ = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return firstStepSubject
                    .handleEvents(receiveCancel: {
                        events.append("first step cancelled")
                    })
                    .eraseToAnyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                events.append("second step")
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(events, [])

        firstStepSubject.send(((), ()))

        XCTAssertEqual(events, ["second step", "first step cancelled"])
        XCTAssertEqual(1, workflow.completeCallCount)
    }

    func test_step_completesWithoutValue_completesWorkflowWithoutRunningFollowingSteps() {
        let workflow = TestWorkflow()

        var secondStepCallCount = 0
        _ = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return Empty().eraseToAnyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                secondStepCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(0, secondStepCallCount)
        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_subscribe_keepsWorkflowAliveUntilItCompletes() {
        let interactor = Interactor()
        var workflow: TestWorkflow? = TestWorkflow()
        var didDeinit = false
        workflow?.deinitHandler = {
            didDeinit = true
        }

        var stepCallCount = 0
        // Neither the workflow nor the returned cancellable is retained by the test past this point.
        _ = workflow?
            .onStep { _ -> AnyPublisher<(Interactor, ()), Error> in
                return Just((interactor, ()))
                    .setFailureType(to: Error.self)
                    .eraseToAnyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                stepCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())
        workflow = nil

        XCTAssertFalse(didDeinit)
        XCTAssertEqual(0, stepCallCount)

        interactor.activate()

        XCTAssertEqual(1, stepCallCount)
        XCTAssertTrue(didDeinit)
    }

    func test_step_waitsForInteractorToBecomeActive() {
        let workflow = TestWorkflow()
        let interactor = Interactor()

        var stepCallCount = 0
        // The returned cancellable is discarded on purpose, the workflow is expected to keep running.
        _ = workflow
            .onStep { _ -> AnyPublisher<(Interactor, ()), Error> in
                return Just((interactor, ()))
                    .setFailureType(to: Error.self)
                    .eraseToAnyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                stepCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(0, stepCallCount)
        XCTAssertEqual(0, workflow.completeCallCount)

        interactor.activate()

        XCTAssertEqual(1, stepCallCount)
        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_subscribe_cancel_stopsWorkflow() {
        let workflow = TestWorkflow()
        let interactor = Interactor()

        var stepCallCount = 0
        let cancellable = workflow
            .onStep { _ -> AnyPublisher<(Interactor, ()), Error> in
                return Just((interactor, ()))
                    .setFailureType(to: Error.self)
                    .eraseToAnyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                stepCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        cancellable.cancel()
        interactor.activate()

        XCTAssertEqual(0, stepCallCount)
        XCTAssertEqual(0, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_onError_invokedWithStepError() {
        let workflow = TestWorkflow()

        var receivedError: Error?
        _ = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return Fail(error: WorkflowTestError.error).eraseToAnyPublisher()
            }
            .onError { error in
                receivedError = error
            }
            .commit()
            .subscribe(())

        XCTAssertTrue(receivedError is WorkflowTestError)
        XCTAssertEqual(1, workflow.errorCallCount)
    }

    func test_cancelWith_cancelsSubscriptionWhenWorkflowIsCancelled() {
        let workflow = TestWorkflow()
        let subject = PassthroughSubject<Int, Never>()
        var values = [Int]()

        let cancellable = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        subject
            .sink { values.append($0) }
            .cancelWith(workflow: workflow)

        subject.send(1)

        XCTAssertEqual(values, [1])

        cancellable.cancel()
        subject.send(2)

        XCTAssertEqual(values, [1])
    }

    func test_step_actionableItemReactivatedDuringStep_runsFollowingSteps() {
        let workflow = TestWorkflow()
        let interactor = Interactor()
        interactor.activate()

        var events = [String]()
        _ = workflow
            .onStep { _ -> AnyPublisher<(Interactor, ()), Error> in
                return Just((interactor, ()))
                    .setFailureType(to: Error.self)
                    .eraseToAnyPublisher()
            }
            .onStep { interactor, _ -> AnyPublisher<((), ()), Error> in
                // For example a step that makes the parent router detach and attach this RIB again.
                interactor.deactivate()
                interactor.activate()
                events.append("second step, completeCallCount: \(workflow.completeCallCount)")
                return makeEmptyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                events.append("third step")
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(events, ["second step, completeCallCount: 0", "third step"])
        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_step_emitsAgainWhileABranchRuns_runsEveryBranchOnce() {
        let workflow = TestWorkflow()
        let stepSubject = PassthroughSubject<((), ()), Error>()

        var mainBranchCallCount = 0
        var forkedBranchCallCount = 0
        let rootStep = workflow
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return stepSubject.eraseToAnyPublisher()
            }
        rootStep
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                mainBranchCallCount += 1
                stepSubject.send(((), ()))
                return makeEmptyPublisher()
            }
            .commit()

        let forkedStep: Step<(), (), ()> = rootStep.asPublisher().fork(workflow)
        forkedStep
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                forkedBranchCallCount += 1
                stepSubject.send(((), ()))
                return makeEmptyPublisher()
            }
            .commit()

        _ = workflow.subscribe(())
        stepSubject.send(((), ()))

        XCTAssertEqual(1, mainBranchCallCount)
        XCTAssertEqual(1, forkedBranchCallCount)
        XCTAssertEqual(1, workflow.completeCallCount)
    }

    func test_fork_fromStepOfForkedChainThatAlreadyEmitted_doesNotRunStepAgain() {
        let workflow = TestWorkflow()

        var firstStepCallCount = 0
        var secondStepCallCount = 0
        var thirdStepCallCount = 0
        var nestedForkCallCount = 0

        // Every step of a fork from a publisher that emits when it is subscribed to runs during `commit()`.
        let forkedStep: Step<(), (), ()> = makeEmptyPublisher().fork(workflow)
        let firstStep = forkedStep
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                firstStepCallCount += 1
                return makeEmptyPublisher()
            }
        firstStep
            .onStep { [weak workflow] _, _ -> AnyPublisher<((), ()), Error> in
                secondStepCallCount += 1
                if let workflow = workflow {
                    // The first step has already emitted its value at this point, so the nested fork never runs.
                    let nestedFork: Step<(), (), ()> = firstStep.asPublisher().fork(workflow)
                    nestedFork
                        .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                            nestedForkCallCount += 1
                            return makeEmptyPublisher()
                        }
                        .commit()
                }
                return makeEmptyPublisher()
            }
            .onStep { _, _ -> AnyPublisher<((), ()), Error> in
                thirdStepCallCount += 1
                return makeEmptyPublisher()
            }
            .commit()

        XCTAssertEqual(1, firstStepCallCount)
        XCTAssertEqual(1, secondStepCallCount)
        XCTAssertEqual(1, thirdStepCallCount)
        XCTAssertEqual(0, nestedForkCallCount)
        XCTAssertEqual(1, workflow.completeCallCount)
        XCTAssertEqual(0, workflow.errorCallCount)
    }

    func test_workflow_isReleasedAfterCompletion() {
        var workflow: TestWorkflow? = TestWorkflow()
        var didDeinit = false
        workflow?.deinitHandler = {
            didDeinit = true
        }

        _ = workflow?
            .onStep { _ -> AnyPublisher<((), ()), Error> in
                return makeEmptyPublisher()
            }
            .commit()
            .subscribe(())

        XCTAssertEqual(1, workflow?.completeCallCount)

        workflow = nil

        XCTAssertTrue(didDeinit)
    }
}

private enum WorkflowTestError: Error {
    case error
}

private func makeEmptyPublisher() -> AnyPublisher<((), ()), Error> {
    return Just(((), ()))
        .setFailureType(to: Error.self)
        .eraseToAnyPublisher()
}

private class TestWorkflow: Workflow<()> {
    var completeCallCount = 0
    var errorCallCount = 0
    var forkCallCount = 0
    var deinitHandler: (() -> ())?

    override func didComplete() {
        completeCallCount += 1
    }

    override func didFork() {
        forkCallCount += 1
    }

    override func didReceiveError(_ error: Error) {
        errorCallCount += 1
    }

    deinit {
        deinitHandler?()
    }
}
