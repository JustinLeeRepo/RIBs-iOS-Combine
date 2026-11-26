<p align="center">
<img src="https://github.com/uber/ribs/blob/assets/rib_horizontal_black.png" width="60%" height="60%" alt="RIBs"/>
</p>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> This is a fork of [uber/RIBs-iOS](https://github.com/uber/RIBs-iOS) that uses Apple's Combine framework instead of RxSwift. It has no third-party dependencies, requires iOS 15 or later, and is distributed through Swift Package Manager only.

RIBs is the cross-platform architecture framework behind many mobile apps at Uber. The name RIBs is short for Router, Interactor and Builder, which are core components of this architecture. This framework is designed for mobile apps with a large number of engineers and nested states.

The RIBs architecture provides:
* **Shared architecture across iOS and Android.** Build cross-platform apps that have similar architecture, enabling iOS and Android teams to cross-review business logic code.
* **Testability and Isolation.** Classes must be easy to unit test and reason about in isolation. Individual RIB classes have distinct responsibilities like: routing, business, view logic, creation. Plus, most RIB logic is decoupled from child RIB logic. This makes RIB classes easy to test and reason about independently.
* **Tooling for developer productivity.** RIBs come with IDE tooling around code generation, memory leak detection, static analysis and runtime integrations - all which improve developer productivity for large teams or small.
* **An architecture that scales.** This architecture has proven to scale to hundreds of engineers working on the same codebase and apps with hundreds of RIBs.


## Documentation
To get started with RIBs, please refer to the [RIBs documentation](https://github.com/uber/RIBs/wiki). This describes key concepts on RIBs, from what they are for, their structure and common use cases.

To get more hands on with RIBs, we have written a [series of tutorials](https://github.com/uber/RIBs/wiki) that run you through the main aspects of the architecture with hands-on examples.

The tutorial projects are in the [tutorials](tutorials) folder, and build a TicTacToe game step by step. They use the Combine API of this fork, while the documentation and the tutorial steps on the wiki were written for RxSwift. Where the wiki shows RxSwift code, use the Combine equivalent:

| RxSwift, as shown on the wiki | This fork |
| --- | --- |
| `import RxSwift` | `import Combine` |
| `Observable<T>` | `AnyPublisher<T, Never>`, or `AnyPublisher<T, Error>` for workflow steps |
| `Variable<T>(x)`, `variable.value = y`, or `BehaviorRelay(value: x)`, `relay.accept(y)` | `CurrentValueSubject<T, Never>(x)`, `subject.send(y)` |
| `ReplaySubject<T>.create(bufferSize: 1)` | `CurrentValueSubject<T?, Never>(nil)`, read with `.compactMap { $0 }` |
| `subject.onNext(x)` | `subject.send(x)` |
| `.asObservable().distinctUntilChanged { ... }` | `.removeDuplicates { ... }.eraseToAnyPublisher()` |
| `.subscribe(onNext: { ... })` | `.sink { ... }` |
| `.disposeOnDeactivate(interactor:)` | `.cancelOnDeactivate(interactor:)` |
| `.disposeOnStop(_:)` | `.cancelOnStop(_:)` |
| `.disposeWith(workflow:)` | `.cancelWith(workflow:)` |
| `Observable.just(x)` in a workflow step | `Just(x).setFailureType(to: Error.self).eraseToAnyPublisher()` |
| `subject.map { ... }` as the result of a workflow step | `subject.map { ... }.setFailureType(to: Error.self).eraseToAnyPublisher()` |
| `step.asObservable()` | `step.asPublisher()` |
| `button.rx.tap.subscribe(onNext: { ... })` | `button.addAction(UIAction { _ in ... }, for: .touchUpInside)` |
| `import RxCocoa`, `DisposeBag()`, `.disposed(by: disposeBag)` | Not needed, the button keeps its action |

The wiki code was also written before Swift 5, which the tutorial projects now use, and it could rely on `import RIBs` to bring in Foundation and UIKit. Where it does not compile:

- Add `import Foundation`, or `import UIKit`, to a file that uses one of their types, such as `URL`.
- Remove `weak` from a property requirement of a protocol, such as `weak var router: RootRouting? { get set }`.
- Use the current UIKit names, which Xcode offers as a fix: `UITextField.BorderStyle` for `UITextBorderStyle`, `UIAlertAction.Style` for `UIAlertActionStyle`, and `UIApplication.LaunchOptionsKey` for `UIApplicationLaunchOptionsKey`.

To read about the backstory on why we created RIBs, see [this blog post](https://www.uber.com/blog/new-rider-app-architecture/) we wrote when releasing RIBs in production the first time and see [this short video](https://www.youtube.com/watch?v=Q5cTT0M0YXg) where we discussed how the RIBs architecture works.

#### What is the difference between RIBs and MV*/VIPER?

MVC, MVP, MVI, MVVM and VIPER are architecture patterns. RIBs is a framework. What differentiates RIBs from frameworks based on MV*/VIPER is:

- **Business logic drives the app, not the view tree**. Unlike with MV*/VIPER, a RIB does not have to have a view. This means that the app hierarchy is driven by the business logic, not the view tree.
- **Independent business logic and view trees**. RIBs decouple how the business logic scopes are structured from view hierarchies. This allows the application to have a deep business logic tree, isolating business logic nodes, while maintaining a shallow view hierarchy making layouts, animations and transitions easy.

There are some other novel things about RIBs. However, these could also be implemented with other MV*/VIPER frameworks. These are:
- **Cross-platform approach**, allowing iOS and Android architecture to stay in sync.
- **Tooling for easier adoption** on larger apps or teams. Tooling we are open sourcing includes IDE plugins for code generation and static code analysis.
- **Strong opinions about how state should be communicated**, using DI and Combine. Each RIB defines its dependencies and what dependencies it needs from its parent. Parent components that fulfill a child’s parent dependencies are provided to child Builders as a constructor dependency to allow for hierarchical DI scoping. This means that information is communicated via these dependencies up and down the tree.

## Usage

1. Clone this repository
2. Integrate using your preferred installation mechanism

For usage of the tooling built around RIBs, please see the [Tooling section](https://github.com/uber/RIBs/wiki#rib-tooling) in our documentation.

## Installation for iOS
#### Swift Package Manager

To integrate RIBs into your project, add `https://github.com/JustinLeeRepo/RIBs-iOS.git` as a package dependency in Xcode (File > Add Package Dependencies...) and set the dependency rule to the `combine` branch, or add the following to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/JustinLeeRepo/RIBs-iOS.git", branch: "combine"),
],
targets: [
    .target(name: "MyApp", dependencies: [.product(name: "RIBs", package: "RIBs-iOS")]),
]
```

Depend on the `combine` branch, or on one of its commits, and not on a version: the version tags in this repository come from the upstream project and point at its RxSwift code.

RIBs requires iOS 15 or later.

## Related projects

If you like RIBs, check out other related open source projects from our team:
- [Needle](https://github.com/uber/needle): a compile-time safe Swift dependency injection framework.
- [Motif](https://github.com/uber/motif): An abstract on top of Dagger offering simpler APIs for nested scopes.
- [Swift Concurrency](https://github.com/uber/swift-concurrency): a set of concurrency utility classes used by Uber, inspired by the equivalent [java.util.concurrent](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/package-summary.html) package classes.
- [Swift Abstract Class](https://github.com/uber/swift-abstract-class): a light-weight library along with an executable that enables compile-time safe abstract class development for Swift projects.
- [Swift Common](https://github.com/uber/swift-common): common libraries used by this set of Swift open source projects.

## License

    Copyright (C) 2017 Uber Technologies

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
