// swift-tools-version:5.1
import PackageDescription

let package = Package(
    name: "RIBs",
    platforms: [
        .iOS("15.0"),
    ],
    products: [
        .library(name: "RIBs", targets: ["RIBs"]),
    ],
    targets: [
        .target(
            name: "RIBs",
            path: "RIBs"
        ),
        .testTarget(
            name: "RIBsTests",
            dependencies: ["RIBs"],
            path: "RIBsTests",
            exclude: [
                // These rely on an `expectAssertionFailure` helper that does not exist in this repository, and were
                // never part of the test target.
                "ComponentizedBuilderTests.swift",
                "MultiStageComponentizedBuilderTests.swift",
            ]
        ),
    ]
)
