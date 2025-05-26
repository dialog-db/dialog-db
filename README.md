# Dialog DB

![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue?label=License)
[![Tests](https://img.shields.io/github/checks-status/dialog-db/dialog-db/main)](https://github.com/dialog-db/dialog-db/actions/workflows/run_test_suite.yaml?query=branch%3Amain)

<div align="center">
  <picture>
    <img width="70%" alt="Dialog DB" src="./notes/images/dialog.webp">
  </picture>
</div>

> The world is divided into word-processing, spreadsheet, database and other applications. Computers are hierarchical. We have a desktop and hierarchical files which have to mean everything. 
>
> Right now you are a prisoner of each application you use. You have only the options that were given you by the developer of that application.
>
> All of this is phoney. It is a system of conventions that have been established and we can have better conventions.
>
> \- Ted Nelson, _[Visionary lays into the web]_

**Dialog** is an embeddable database designed for local-first software.

It has (or aims to have) the following properties:

- Schema-on-read via an expressive [Datalog]-esque query API
- Efficient synchronization across replicas
- Support for both [Web Assembly] and native runtime environments
- Emphasis on data privacy and user-centered authority

## Status: Experimental

**Dialog** is in active development and should be considered experimental in nature.

Expect fundamental details to break over time (binary encoding, index construction etc.). At this stage, we cannot offer any promise of a path to migrate old versions of the database forward as breaking changes are landed.

## Project Layout

- **[`./rust`](/rust)**: the core implementation of Dialog.
- **[`./typescript`](/typescript)**: packages for using Dialog in TypeScript and/or React.
- **[`./adr`](/adr)**: architectural design records
- **[`./notes`](/notes)**: informal notes about Dialog or adjacent topics

## Getting Started

This project encodes a comprehensive development environment via [Nix flakes]. If you have Nix installed, you can drop into a shell with all the tools you need to build and modify Dialog using the following command:

```sh
nix develop
```

From a suitable development environment, you can build using standard Rust and Node.js tooling. For example: `cargo test` will run the full Rust test suite for your native target.

## License

This project is dual licensed under [MIT] and [Apache-2.0].

[MIT]: https://www.opensource.org/licenses/mit  
[Apache-2.0]: https://www.apache.org/licenses/license-2.0
[Datalog]: https://en.wikipedia.org/wiki/Datalog
[Web Assembly]: https://webassembly.org/
[Visionary lays into the web]: http://news.bbc.co.uk/2/hi/science/nature/1581891.stm
[Nix flakes]: https://nixos.wiki/wiki/flakes