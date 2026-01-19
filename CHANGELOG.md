# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.1.1 - 2025-04-08

- Don't timeout when sending event to producer

## 1.1.0 - 2024-06-18

- Use new `Extreme.ListenerWithBackPressure`

## 1.0.1 - 2024-01-31

- Bump all dependencies including bumping `:extreme` to v1.0.5 which fixes a
  bug with live events sometimes getting dropped after subscribing to a stream

## 1.0.0 - 2022-05-11

This release represents stability in the API. There is no functional change
between this release and v0.4.2.

## 0.4.2 - 2022-01-26

### Fixed

- Sending an InOrderSubscription process a `:subscribe` message no longer
  creates a duplicate subscription. Instead a warning is emitted.

## 0.4.1 - 2021-08-12

### Changed

- Changed the InOrderSubscription internals to buffer events from the
  extreme subscription in a queue when there is no demand
    - this unlocks the ability for this producer to emit multiple events
      at once

## 0.4.0 - 2021-06-10

### Added

- Added `:subscribe_after` option which determines how long the producer will
  wait before checking whether it should subscribe or not

## 0.3.0 - 2021-06-04

### Changed

- Events are now produced in the shape of `{producer, event}` instead of simply
  `event`
    - this allows a consumer to use the name of the producer to inform decisions
      about how to store the stream position

## 0.2.2 - 2021-06-03

### Added

- Blanket configuration of the `:catch_up_chunk_size` can be set with the
  `:kelvin` application-env var of `:catch_up_chunk_size`, defaulting to 256

## 0.2.1 - 2021-06-03

### Fixed

- Increased timeout in call to subscibe in InOrder subscriber

### Added

- Added a configuration parameter for the InOrder subscription which controls
  the batch size of events read while catching up: `:catch_up_chunk_size`

## 0.2.0 - 2021-06-01

### Added

- Added noise to start-up-time checking of the auto-subscription option
    - This works well for awaiting the bring-up of a client to an external
      service such as LaunchDarkly

## 0.1.0 - 2021-06-01

### Added

- Initial implementation of the in-order subscription GenStage producer

## 0.0.0 - 2021-05-27

### Added
