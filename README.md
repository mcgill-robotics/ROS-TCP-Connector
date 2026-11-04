# ROS TCP Connector (McGill Robotics fork)

Fork of Unity's [ROS-TCP-Connector](https://github.com/Unity-Technologies/ROS-TCP-Connector) 0.7.0, used by the McGill Robotics AUV simulator (Unity 6). The ROS side is our [endpoint fork](https://github.com/kaidalisohaib/ROS-TCP-Endpoint) running on ROS 2 Jazzy.

Upstream hasn't had a release since 2022. The main reason for this fork is that the bridge would get stuck if the sim or ROS wasn't shut down cleanly, or if they were started in a different order. There are also a few send-path optimizations from summer 2026.

The wire protocol is the same as upstream, so you can mix this connector with the upstream endpoint or the other way around. Dead-connection detection through keepalives only works when both sides are the fork.

## What is different from upstream

Performance changes (summer 2026):

- `TCP_NODELAY` is set on the socket, so small messages are not held back by Nagle's algorithm.
- `MessageSerializer.SendTo` copies the message into one reusable buffer and writes it with a single `Write()` call, instead of one call per field. Sys commands still use several small writes.
- The reader no longer polls `DataAvailable` with `Task.Delay(10 ms)` between reads. It just awaits `ReadAsync`.
- `SendServiceMessage` uses its own `MessageSerializer` instead of sharing the main thread's one.
- `TopicMessageSender`: when a topic's queue overflows, the oldest message is dropped and a warning with the topic name is logged every 30 drops. Upstream logged a generic "Queue full!" warning from the network thread for every drop.
- Visualizations: `Texture2D.Resize` replaced with `Reinitialize` in the occupancy grid visualizer (Unity 6 deprecation).

The summer commit also rented incoming buffers from `ArrayPool`. That was a bug and has been reverted (see below).

Connection fixes (`connection-robustness` branch):

- Incoming messages get exact-size buffers again. `ArrayPool.Rent` returns bigger arrays than asked for, and sys command JSON was decoded from the whole array, trailing bytes included. The arrays were never returned to the pool anyway.
- A 0-byte read is treated as the endpoint closing the connection. The summer change made the read loop spin at 100% CPU on `read += 0` in that case.
- Any read error stops the reader and the connection thread reconnects right away (upstream kept retrying on the dead socket).
- `Disconnect()` closes the socket right away. This unblocks a pending read, write or connect, which matters because Mono's `ReadAsync` ignores the cancellation token once the read is pending. Leaving play mode calls `OnApplicationQuit`, so the socket should be closed before the domain reload (not tested with a real domain reload yet). `OnDestroy` now disconnects too.
- Calling `Connect()` while already connected stops the old connection thread first. It used to start a second one.
- Writes time out after 10 s and connecting after 5 s. Both used to be able to block for minutes.
- Keepalives are timed with a `Stopwatch` instead of `Time.realtimeSinceStartup`. That value only advances in `Update()`, so no keepalives went out while the editor was paused.
- If no handshake arrives within 10 s of connecting, Unity reconnects. If the endpoint is the fork, it sends keepalives, so 10 s of silence also triggers a reconnect.
- Publisher registrations on reconnect go straight to the new socket, like the other registrations. Upstream queued them behind that topic's messages, so the endpoint rejected the first few messages.
- The "connection failed" warning is logged once per outage, not every second.

## Installing

In Unity, open `Window > Package Manager`, click `+`, pick `Add package from git URL...` and enter:

```
https://github.com/kaidalisohaib/ROS-TCP-Connector.git?path=/com.unity.robotics.ros-tcp-connector#main-ros2-optimized
```

For the visualizations package, use `?path=/com.unity.robotics.visualizations` with the same `#...` suffix.

Or edit `Packages/manifest.json` directly:

```json
"com.unity.robotics.ros-tcp-connector": "https://github.com/kaidalisohaib/ROS-TCP-Connector.git?path=/com.unity.robotics.ros-tcp-connector#main-ros2-optimized"
```

The part after `#` can be a branch, a tag or a full commit hash. Unity writes the resolved commit to `Packages/packages-lock.json`, so commit that file to make sure everyone gets the same version. For a fully reproducible build, put a commit hash after `#`. To pick up new commits on a branch, delete the package's entry from `packages-lock.json`.

Remember to set the protocol to ROS 2 under `Robotics > ROS Settings` (it adds the `ROS2` scripting define).

## Known limitations

- A `SendServiceMessage` call whose request or response is lost in a disconnect never completes. There is still no timeout on the Unity side. The fork's endpoint now always answers (with an empty response if the call failed), so this only happens when the connection drops mid-call.
- Messages queued while disconnected are thrown away on reconnect, same as upstream. Latched publishers re-send their last message.
- Dead-endpoint detection by silence only works with the fork's endpoint. With the upstream endpoint, Unity notices a dead connection when the socket closes or a write times out.
- The 10 s and 5 s timeouts are constants in `ROSConnection.cs` (`k_DeadConnectionTimeoutSeconds`, `k_ConnectTimeoutSeconds`), not inspector settings.
- Only tested in ROS 2 mode, with Unity 6000.0 and the endpoint fork on Jazzy and Humble. ROS 1 mode compiles but was not tested.

---

*Original upstream README below.*

# ROS TCP Connector

[![Version](https://img.shields.io/github/v/tag/Unity-Technologies/ROS-TCP-Connector)](https://github.com/Unity-Technologies/ROS-TCP-Connector/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE.md)
![ROS](https://img.shields.io/badge/ros-melodic,noetic-brightgreen)
![ROS](https://img.shields.io/badge/ros2-foxy,galactic-brightgreen)
![Unity](https://img.shields.io/badge/unity-2020.2+-brightgreen)

---

We're currently working on lots of things! Please take a short moment fill out our [survey](https://unitysoftware.co1.qualtrics.com/jfe/form/SV_0ojVkDVW0nNrHkW) to help us identify what products and packages to build next.

---

## Introduction

This repository contains two Unity packages: the ROS TCP Connector, for sending/receiving messages from ROS, and the Visualizations Package, for adding visualizations of incoming and outgoing messages in the Unity scene.

## Installation
1. Using Unity 2020.2 or later, open the Package Manager from `Window` -> `Package Manager`.
2. In the Package Manager window, find and click the + button in the upper lefthand corner of the window. Select `Add package from git URL....`

    ![image](https://user-images.githubusercontent.com/29758400/110989310-8ea36180-8326-11eb-8318-f67ee200a23d.png)

3. Enter the git URL for the desired package. Note: you can append a version tag to the end of the git url, like `#v0.4.0` or `#v0.5.0`, to declare a specific package version, or exclude the tag to get the latest from the package's `main` branch.
    1. For the ROS-TCP-Connector, enter `https://github.com/Unity-Technologies/ROS-TCP-Connector.git?path=/com.unity.robotics.ros-tcp-connector`.
    2. For Visualizations, enter `https://github.com/Unity-Technologies/ROS-TCP-Connector.git?path=/com.unity.robotics.visualizations`.
4. Click `Add`.

To install from a local clone of the repository, see [installing a local package](https://docs.unity3d.com/Manual/upm-ui-local.html) in the Unity manual.

## Tutorials
<!-- Scripts used to send [ROS](https://www.ros.org/) messages to an [TCP endpoint](https://github.com/Unity-Technologies/ROS_TCP_Endpoint) running as a ROS node. -->

This Unity package provides four main features:

- ROSConnection: A component that sets up communication between ROS and Unity. See the [Unity Robotics Hub](https://github.com/Unity-Technologies/Unity-Robotics-Hub/blob/main/tutorials/ros_unity_integration/README.md) for information and tutorials.

- [Message Generation](MessageGeneration.md): A tool to generate C# classes for ROS message types.

- Visualizations: A suite of default configurations and APIs to visualize incoming and outgoing information from ROS.
    - See the [Nav2 SLAM Example](https://github.com/Unity-Technologies/Robotics-Nav2-SLAM-Example) for tutorials on using the Visualizations Package!
    - You can also view the package's [Usage Information](com.unity.robotics.visualizations/Documentation~/README.md) for more details on using the package in your own project.

- [ROSGeometry](ROSGeometry.md): A set of extensions that convert geometries between Unity and other coordinate frames.

## ROS#

Special thanks to the Siemens [ROS# Project Team]( https://github.com/siemens/ros-sharp/wiki/Info_Acknowledgements) for driving the ROS-Unity Integration Open Source since 2017.

## Community and Feedback

The Unity Robotics projects are open-source and we encourage and welcome contributions.
If you wish to contribute, be sure to review our [contribution guidelines](CONTRIBUTING.md)
and [code of conduct](CODE_OF_CONDUCT.md).

## Support
For questions or discussions about Unity Robotics package installations or how to best set up and integrate your robotics projects, please create a new thread on the [Unity Robotics forum](https://forum.unity.com/forums/robotics.623/) and make sure to include as much detail as possible.

For feature requests, bugs, or other issues, please file a [GitHub issue](https://github.com/Unity-Technologies/ROS-TCP-Connector/issues) using the provided templates and the Robotics team will investigate as soon as possible.

For any other questions or feedback, connect directly with the
Robotics team at [unity-robotics@unity3d.com](mailto:unity-robotics@unity3d.com).

## License
[Apache License 2.0](LICENSE)