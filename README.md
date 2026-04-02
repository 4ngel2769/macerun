# Macerun

A bare-metal Minecraft 1.16.5 server written in C for the ESP32-S3 microcontroller.
This is a proof-of-concept for running a functional Minecraft server on highly constrained hardware.

## Why?
Because I got some inspiration from PortalRunner's **[bareiron](https://github.com/p2r3/bareiron)** and wanted to see if I could run a Minecraft server on an esp32.
Running a Minecraft server on an x86 server with plenty of RAM is not very fun, running it on a microcontroller with barely any resources; now that's fun.

##### Keep in mind if you want to use this as your next survival server, this won't cut it.

## Hardware Requirements

* ESP32-S3 with 16MB flash and octal SPIRAM - `esp32s3n16r8`

## Technical Architecture

Macerun implements a highly constrained subset of the Minecraft Java protocol (version 754). It operates entirely on the microcontroller, managing raw sockets and protocol buffers without standard server wrappers.

* **Networking:** Uses raw lwIP TCP sockets over FreeRTOS. It manages asynchronous packet framing to handle chunk distribution, entity tracking, and keepalives for up to 4 concurrent players.
* **World Generation:** Chunks are procedurally generated on-the-fly to minimize heap allocation. The engine uses 2D biome generation combined with bilinear interpolated heightmaps to construct sections dynamically before sending them out over the network.
* **State Persistence:** Player modifications (block deltas) and inventories are tracked in memory and committed directly to the ESP32's Non-Volatile Storage (NVS) partition.
* **Mechanics:** Includes minimal implementations of core game loops: player physics verification, block breaking/placement, minimal 2x2 inventory crafting, and chat command interception.

## Building

Requires ESP-IDF v6.0.0.

```sh
idf.py set-target esp32s3
idf.py build
idf.py flash monitor
```

## Todo

- Player Persistence: Saving player data upon disconnecting (coordinates, health, hunger, and inventory items).
- Advanced Crafting: 3x3 crafting grid support.
- Containers: Chests and block inventory management.
- Entities: Mobs, spawning, and mob AI.
- Respawning: Handling player death and the respawn.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

