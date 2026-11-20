# Stability soak

Verifies the server survives sustained multi-player play.

## Setup

```sh
cd test/soak
npm install minecraft-protocol@1.68.0 --no-audit --no-fund
```

## Run

1. Flash the firmware and let it connect to Wi-Fi (note the IP; default below is 192.168.101.117).
2. In one terminal, capture serial:
   ```sh
   python ../.espressif/python_env/idf6.1_py3.14_env/Scripts/python.exe capture_serial.py last_run.log 1900
   ```
   (use your ESP-IDF python / pyserial, or any python with pyserial installed).
3. In another terminal, run the bots:
   ```sh
   SOAK_MS=1800000 node soak.mjs
   ```
4. Check the log:
   ```sh
   node check_log.mjs last_run.log
   ```

`check_log.mjs` fails on forbidden log patterns (`send stalled`, `chunk send failed`,
`recv failed`, watchdog/Guru Meditation/stack overflow), requires >= 2 `health:`
lines, and fails if `tick_max` > 100 ms.

## Gate

The stability sub-project passes only when the 30-minute soak reports PASS.
A 5-minute smoke is useful during development but does not replace the full soak.
