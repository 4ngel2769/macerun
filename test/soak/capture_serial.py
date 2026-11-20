"""Append ESP32 serial output (COM5, 115200) to a log file.

Usage: python capture_serial.py <outfile> [duration_seconds]
Keeps appending until interrupted (or duration elapses). Never toggles DTR/RTS.
"""
import sys
import time

import serial

out_path = sys.argv[1] if len(sys.argv) > 1 else "test/soak/last_run.log"
duration = float(sys.argv[2]) if len(sys.argv) > 2 else 3600.0

ser = serial.Serial("COM5", 115200, timeout=0.2)
start = time.time()
with open(out_path, "ab") as f:
    while time.time() - start < duration:
        data = ser.read(4096)
        if data:
            f.write(data)
            f.flush()
        else:
            time.sleep(0.05)
ser.close()
