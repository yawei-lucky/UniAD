#!/usr/bin/env python3
"""Cloud/monitor-side receiver for 6-camera + LiDAR ZMQ stream."""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import List

import cv2
import msgpack
import numpy as np
import zmq


WINDOW_NAMES = [
    "cam_front", "cam_front_left", "cam_front_right",
    "cam_back", "cam_back_left", "cam_back_right",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Receive and display 6-camera + LiDAR stream")
    p.add_argument("--connect", required=True, help="PUB endpoint, e.g. tcp://100.x.x.x:5555")
    p.add_argument("--topic", default="sens", help="ZMQ topic to subscribe")
    p.add_argument("--rcvhwm", type=int, default=1, help="SUB receive high-water mark")
    p.add_argument("--save-dir", default=None, help="Optional directory to dump latest images + lidar")
    p.add_argument("--headless", action="store_true", help="Do not open OpenCV windows")
    p.add_argument("--latency-log", default=None, help="Optional CSV file to append latency metrics")
    p.add_argument("--print-every", type=int, default=30, help="Print rolling stats every N frames")
    p.add_argument("--max-frames", type=int, default=0, help="Stop after N received frames, 0 means run forever")
    return p.parse_args()


def decode_jpeg(blob: bytes):
    arr = np.frombuffer(blob, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise RuntimeError("Failed to decode JPEG frame")
    return img


def annotate(img, text: str):
    cv2.putText(img, text, (15, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)


def save_payload(base_dir: Path, images: List[np.ndarray], lidar_blob: bytes, header: dict):
    base_dir.mkdir(parents=True, exist_ok=True)
    stamp = int(header.get("ts_unix", time.time()) * 1000)
    frame_dir = base_dir / f"frame_{stamp}"
    frame_dir.mkdir(parents=True, exist_ok=True)
    for i, img in enumerate(images):
        cv2.imwrite(str(frame_dir / f"cam_{i}.jpg"), img)
    if lidar_blob:
        suffix = Path(header.get("lidar_name", "lidar.bin")).suffix or ".bin"
        (frame_dir / f"lidar{suffix}").write_bytes(lidar_blob)
    (frame_dir / "header.msgpack").write_bytes(msgpack.packb(header, use_bin_type=True))


def main() -> int:
    args = parse_args()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.RCVHWM, args.rcvhwm)
    sock.setsockopt(zmq.CONFLATE, 1)
    sock.setsockopt(zmq.SUBSCRIBE, args.topic.encode("utf-8"))
    sock.connect(args.connect)

    print(f"[receiver] connect={args.connect}, topic={args.topic}")
    csv_file = None
    csv_writer = None
    if args.latency_log:
        latency_path = Path(args.latency_log)
        latency_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file = latency_path.open("a", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        if latency_path.stat().st_size == 0:
            csv_writer.writerow(["recv_ts_unix", "sender_ts_unix", "frame_id", "latency_ms", "lidar_bytes", "label"])

    if not args.headless:
        for name in WINDOW_NAMES:
            cv2.namedWindow(name, cv2.WINDOW_NORMAL)

    recv_count = 0
    latency_window = []
    try:
        while True:
            parts = sock.recv_multipart()
            # expect: topic + header + 6 jpg + lidar blob = 9 parts
            if len(parts) < 9:
                print(f"[receiver][warn] invalid multipart len={len(parts)}")
                continue

            topic, header_b = parts[0], parts[1]
            if topic.decode("utf-8", errors="ignore") != args.topic:
                continue

            header = msgpack.unpackb(header_b, raw=False)
            cam_count = int(header.get("cam_count", 6))
            image_blobs = parts[2:2 + cam_count]
            lidar_blob = parts[2 + cam_count] if len(parts) > 2 + cam_count else b""

            if cam_count != 6 or len(image_blobs) != 6:
                print(f"[receiver][warn] expected 6 cams, got {cam_count}/{len(image_blobs)}")
                continue

            images = [decode_jpeg(x) for x in image_blobs]
            now = time.time()
            latency_ms = max((now - float(header.get("ts_unix", now))) * 1000.0, 0.0)
            recv_count += 1
            latency_window.append(latency_ms)
            info = (
                f"label={header.get('label', '')} | latency={latency_ms:.1f}ms | "
                f"lidar={len(lidar_blob)/1024:.1f}KB"
            )
            if csv_writer is not None:
                csv_writer.writerow([
                    now,
                    header.get("ts_unix", ""),
                    header.get("frame_id", ""),
                    f"{latency_ms:.3f}",
                    len(lidar_blob),
                    header.get("label", ""),
                ])
                csv_file.flush()

            if args.print_every > 0 and recv_count % args.print_every == 0:
                avg_ms = sum(latency_window) / len(latency_window)
                p95_ms = sorted(latency_window)[max(int(len(latency_window) * 0.95) - 1, 0)]
                print(
                    f"[receiver][stats] frames={recv_count} "
                    f"avg={avg_ms:.1f}ms p95={p95_ms:.1f}ms latest={latency_ms:.1f}ms"
                )
                latency_window = []

            if args.save_dir:
                save_payload(Path(args.save_dir), images, lidar_blob, header)

            if not args.headless:
                for i, img in enumerate(images):
                    annotate(img, info)
                    cv2.imshow(WINDOW_NAMES[i], img)
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord("q")):
                    break
            else:
                print(f"[receiver] {info}")
            if args.max_frames > 0 and recv_count >= args.max_frames:
                print(f"[receiver] reached max-frames={args.max_frames}, exiting")
                break
    except KeyboardInterrupt:
        print("\n[receiver] stopped")
    finally:
        sock.close(0)
        ctx.term()
        if csv_file is not None:
            csv_file.close()
        if not args.headless:
            cv2.destroyAllWindows()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
