#!/usr/bin/env python3
"""Car-side sender: publish 6 camera frames + 1 LiDAR packet over ZMQ.

Supports two input modes:
1) file mode: read six JPG/PNG files + one PCD/NPY/BIN file in a loop (demo/replay)
2) live mode: read six USB cameras (/dev/video*) and optional LiDAR bytes from file

Wire format uses multipart ZMQ message:
- frame 0: topic bytes (default: b"sens")
- frame 1: msgpack header (metadata only)
- frame 2..7: JPEG bytes for camera frames
- frame 8: LiDAR bytes
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import cv2
import msgpack
import zmq


@dataclass
class CameraSource:
    cap: cv2.VideoCapture
    name: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Send 6-camera + LiDAR packets via ZMQ PUB")
    p.add_argument("--bind", default="tcp://*:5555", help="PUB bind endpoint")
    p.add_argument("--topic", default="sens", help="ZMQ topic")
    p.add_argument("--fps", type=float, default=10.0, help="Target send FPS")
    p.add_argument("--jpeg-quality", type=int, default=55, help="JPEG quality [1,100]")
    p.add_argument("--resize", default="640x384", help="Output image size WxH")
    p.add_argument("--sndhwm", type=int, default=1, help="PUB send high-water mark")

    p.add_argument("--cam-files", nargs="*", default=None, help="Exactly 6 image files for replay mode")
    p.add_argument("--cam-glob", default=None, help="Glob pattern containing >=6 images; first 6 are used")
    p.add_argument("--cam-devices", nargs="*", default=None, help="Exactly 6 camera device ids, e.g. 0 1 2 3 4 5")

    p.add_argument("--lidar-file", default=None, help="LiDAR file: .pcd/.bin/.npy bytes payload")
    p.add_argument("--lidar-loop", action="store_true", help="Reload LiDAR file each frame")
    p.add_argument("--label", default="demo", help="Vehicle id / stream label")
    p.add_argument("--max-frames", type=int, default=0, help="Stop after N frames, 0 means run forever")
    p.add_argument("--ack-connect", default=None, help="Optional ACK SUB endpoint, e.g. tcp://100.x.x.x:5556")
    p.add_argument("--ack-topic", default="ack", help="ACK topic when --ack-connect is enabled")
    p.add_argument("--rtt-log", default=None, help="Optional CSV path for RTT records on sender side")
    p.add_argument("--pending-ack-window", type=int, default=2048, help="Max in-flight frame IDs to track for RTT")
    return p.parse_args()


def parse_resize(resize: str) -> Tuple[int, int]:
    try:
        w_str, h_str = resize.lower().split("x")
        w, h = int(w_str), int(h_str)
    except Exception as exc:
        raise ValueError(f"Invalid --resize {resize}, expected like 640x384") from exc
    if w <= 0 or h <= 0:
        raise ValueError("Resize width and height must be positive")
    return w, h


def resolve_cam_files(args: argparse.Namespace) -> Optional[List[str]]:
    if args.cam_files:
        if len(args.cam_files) != 6:
            raise ValueError("--cam-files must provide exactly 6 paths")
        files = args.cam_files
    elif args.cam_glob:
        files = sorted(glob.glob(args.cam_glob))[:6]
        if len(files) < 6:
            raise ValueError("--cam-glob matched fewer than 6 images")
    else:
        return None

    for fp in files:
        if not Path(fp).is_file():
            raise FileNotFoundError(fp)
    return files


def resolve_cam_devices(args: argparse.Namespace) -> Optional[List[int]]:
    if not args.cam_devices:
        return None
    if len(args.cam_devices) != 6:
        raise ValueError("--cam-devices must provide exactly 6 device IDs")
    return [int(x) for x in args.cam_devices]


def open_cameras(device_ids: Sequence[int]) -> List[CameraSource]:
    cams: List[CameraSource] = []
    for dev in device_ids:
        cap = cv2.VideoCapture(dev)
        if not cap.isOpened():
            for c in cams:
                c.cap.release()
            raise RuntimeError(f"Failed to open camera device {dev}")
        cams.append(CameraSource(cap=cap, name=f"cam_{dev}"))
    return cams


def load_lidar_bytes(path: Optional[str]) -> bytes:
    if not path:
        return b""
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(path)
    return file_path.read_bytes()


def encode_jpeg(frame, wh: Tuple[int, int], quality: int) -> bytes:
    w, h = wh
    resized = cv2.resize(frame, (w, h), interpolation=cv2.INTER_AREA)
    ok, enc = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise RuntimeError("cv2.imencode failed")
    return enc.tobytes()


def get_frames_from_files(paths: Sequence[str]) -> List:
    frames = []
    for fp in paths:
        frame = cv2.imread(fp, cv2.IMREAD_COLOR)
        if frame is None:
            raise RuntimeError(f"Failed to read image: {fp}")
        frames.append(frame)
    return frames


def get_frames_from_cams(cams: Sequence[CameraSource]) -> List:
    frames = []
    for cam in cams:
        ok, frame = cam.cap.read()
        if not ok or frame is None:
            raise RuntimeError(f"Failed to read frame from {cam.name}")
        frames.append(frame)
    return frames


def main() -> int:
    args = parse_args()
    target_size = parse_resize(args.resize)
    cam_files = resolve_cam_files(args)
    cam_devices = resolve_cam_devices(args)

    if (cam_files is None) == (cam_devices is None):
        raise ValueError("Provide exactly one camera source: --cam-files/--cam-glob OR --cam-devices")

    lidar_payload = load_lidar_bytes(args.lidar_file)

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.SNDHWM, args.sndhwm)
    sock.setsockopt(zmq.CONFLATE, 1)
    sock.bind(args.bind)

    ack_sock = None
    ack_topic_bytes = args.ack_topic.encode("utf-8")
    if args.ack_connect:
        ack_sock = ctx.socket(zmq.SUB)
        ack_sock.setsockopt(zmq.RCVHWM, 1024)
        ack_sock.setsockopt(zmq.SUBSCRIBE, ack_topic_bytes)
        ack_sock.connect(args.ack_connect)

    rtt_csv_file = None
    rtt_csv_writer = None
    if args.rtt_log:
        rtt_path = Path(args.rtt_log)
        rtt_path.parent.mkdir(parents=True, exist_ok=True)
        rtt_csv_file = rtt_path.open("a", newline="", encoding="utf-8")
        rtt_csv_writer = csv.writer(rtt_csv_file)
        if rtt_path.stat().st_size == 0:
            rtt_csv_writer.writerow(
                ["ack_recv_ts_unix", "frame_id", "rtt_ms", "sender_ts_unix", "receiver_recv_ts_unix", "label"]
            )

    cams: List[CameraSource] = []
    if cam_devices is not None:
        cams = open_cameras(cam_devices)

    period_s = 1.0 / args.fps if args.fps > 0 else 0.0
    topic_bytes = args.topic.encode("utf-8")
    pending_ts = OrderedDict()

    print(f"[sender] bind={args.bind}, topic={args.topic}, fps={args.fps}, label={args.label}")
    frame_id = 0
    try:
        while True:
            t0 = time.time()
            if cam_files is not None:
                raw_frames = get_frames_from_files(cam_files)
            else:
                raw_frames = get_frames_from_cams(cams)

            jpg_list = [encode_jpeg(f, target_size, args.jpeg_quality) for f in raw_frames]
            if len(jpg_list) != 6:
                raise RuntimeError("Internal error: expected 6 camera frames")

            if args.lidar_loop and args.lidar_file:
                lidar_payload = load_lidar_bytes(args.lidar_file)

            now = time.time()
            header = {
                "ver": 1,
                "label": args.label,
                "ts_unix": now,
                "frame_id": frame_id,
                "cam_count": 6,
                "img_fmt": "jpg",
                "img_size": [target_size[0], target_size[1]],
                "lidar_len": len(lidar_payload),
                "lidar_name": os.path.basename(args.lidar_file) if args.lidar_file else "",
            }
            header_bytes = msgpack.packb(header, use_bin_type=True)

            multipart = [topic_bytes, header_bytes, *jpg_list, lidar_payload]
            sock.send_multipart(multipart, copy=False)
            pending_ts[frame_id] = now
            if len(pending_ts) > args.pending_ack_window:
                pending_ts.popitem(last=False)
            frame_id += 1
            if args.max_frames > 0 and frame_id >= args.max_frames:
                print(f"[sender] reached max-frames={args.max_frames}, exiting")
                break

            if ack_sock is not None:
                while True:
                    try:
                        ack_parts = ack_sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(ack_parts) < 2:
                        continue
                    ack_header = msgpack.unpackb(ack_parts[1], raw=False)
                    ack_frame_id = int(ack_header.get("frame_id", -1))
                    sender_ts = pending_ts.pop(ack_frame_id, None)
                    if sender_ts is None:
                        continue
                    ack_now = time.time()
                    rtt_ms = max((ack_now - sender_ts) * 1000.0, 0.0)
                    if rtt_csv_writer is not None:
                        rtt_csv_writer.writerow(
                            [
                                ack_now,
                                ack_frame_id,
                                f"{rtt_ms:.3f}",
                                sender_ts,
                                ack_header.get("receiver_recv_ts_unix", ""),
                                args.label,
                            ]
                        )
                        rtt_csv_file.flush()

            elapsed = time.time() - t0
            if period_s > 0 and elapsed < period_s:
                time.sleep(period_s - elapsed)
    except KeyboardInterrupt:
        print("\n[sender] stopped")
    finally:
        for c in cams:
            c.cap.release()
        if ack_sock is not None:
            ack_sock.close(0)
        sock.close(0)
        if rtt_csv_file is not None:
            rtt_csv_file.close()
        ctx.term()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
