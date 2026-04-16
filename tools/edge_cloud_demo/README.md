# 车端到云端的 6 路相机 + 1 路 LiDAR 传输 Demo

这个 Demo 针对你当前的需求：**输入固定为 6 张图像 + 1 个 LiDAR 点云（例如 pcd）**，通过 ZMQ 在 Tailscale 隧道中进行低延迟传输。

## 你计划里的关键点：哪些要改

相比你贴的版本，这里做了三处关键改进：

1. **从 pickle 单包改为 ZMQ multipart**
   - 避免大对象 pickle 的性能和安全问题。
   - 元数据（header）与图像/点云字节分离，更易扩展。

2. **显式支持 6 相机**
   - 发送端强制 6 路输入（文件或设备）。
   - 接收端强校验 cam_count=6，避免协议漂移。

3. **“最新帧优先”策略更完整**
   - PUB/SUB 均设置 `HWM=1` + `CONFLATE=1`，抖动时主动丢旧帧。

## 目录

- `sender.py`：车端发送（文件回放模式 / 实时 USB 相机模式）
- `receiver.py`：云端接收（显示模式 / 无头模式）

## 依赖

```bash
pip install pyzmq opencv-python msgpack numpy
```

## 运行方式

### 1) 文件回放模式（先用你当前的 6 张图片 + 1 个 pcd 快速验证）

```bash
python tools/edge_cloud_demo/sender.py \
  --bind tcp://*:5555 \
  --cam-files cam0.jpg cam1.jpg cam2.jpg cam3.jpg cam4.jpg cam5.jpg \
  --lidar-file sample.pcd \
  --fps 10
```

### 2) 实时 USB 相机模式（部署时更接近真实）

```bash
python tools/edge_cloud_demo/sender.py \
  --bind tcp://*:5555 \
  --cam-devices 0 1 2 3 4 5 \
  --lidar-file /data/lidar/latest.pcd \
  --lidar-loop \
  --fps 10
```

### 3) 云端接收

```bash
python tools/edge_cloud_demo/receiver.py \
  --connect tcp://100.x.x.x:5555
```

> `100.x.x.x` 替换成车端的 Tailscale IP。

## 关于你问的“部署时是文件还是数据流？”

真实部署时，你的理解是正确的：

- **车载 USB 摄像头输入给传输模块的是连续帧流**（不是离散 jpg 文件）。
- **LiDAR 输入通常也是连续点云流/UDP packet**（不是离散 pcd 文件）。
- `jpg/pcd` 文件通常只是**离线录包或调试介质**。

本 Demo 里保留 `--cam-files` / `--lidar-file` 是为了便于你现在快速联调；上线时建议切到 `--cam-devices` + 实际 LiDAR 驱动数据接口。

## 工程建议（从 Demo 到实车）

1. 协议层：从“单 topic”升级为“多 topic”（6 camera topic + lidar topic + 心跳 topic）。
2. 时钟同步：统一使用 `monotonic timestamp + frame_id` 做跨传感器对齐。
3. 序列化：继续使用 msgpack/flatbuffers，避免 pickle 在生产环境的风险。
4. 异常恢复：相机掉线自动重连；LiDAR 超时告警。
5. 带宽控制：在 5G 抖动时动态调整 JPEG 质量与分辨率。
