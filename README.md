# ESP32_Eload - 电子负载 ESP32 WiFi 数据推送模块

基于 ESP32 + ESP-IDF 开发，配合智能电源特性分析仪使用的 WiFi 数据中转模块，通过 UART 接收 CH32V307 推送的实时数据，经 WebSocket 广播到网页端，支持 IV 扫描曲线实时显示。

## 功能特性

- **WiFi AP 模式**：ESP32 作为热点（SSID: `ELoad-AP`，密码: `12345678`），无需路由器直连
- **UART 数据接收**：以 921600 波特率接收 CH32V307 推送的 JSON 数据
- **WebSocket 广播**：实时将数据广播给所有已连接的网页客户端（最多3个）
- **IV 扫描数据**：接收并解析 IV 扫描的电压/电流数组，组装成 JSON 推送到网页
- **内嵌网页**：网页 HTML/JS 内嵌在固件中（`web.h`），无需外部文件服务器

## 硬件连接

| ESP32 引脚 | 连接 |
|-----------|------|
| GPIO4 (TX) | CH32V307 UART5 RX |
| GPIO5 (RX) | CH32V307 UART5 TX |
| GND | 共地 |

## 软件架构

### 任务结构

| 任务 | 功能 |
|------|------|
| `uart_rx_task` | 接收 UART 数据，解析 JSON 和 IV 数据，放入广播队列 |
| `ws_broadcast_task` | 从队列取数据，广播给所有 WebSocket 客户端 |

### 数据流

```
CH32V307 → UART(921600) → ESP32 uart_rx_task → bcast_queue → ws_broadcast_task → WebSocket → 网页
```

### IV 扫描流程

1. CH32V307 发送 `IV_START` 标志
2. ESP32 开始收集最多 200 组电压/电流数据
3. 收集完成后组装 JSON 数组，通过 WebSocket 推送

## 开发环境

- 框架：ESP-IDF
- 目标芯片：ESP32
- UART：UART1，波特率 921600，TX=GPIO4，RX=GPIO5

## 编译烧录

```bash
idf.py build
idf.py flash
idf.py monitor
```

## 使用方法

1. 烧录固件到 ESP32
2. 手机或电脑连接 WiFi `ELoad-AP`，密码 `12345678`
3. 浏览器访问 `http://192.168.4.1`
4. 打开电子负载，数据实时显示在网页上
