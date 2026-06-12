#include <stdio.h>
#include <string.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"
#include "driver/uart.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_http_server.h"
#include "mqtt_client.h"
#include "web.h"

#define WIFI_SSID "ELoad-AP"
#define WIFI_PASS "12345678"

// ---- STA（上网）配置 ----
#define STA_SSID "CD5653"
#define STA_PASS "12345678"

// ---- OneNET MQTT 配置 ----
#define ONENET_HOST "50359UYKlJ.mqtts.acc.cmcconenet.cn"
#define ONENET_PORT 1883
#define ONENET_PRODUCT "50359UYKlJ"
#define ONENET_DEVICE "Eload_Bycd"
#define ONENET_TOKEN "version=2018-10-31&res=products%2F50359UYKlJ%2Fdevices%2FEload_Bycd&et=1907466005&method=md5&sign=5vjPLzGnhgB3Wjpn4g4hpQ%3D%3D"
// 上报 topic
#define ONENET_TOPIC_PUB "$sys/" ONENET_PRODUCT "/" ONENET_DEVICE "/thing/property/post"
#define UART_NUM UART_NUM_1
#define UART_BAUD 921600
#define UART_TX_PIN 4
#define UART_RX_PIN 5
#define UART_BUF_SIZE 512

static const char *TAG = "eload";

// ---- STA 连接标志 ----
static EventGroupHandle_t s_wifi_eg;
#define WIFI_STA_CONNECTED_BIT BIT0

// ---- MQTT 客户端句柄 ----
static esp_mqtt_client_handle_t mqtt_client = NULL;
static bool mqtt_connected = false;
static void mqtt_init(void); // 前向声明

// ---- 广播队列：uart_rx_task 生产，ws_broadcast_task 消费 ----
#define BCAST_MSG_LEN 320
#define BCAST_QUEUE_LEN 4
typedef struct
{
    char buf[BCAST_MSG_LEN];
} bcast_msg_t;
static QueueHandle_t bcast_queue;

// ---- IV数据：heap分配，用信号量通知广播任务 ----
static char *iv_json_buf = NULL; // heap分配，广播后free
static int iv_json_len = 0;
static SemaphoreHandle_t iv_sem = NULL;

// ---- IV收集状态（uart_rx_task内使用）----
#define IV_MAX_POINTS 200
static uint16_t iv_ibuf[IV_MAX_POINTS];
static uint16_t iv_vbuf[IV_MAX_POINTS];
static int iv_n = 0;
static int iv_collecting = 0;

// ---- WebSocket 客户端列表 ----
#define MAX_WS_CLIENTS 3
static int ws_fds[MAX_WS_CLIENTS];
static httpd_handle_t server_handle = NULL;

static void ws_add_client(int fd)
{
    // 先检查是否已存在
    for (int i = 0; i < MAX_WS_CLIENTS; i++)
        if (ws_fds[i] == fd)
            return;
    // 找空位加入
    for (int i = 0; i < MAX_WS_CLIENTS; i++)
        if (ws_fds[i] == -1)
        {
            ws_fds[i] = fd;
            return;
        }
    // 没有空位，踢掉第一个（最老的）
    ws_fds[0] = fd;
}

static void ws_remove_client(int fd)
{
    for (int i = 0; i < MAX_WS_CLIENTS; i++)
        if (ws_fds[i] == fd)
        {
            ws_fds[i] = -1;
            return;
        }
}

// ---- WebSocket 处理器（httpd 任务内执行）----
static esp_err_t ws_handler(httpd_req_t *req)
{
    int fd = httpd_req_to_sockfd(req);

    if (req->method == HTTP_GET)
    {
        ws_add_client(fd);
        ESP_LOGI(TAG, "WS client connected fd=%d", fd);
        return ESP_OK;
    }

    // 先获取帧头，得到实际长度
    httpd_ws_frame_t frame = {0};
    esp_err_t ret = httpd_ws_recv_frame(req, &frame, 0);
    if (ret != ESP_OK || frame.len == 0)
    {
        if (frame.len == 0)
            return ESP_OK; // ping帧等，忽略
        ws_remove_client(fd);
        return ret;
    }

    // 按实际长度分配并接收
    uint8_t buf[128] = {0};
    if (frame.len >= sizeof(buf))
        frame.len = sizeof(buf) - 1;
    frame.payload = buf;
    ret = httpd_ws_recv_frame(req, &frame, frame.len);
    if (ret != ESP_OK)
    {
        ws_remove_client(fd);
        return ret;
    }
    buf[frame.len] = '\0';
    ESP_LOGI(TAG, "WS RX: %s", (char *)buf);

    // 解析命令，转发给 CH32
    // thread10 协议：命令字节 + 0xFF 0xFF 0xFF（无0x40前缀）
    uint8_t cmd[8];
    int cmd_len = 0;

    if (strstr((char *)buf, "\"out\""))
    {
        if (strstr((char *)buf, "\"1\""))
        {
            cmd[0] = 0x40;
            cmd[1] = 0x09;
            cmd[2] = 0xFF;
            cmd[3] = 0xFF;
            cmd[4] = 0xFF;
            cmd_len = 5;
        }
        else
        {
            cmd[0] = 0x40;
            cmd[1] = 0x0A;
            cmd[2] = 0xFF;
            cmd[3] = 0xFF;
            cmd[4] = 0xFF;
            cmd_len = 5;
        }
    }
    else if (strstr((char *)buf, "\"type\":\"bat\""))
    {
        cmd[0] = 0x40;
        cmd[1] = 0x0B;
        cmd[2] = 0xFF;
        cmd[3] = 0xFF;
        cmd[4] = 0xFF;
        cmd_len = 5;
    }
    else if (strstr((char *)buf, "\"type\":\"set\""))
    {
        // {"type":"set","mode":"CC","val":100}
        uint8_t mb = 0;
        if (strstr((char *)buf, "\"CC\""))
            mb = 0x10;
        else if (strstr((char *)buf, "\"CV\""))
            mb = 0x11;
        else if (strstr((char *)buf, "\"CR\""))
            mb = 0x12;
        else if (strstr((char *)buf, "\"CW\""))
            mb = 0x13;
        char *vp = strstr((char *)buf, "\"val\":");
        if (mb && vp)
        {
            int val = atoi(vp + 6);
            if (val < 0)
                val = 0;
            if (val > 99999)
                val = 99999;
            char digits[6];
            snprintf(digits, sizeof(digits), "%05d", val);
            ESP_LOGI(TAG, "SET: mb=0x%02X val=%d digits=%s", mb, val, digits);
            cmd[0] = 0x40;
            cmd[1] = 0x02;
            cmd[2] = mb;
            cmd[3] = (uint8_t)digits[0];
            cmd[4] = (uint8_t)digits[1];
            cmd[5] = (uint8_t)digits[2];
            cmd[6] = (uint8_t)digits[3];
            cmd[7] = (uint8_t)digits[4];
            uart_write_bytes(UART_NUM, (char *)cmd, 8);
            uint8_t tail[3] = {0xFF, 0xFF, 0xFF};
            uart_write_bytes(UART_NUM, (char *)tail, 3);
            cmd_len = 0;
        }
    }
    else if (strstr((char *)buf, "\"type\":\"mode\""))
    {
        // {"type":"mode","val":"CC"}
        uint8_t mb = 0;
        if (strstr((char *)buf, "\"CC\""))
            mb = 0x10;
        else if (strstr((char *)buf, "\"CV\""))
            mb = 0x11;
        else if (strstr((char *)buf, "\"CR\""))
            mb = 0x12;
        else if (strstr((char *)buf, "\"CW\""))
            mb = 0x13;
        if (mb)
        {
            cmd[0] = 0x40;
            cmd[1] = 0x01;
            cmd[2] = mb;
            cmd[3] = 0xFF;
            cmd[4] = 0xFF;
            cmd[5] = 0xFF;
            cmd_len = 6;
        }
    }

    if (cmd_len > 0)
        uart_write_bytes(UART_NUM, (char *)cmd, cmd_len);

    return ESP_OK;
}

// ---- HTTP 主页 ----
static esp_err_t http_root_handler(httpd_req_t *req)
{
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, HTML_PAGE, strlen(HTML_PAGE));
    return ESP_OK;
}

// ---- 广播任务：从队列取消息，逐个发给所有 WS 客户端 ----
static void ws_broadcast_task(void *arg)
{
    bcast_msg_t msg;
    while (1)
    {
        // 优先检查IV数据
        if (xSemaphoreTake(iv_sem, 0) == pdTRUE)
        {
            if (server_handle && iv_json_buf && iv_json_len > 0)
            {
                httpd_ws_frame_t frame = {
                    .type = HTTPD_WS_TYPE_TEXT,
                    .payload = (uint8_t *)iv_json_buf,
                    .len = (size_t)iv_json_len,
                };
                for (int i = 0; i < MAX_WS_CLIENTS; i++)
                {
                    if (ws_fds[i] == -1)
                        continue;
                    esp_err_t ret = httpd_ws_send_frame_async(server_handle, ws_fds[i], &frame);
                    if (ret != ESP_OK)
                    {
                        ESP_LOGW(TAG, "IV ws send failed fd=%d", ws_fds[i]);
                        ws_fds[i] = -1;
                    }
                }
                ESP_LOGI(TAG, "IV sent to WS len=%d", iv_json_len);
            }
            if (iv_json_buf)
            {
                free(iv_json_buf);
                iv_json_buf = NULL;
            }
            continue;
        }

        if (xQueueReceive(bcast_queue, &msg, pdMS_TO_TICKS(10)) != pdTRUE)
            continue;
        if (!server_handle)
            continue;

        httpd_ws_frame_t frame = {
            .type = HTTPD_WS_TYPE_TEXT,
            .payload = (uint8_t *)msg.buf,
            .len = strlen(msg.buf),
        };
        for (int i = 0; i < MAX_WS_CLIENTS; i++)
        {
            if (ws_fds[i] == -1)
                continue;
            esp_err_t ret = httpd_ws_send_frame_async(server_handle, ws_fds[i], &frame);
            if (ret != ESP_OK)
            {
                ESP_LOGW(TAG, "ws send failed fd=%d err=%d, removing", ws_fds[i], ret);
                ws_fds[i] = -1;
            }
        }
    }
}

// ---- 启动 HTTP + WebSocket 服务器 ----
static void start_webserver(void)
{
    for (int i = 0; i < MAX_WS_CLIENTS; i++)
        ws_fds[i] = -1;

    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = 80;

    if (httpd_start(&server_handle, &config) != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to start httpd");
        return;
    }

    httpd_uri_t root = {.uri = "/", .method = HTTP_GET, .handler = http_root_handler};
    httpd_register_uri_handler(server_handle, &root);

    httpd_uri_t ws = {
        .uri = "/ws",
        .method = HTTP_GET,
        .handler = ws_handler,
        .is_websocket = true,
    };
    httpd_register_uri_handler(server_handle, &ws);

    ESP_LOGI(TAG, "Web server started on port 80");
}

// ---- WiFi AP+STA 双模 ----
static void wifi_event_handler(void *arg, esp_event_base_t base,
                               int32_t id, void *data)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START)
    {
        esp_wifi_connect();
    }
    else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED)
    {
        mqtt_connected = false;
        vTaskDelay(pdMS_TO_TICKS(5000));
        esp_wifi_connect();
        ESP_LOGI(TAG, "STA reconnecting...");
    }
    else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP)
    {
        ip_event_got_ip_t *e = (ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "STA got IP: " IPSTR, IP2STR(&e->ip_info.ip));
        xEventGroupSetBits(s_wifi_eg, WIFI_STA_CONNECTED_BIT);
        // IP 拿到后直接启动 MQTT，无需等待
        if (!mqtt_client)
            mqtt_init();
    }
}

static void wifi_init_apsta(void)
{
    s_wifi_eg = xEventGroupCreate();
    esp_netif_init();
    esp_event_loop_create_default();
    esp_netif_create_default_wifi_ap();
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&cfg);

    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event_handler, NULL, NULL);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_event_handler, NULL, NULL);

    wifi_config_t ap_cfg = {
        .ap = {
            .ssid = WIFI_SSID,
            .ssid_len = strlen(WIFI_SSID),
            .password = WIFI_PASS,
            .max_connection = 4,
            .authmode = WIFI_AUTH_WPA2_PSK,
        },
    };
    wifi_config_t sta_cfg = {
        .sta = {
            .ssid = STA_SSID,
            .password = STA_PASS,
        },
    };

    esp_wifi_set_mode(WIFI_MODE_APSTA);
    esp_wifi_set_config(WIFI_IF_AP, &ap_cfg);
    esp_wifi_set_config(WIFI_IF_STA, &sta_cfg);
    esp_wifi_start();

    ESP_LOGI(TAG, "WiFi AP: SSID=%s  STA connecting to: %s", WIFI_SSID, STA_SSID);
}

// ---- MQTT 事件处理 ----
static void mqtt_event_handler(void *arg, esp_event_base_t base,
                               int32_t id, void *data)
{
    (void)data;
    switch (id)
    {
    case MQTT_EVENT_CONNECTED:
        mqtt_connected = true;
        ESP_LOGI(TAG, "MQTT connected to OneNET");
        break;
    case MQTT_EVENT_DISCONNECTED:
        mqtt_connected = false;
        ESP_LOGW(TAG, "MQTT disconnected");
        break;
    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "MQTT error");
        break;
    default:
        break;
    }
}

static void mqtt_init(void)
{
    // OneNET MQTT 用户名格式：产品ID|客户端ID|鉴权方式|过期时间
    // 密码直接使用设备密钥（base64）
    esp_mqtt_client_config_t cfg = {
        .broker.address.hostname = ONENET_HOST,
        .broker.address.port = ONENET_PORT,
        .broker.address.transport = MQTT_TRANSPORT_OVER_TCP,
        .credentials.client_id = ONENET_DEVICE,
        .credentials.username = ONENET_PRODUCT,
        .credentials.authentication.password = ONENET_TOKEN,
    };
    mqtt_client = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
    ESP_LOGI(TAG, "MQTT client started, broker=%s:%d", ONENET_HOST, ONENET_PORT);
}

// ---- MQTT 上报数据（仅在已连接时调用）----
static void mqtt_publish(const char *json)
{
    if (!mqtt_connected || !mqtt_client)
        return;
    esp_mqtt_client_publish(mqtt_client, ONENET_TOPIC_PUB, json, 0, 0, 0);
}

static void uart_init(void)
{
    uart_config_t cfg = {
        .baud_rate = UART_BAUD,
        .data_bits = UART_DATA_8_BITS,
        .parity = UART_PARITY_DISABLE,
        .stop_bits = UART_STOP_BITS_1,
        .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
    };
    uart_param_config(UART_NUM, &cfg);
    uart_set_pin(UART_NUM, UART_TX_PIN, UART_RX_PIN, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE);
    uart_driver_install(UART_NUM, UART_BUF_SIZE * 2, 0, 0, NULL, 0);
    ESP_LOGI(TAG, "UART%d init: baud=%d TX=GPIO%d RX=GPIO%d", UART_NUM, UART_BAUD, UART_TX_PIN, UART_RX_PIN);
}

// ---- UART 接收任务：读 CH32 JSON，入广播队列 ----
static void uart_rx_task(void *arg)
{
    uint8_t buf[280];
    int pos = 0;

    while (1)
    {
        uint8_t ch;
        int len = uart_read_bytes(UART_NUM, &ch, 1, pdMS_TO_TICKS(200));
        if (len <= 0)
            continue;

        if (ch == '\n')
        {
            buf[pos] = '\0';
            if (pos > 5 && buf[0] == '{')
            {
                // 识别IV分包，在本地拼接，不直接入队
                if (strstr((char *)buf, "\"type\":\"ivs\""))
                {
                    // 开始帧，重置收集状态
                    char *np = strstr((char *)buf, "\"n\":");
                    iv_n = np ? atoi(np + 4) : IV_MAX_POINTS;
                    if (iv_n > IV_MAX_POINTS)
                        iv_n = IV_MAX_POINTS;
                    iv_collecting = 1;
                    ESP_LOGI(TAG, "IV collect start n=%d", iv_n);
                }
                else if (iv_collecting && strstr((char *)buf, "\"type\":\"ivp\""))
                {
                    // 数据帧，解析idx/i/v存入缓冲区
                    char *idxp = strstr((char *)buf, "\"idx\":");
                    char *ip = strstr((char *)buf, "\"i\":");
                    char *vp = strstr((char *)buf, "\"v\":");
                    if (idxp && ip && vp)
                    {
                        int idx = atoi(idxp + 6);
                        if (idx >= 0 && idx < IV_MAX_POINTS)
                        {
                            iv_ibuf[idx] = (uint16_t)atoi(ip + 4);
                            iv_vbuf[idx] = (uint16_t)atoi(vp + 4);
                        }
                    }
                }
                else if (iv_collecting && strstr((char *)buf, "\"type\":\"ive\""))
                {
                    // 结束帧，heap分配拼完整JSON，信号量通知广播任务
                    iv_collecting = 0;
                    ESP_LOGI(TAG, "IV collect done n=%d", iv_n);
                    // 每个点最多 "[9999,9999]," = 12字节，头尾约30字节
                    int need = iv_n * 12 + 40;
                    char *jbuf = malloc(need);
                    if (jbuf)
                    {
                        int off = snprintf(jbuf, need, "{\"type\":\"iv\",\"points\":[");
                        for (int k = 0; k < iv_n; k++)
                        {
                            off += snprintf(jbuf + off, need - off,
                                            "%s[%d,%d]", k == 0 ? "" : ",", iv_ibuf[k], iv_vbuf[k]);
                        }
                        off += snprintf(jbuf + off, need - off, "]}");
                        iv_json_buf = jbuf;
                        iv_json_len = off;
                        xSemaphoreGive(iv_sem);
                        ESP_LOGI(TAG, "IV JSON ready len=%d", off);
                    }
                }
                else
                {
                    // 普通实时数据，直接入队 + 上报云端
                    bcast_msg_t msg;
                    memcpy(msg.buf, buf, pos + 1);
                    xQueueSend(bcast_queue, &msg, 0);
                    mqtt_publish((char *)buf);
                }
            }
            pos = 0;
        }
        else if (ch != '\r')
        {
            if (pos < (int)sizeof(buf) - 1)
                buf[pos++] = ch;
            else
                pos = 0;
        }
    }
}

void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        nvs_flash_erase();
        nvs_flash_init();
    }

    bcast_queue = xQueueCreate(BCAST_QUEUE_LEN, sizeof(bcast_msg_t));
    iv_sem = xSemaphoreCreateBinary();

    uart_init();
    wifi_init_apsta();
    start_webserver();

    xTaskCreate(uart_rx_task, "uart_rx", 4096, NULL, 5, NULL);
    xTaskCreate(ws_broadcast_task, "ws_bcast", 4096, NULL, 4, NULL);

    // MQTT 由 wifi_event_handler 在 STA 拿到 IP 时自动启动，无需在此等待
    ESP_LOGI(TAG, "Ready. AP='%s' http://192.168.4.1", WIFI_SSID);
}
