idf_component_register(
    SRCS "main.c"
    INCLUDE_DIRS "."
    REQUIRES mqtt driver esp_wifi esp_event esp_netif nvs_flash esp_http_server
)
