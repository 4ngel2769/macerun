idf_component_register(SRCS "main.c"
                    INCLUDE_DIRS "."
                    REQUIRES net config esp_wifi esp_netif nvs_flash esp_event)
