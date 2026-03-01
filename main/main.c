#include <string.h>

#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "nvs_flash.h"

#include "net_server.h"
#include "server_limits.h"

static const char *TAG = "macerun";

static void init_nvs(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    }
}

static void init_wifi_softap(void)
{
    const char *ssid = "macerun-ap";
    const char *password = "macerun123";

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_ap();

    wifi_init_config_t wifi_init_cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&wifi_init_cfg));

    wifi_config_t wifi_config = {0};
    snprintf((char *)wifi_config.ap.ssid, sizeof(wifi_config.ap.ssid), "%s", ssid);
    snprintf((char *)wifi_config.ap.password, sizeof(wifi_config.ap.password), "%s", password);
    wifi_config.ap.ssid_len = strlen(ssid);
    wifi_config.ap.channel = 1;
    wifi_config.ap.max_connection = SERVER_MAX_PLAYERS;
    wifi_config.ap.authmode = WIFI_AUTH_WPA2_PSK;

    if (password[0] == '\0')
    {
        wifi_config.ap.authmode = WIFI_AUTH_OPEN;
    }

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "Wi-Fi SoftAP started (SSID=%s)", ssid);
}

void app_main(void)
{
    init_nvs();
    init_wifi_softap();

    net_server_config_t config = {
        .bind_port = SERVER_TCP_PORT,
        .protocol_version = SERVER_PROTOCOL_VERSION,
        .max_players = SERVER_MAX_PLAYERS,
    };
    snprintf(config.motd, sizeof(config.motd), "%s", "macerun: esp32 server prototype");

    ESP_ERROR_CHECK(net_server_start(&config));
    ESP_LOGI(TAG, "Server startup complete on port %u", (unsigned int)config.bind_port);

    while (true)
    {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}
