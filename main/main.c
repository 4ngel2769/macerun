/*
 * Macerun - A lightweight Minecraft 1.16.5 server implementation in C for the ESP32-S3.
 * Copyright (C) 2026 Angel Capra (@angeldev0)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sdkconfig.h"
#include "esp_err.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "freertos/task.h"
#include "lwip/ip4_addr.h"
#include "nvs_flash.h"

#include "net_server.h"
#include "server_limits.h"

#ifndef CONFIG_MACERUN_STA_SSID
#define CONFIG_MACERUN_STA_SSID ""
#endif

#ifndef CONFIG_MACERUN_STA_PASSWORD
#define CONFIG_MACERUN_STA_PASSWORD ""
#endif

static const char *TAG = "macerun";

static const char *PRIMARY_STA_SSID = CONFIG_MACERUN_STA_SSID;
static const char *PRIMARY_STA_PASSWORD = CONFIG_MACERUN_STA_PASSWORD;
static const char *FALLBACK_AP_SSID = "macerun-server";
static const char *FALLBACK_AP_PASSWORD = "macerun-server";

#define WIFI_CONNECTED_BIT BIT0
#define WIFI_FAIL_BIT BIT1
#define WIFI_STA_MAX_RETRY 5
#define WIFI_STA_CONNECT_TIMEOUT_MS 15000

#define SERIAL_CONSOLE_STACK (6 * 1024)
#define SERIAL_CONSOLE_PRIORITY 4
#define SERIAL_LINE_MAX 192

static EventGroupHandle_t s_wifi_event_group;
static int s_sta_retry_num;
static esp_netif_t *s_sta_netif;
static esp_netif_t *s_ap_netif;
static net_server_config_t s_server_config;

static void init_nvs(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    }
}

static void init_server_config(void)
{
    memset(&s_server_config, 0, sizeof(s_server_config));
    s_server_config.bind_port = SERVER_TCP_PORT;
    s_server_config.protocol_version = SERVER_PROTOCOL_VERSION;
    s_server_config.max_players = SERVER_MAX_PLAYERS;
    snprintf(s_server_config.motd,
             sizeof(s_server_config.motd),
             "%s",
             "macerun: esp32 server prototype");
}

static bool sta_credentials_configured(void)
{
    return PRIMARY_STA_SSID[0] != '\0';
}

static void log_endpoint_for_netif(const char *label, esp_netif_t *netif)
{
    if (netif == NULL)
    {
        return;
    }

    esp_netif_ip_info_t ip_info;
    if (esp_netif_get_ip_info(netif, &ip_info) == ESP_OK)
    {
        ESP_LOGW(TAG,
                 "%s endpoint: " IPSTR ":%u",
                 label,
                 IP2STR(&ip_info.ip),
                 (unsigned int)s_server_config.bind_port);
    }
}

static void log_active_server_endpoint(void)
{
    wifi_mode_t mode = WIFI_MODE_NULL;
    if (esp_wifi_get_mode(&mode) != ESP_OK)
    {
        ESP_LOGW(TAG,
                 "Minecraft server port: %u (IP unavailable)",
                 (unsigned int)s_server_config.bind_port);
        return;
    }

    if (mode == WIFI_MODE_STA)
    {
        log_endpoint_for_netif("STA", s_sta_netif);
        return;
    }

    if (mode == WIFI_MODE_AP)
    {
        log_endpoint_for_netif("AP", s_ap_netif);
        return;
    }

    log_endpoint_for_netif("STA", s_sta_netif);
    log_endpoint_for_netif("AP", s_ap_netif);
}

static bool start_mc_server(void)
{
    if (net_server_is_running())
    {
        ESP_LOGW(TAG, "Minecraft server is already running");
        log_active_server_endpoint();
        return true;
    }

    esp_err_t err = net_server_start(&s_server_config);
    if (err != ESP_OK)
    {
        ESP_LOGE(TAG, "Failed to start server: %s", esp_err_to_name(err));
        return false;
    }

    ESP_LOGW(TAG, "Minecraft server started on port %u", (unsigned int)s_server_config.bind_port);
    log_active_server_endpoint();
    return true;
}

static void stop_mc_server(void)
{
    if (!net_server_is_running())
    {
        ESP_LOGW(TAG, "Minecraft server is already stopped");
        return;
    }

    net_server_stop();
    ESP_LOGW(TAG, "Minecraft server stop requested");
}

static void init_wifi_stack(void)
{
    ESP_ERROR_CHECK(esp_netif_init());

    esp_err_t event_ret = esp_event_loop_create_default();
    if (event_ret != ESP_OK && event_ret != ESP_ERR_INVALID_STATE)
    {
        ESP_ERROR_CHECK(event_ret);
    }

    wifi_init_config_t wifi_init_cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&wifi_init_cfg));
}

static void wifi_event_handler(void *arg,
                               esp_event_base_t event_base,
                               int32_t event_id,
                               void *event_data)
{
    (void)arg;

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START)
    {
        esp_err_t connect_ret = esp_wifi_connect();
        if (connect_ret != ESP_OK)
        {
            ESP_LOGW(TAG, "Initial STA connect failed: %s", esp_err_to_name(connect_ret));
        }
        return;
    }

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED)
    {
        if (s_sta_retry_num < WIFI_STA_MAX_RETRY)
        {
            s_sta_retry_num++;
            ESP_LOGW(TAG,
                     "STA disconnected, retry %d/%d",
                     s_sta_retry_num,
                     WIFI_STA_MAX_RETRY);
            esp_err_t connect_ret = esp_wifi_connect();
            if (connect_ret != ESP_OK)
            {
                ESP_LOGW(TAG, "STA reconnect failed: %s", esp_err_to_name(connect_ret));
            }
        }
        else if (s_wifi_event_group != NULL)
        {
            xEventGroupSetBits(s_wifi_event_group, WIFI_FAIL_BIT);
        }
        return;
    }

    if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP)
    {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;

        s_sta_retry_num = 0;
        if (s_wifi_event_group != NULL)
        {
            xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
        }

        if (event != NULL)
        {
            ESP_LOGW(TAG,
                     "STA got IP: " IPSTR ":%u",
                     IP2STR(&event->ip_info.ip),
                     (unsigned int)s_server_config.bind_port);
        }
    }
}

static bool init_wifi_sta_with_timeout(void)
{
    if (!sta_credentials_configured())
    {
        ESP_LOGW(TAG, "STA SSID not configured; skipping STA and using AP fallback");
        return false;
    }

    s_sta_netif = esp_netif_create_default_wifi_sta();
    if (s_sta_netif == NULL)
    {
        ESP_LOGE(TAG, "Failed to create STA netif");
        return false;
    }

    s_wifi_event_group = xEventGroupCreate();
    if (s_wifi_event_group == NULL)
    {
        ESP_LOGE(TAG, "Failed to create Wi-Fi event group");
        return false;
    }

    esp_event_handler_instance_t wifi_instance = NULL;
    esp_event_handler_instance_t ip_instance = NULL;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT,
                                                        ESP_EVENT_ANY_ID,
                                                        &wifi_event_handler,
                                                        NULL,
                                                        &wifi_instance));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT,
                                                        IP_EVENT_STA_GOT_IP,
                                                        &wifi_event_handler,
                                                        NULL,
                                                        &ip_instance));

    wifi_config_t wifi_config = {0};
    snprintf((char *)wifi_config.sta.ssid, sizeof(wifi_config.sta.ssid), "%s", PRIMARY_STA_SSID);
    snprintf((char *)wifi_config.sta.password,
             sizeof(wifi_config.sta.password),
             "%s",
             PRIMARY_STA_PASSWORD);
    wifi_config.sta.threshold.authmode = WIFI_AUTH_WPA2_PSK;
    wifi_config.sta.pmf_cfg.capable = true;
    wifi_config.sta.pmf_cfg.required = false;

    s_sta_retry_num = 0;
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGW(TAG, "Trying STA Wi-Fi first (SSID=%s)", PRIMARY_STA_SSID);
    EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group,
                                           WIFI_CONNECTED_BIT | WIFI_FAIL_BIT,
                                           pdTRUE,
                                           pdFALSE,
                                           pdMS_TO_TICKS(WIFI_STA_CONNECT_TIMEOUT_MS));
    bool connected = (bits & WIFI_CONNECTED_BIT) != 0;

    ESP_ERROR_CHECK(esp_event_handler_instance_unregister(IP_EVENT, IP_EVENT_STA_GOT_IP, ip_instance));
    ESP_ERROR_CHECK(esp_event_handler_instance_unregister(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_instance));

    vEventGroupDelete(s_wifi_event_group);
    s_wifi_event_group = NULL;

    if (!connected)
    {
        esp_err_t stop_ret = esp_wifi_stop();
        if (stop_ret != ESP_OK && stop_ret != ESP_ERR_WIFI_NOT_STARTED)
        {
            ESP_ERROR_CHECK(stop_ret);
        }
    }

    return connected;
}

static bool init_wifi_softap(void)
{
    s_ap_netif = esp_netif_create_default_wifi_ap();
    if (s_ap_netif == NULL)
    {
        ESP_LOGE(TAG, "Failed to create AP netif");
        return false;
    }

    wifi_config_t wifi_config = {0};
    snprintf((char *)wifi_config.ap.ssid, sizeof(wifi_config.ap.ssid), "%s", FALLBACK_AP_SSID);
    snprintf((char *)wifi_config.ap.password,
             sizeof(wifi_config.ap.password),
             "%s",
             FALLBACK_AP_PASSWORD);
    wifi_config.ap.ssid_len = strlen(FALLBACK_AP_SSID);
    wifi_config.ap.channel = 1;
    wifi_config.ap.max_connection = SERVER_MAX_PLAYERS;
    wifi_config.ap.authmode = WIFI_AUTH_WPA2_PSK;

    if (FALLBACK_AP_PASSWORD[0] == '\0')
    {
        wifi_config.ap.authmode = WIFI_AUTH_OPEN;
    }

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGW(TAG, "Wi-Fi SoftAP fallback started (SSID=%s)", FALLBACK_AP_SSID);
    log_endpoint_for_netif("AP", s_ap_netif);
    return true;
}

static void trim_line(char *line)
{
    size_t length = strlen(line);
    while (length > 0)
    {
        char tail = line[length - 1];
        if (tail != '\n' && tail != '\r' && tail != ' ' && tail != '\t')
        {
            break;
        }

        line[length - 1] = '\0';
        length--;
    }

    size_t start = 0;
    while (line[start] == ' ' || line[start] == '\t')
    {
        start++;
    }

    if (start > 0)
    {
        memmove(line, line + start, strlen(line + start) + 1);
    }
}

static void print_shell_help(void)
{
    ESP_LOGW(TAG, "Console commands: help, status, start, stop, mc, mc <command>");
}

static void print_mc_help(void)
{
    ESP_LOGW(TAG, "MC root commands: help, status, list, start, stop, say <message>, /give <target|@a> <item_name> <amount>, exit");
}

static void print_server_status(void)
{
    ESP_LOGW(TAG,
             "Server status: %s (%u/%u players)",
             net_server_is_running() ? "running" : "stopped",
             (unsigned int)net_server_online_players(),
             (unsigned int)s_server_config.max_players);
    log_active_server_endpoint();
}

static void handle_mc_command(const char *command, bool *mc_root_mode)
{
    if (strcmp(command, "help") == 0)
    {
        print_mc_help();
        return;
    }

    if (strcmp(command, "status") == 0)
    {
        print_server_status();
        return;
    }

    if (strcmp(command, "list") == 0)
    {
        ESP_LOGW(TAG,
                 "Players online: %u/%u",
                 (unsigned int)net_server_online_players(),
                 (unsigned int)s_server_config.max_players);
        return;
    }

    if (strcmp(command, "start") == 0)
    {
        start_mc_server();
        return;
    }

    if (strcmp(command, "stop") == 0)
    {
        stop_mc_server();
        return;
    }

    if (strcmp(command, "exit") == 0)
    {
        *mc_root_mode = false;
        ESP_LOGW(TAG, "Exited MC root console");
        return;
    }

    if (strncmp(command, "say ", 4) == 0)
    {
        const char *message = command + 4;
        while (*message == ' ')
        {
            message++;
        }

        if (*message == '\0')
        {
            ESP_LOGW(TAG, "Usage: say <message>");
            return;
        }

        char prefixed[256];
        int written = snprintf(prefixed, sizeof(prefixed), "[CONSOLE] %s", message);
        if (written <= 0 || written >= (int)sizeof(prefixed))
        {
            ESP_LOGW(TAG, "Message too long");
            return;
        }

        esp_err_t err = net_server_broadcast_chat(prefixed);
        if (err == ESP_OK)
        {
            ESP_LOGW(TAG, "Broadcast sent");
        }
        else if (err == ESP_ERR_NOT_FOUND)
        {
            ESP_LOGW(TAG, "No online players to receive the message");
        }
        else if (err == ESP_ERR_INVALID_STATE)
        {
            ESP_LOGW(TAG, "Server is not running");
        }
        else
        {
            ESP_LOGW(TAG, "Broadcast failed: %s", esp_err_to_name(err));
        }
        return;
    }

    if (strncmp(command, "/give ", 6) == 0 || strncmp(command, "give ", 5) == 0)
    {
        const char *args = (command[0] == '/') ? (command + 6) : (command + 5);
        while (*args == ' ' || *args == '\t')
        {
            args++;
        }

        if (*args == '\0')
        {
            ESP_LOGW(TAG, "Usage: /give <target|@a> <item_name> <amount>");
            return;
        }

        char args_copy[SERIAL_LINE_MAX];
        snprintf(args_copy, sizeof(args_copy), "%s", args);

        char *save_ptr = NULL;
        char *target = strtok_r(args_copy, " \t", &save_ptr);
        char *item_name = strtok_r(NULL, " \t", &save_ptr);
        char *amount_text = strtok_r(NULL, " \t", &save_ptr);
        char *extra = strtok_r(NULL, " \t", &save_ptr);

        if (target == NULL || item_name == NULL || amount_text == NULL || extra != NULL)
        {
            ESP_LOGW(TAG, "Usage: /give <target|@a> <item_name> <amount>");
            return;
        }

        char *amount_end = NULL;
        long parsed_amount = strtol(amount_text, &amount_end, 10);
        if (amount_end == amount_text || *amount_end != '\0' || parsed_amount <= 0 || parsed_amount > 65535)
        {
            ESP_LOGW(TAG, "Invalid amount '%s'. Expected 1..65535", amount_text);
            return;
        }

        uint16_t players_affected = 0;
        uint32_t items_granted = 0;
        esp_err_t err = net_server_give_item(target,
                                             item_name,
                                             (uint16_t)parsed_amount,
                                             &players_affected,
                                             &items_granted);

        if (err == ESP_OK)
        {
            ESP_LOGW(TAG,
                     "Given %u x %s to %u player(s)",
                     (unsigned int)parsed_amount,
                     item_name,
                     (unsigned int)players_affected);
        }
        else if (err == ESP_ERR_INVALID_STATE)
        {
            ESP_LOGW(TAG, "Server is not running");
        }
        else if (err == ESP_ERR_INVALID_ARG)
        {
            ESP_LOGW(TAG, "Unknown item or invalid arguments. Usage: /give <target|@a> <item_name> <amount>");
        }
        else if (err == ESP_ERR_NOT_FOUND)
        {
            ESP_LOGW(TAG, "No matching online player found for target '%s'", target);
        }
        else if (err == ESP_ERR_NO_MEM)
        {
            ESP_LOGW(TAG,
                     "Partial give: granted %lu total item(s) to %u player(s); some inventories may be full",
                     (unsigned long)items_granted,
                     (unsigned int)players_affected);
        }
        else
        {
            ESP_LOGW(TAG, "Give failed: %s", esp_err_to_name(err));
        }

        return;
    }

    ESP_LOGW(TAG, "Unknown MC command: %s", command);
    print_mc_help();
}

static void handle_shell_command(const char *command, bool *mc_root_mode)
{
    if (strcmp(command, "help") == 0)
    {
        print_shell_help();
        return;
    }

    if (strcmp(command, "status") == 0)
    {
        print_server_status();
        return;
    }

    if (strcmp(command, "start") == 0)
    {
        start_mc_server();
        return;
    }

    if (strcmp(command, "stop") == 0)
    {
        stop_mc_server();
        return;
    }

    if (strcmp(command, "mc") == 0)
    {
        *mc_root_mode = true;
        ESP_LOGW(TAG, "Entered MC root console. Type 'help' for MC commands and 'exit' to leave.");
        return;
    }

    if (strncmp(command, "mc ", 3) == 0)
    {
        bool keep_mode = false;
        handle_mc_command(command + 3, &keep_mode);
        return;
    }

    ESP_LOGW(TAG, "Unknown command: %s", command);
    print_shell_help();
}

static void serial_console_task(void *arg)
{
    (void)arg;

    bool mc_root_mode = false;
    char line[SERIAL_LINE_MAX];

    ESP_LOGW(TAG, "Serial command console ready. Type 'help' to list commands.");

    while (true)
    {
        if (fgets(line, sizeof(line), stdin) == NULL)
        {
            vTaskDelay(pdMS_TO_TICKS(50));
            continue;
        }

        trim_line(line);
        if (line[0] == '\0')
        {
            continue;
        }

        if (mc_root_mode)
        {
            handle_mc_command(line, &mc_root_mode);
        }
        else
        {
            handle_shell_command(line, &mc_root_mode);
        }
    }
}

void app_main(void)
{
    init_nvs();
    init_server_config();
    init_wifi_stack();

    if (init_wifi_sta_with_timeout())
    {
        ESP_LOGW(TAG, "Connected to STA network (%s)", PRIMARY_STA_SSID);
    }
    else
    {
        ESP_LOGW(TAG, "STA connection failed; starting AP fallback");
        if (!init_wifi_softap())
        {
            ESP_LOGE(TAG, "AP fallback startup failed");
        }
    }

    start_mc_server();

    BaseType_t console_ok = xTaskCreate(serial_console_task,
                                        "serial_console",
                                        SERIAL_CONSOLE_STACK,
                                        NULL,
                                        SERIAL_CONSOLE_PRIORITY,
                                        NULL);
    if (console_ok != pdPASS)
    {
        ESP_LOGE(TAG, "Failed to start serial console task");
    }

    while (true)
    {
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}
