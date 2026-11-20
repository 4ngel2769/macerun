#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "esp_err.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

typedef struct
{
    uint16_t bind_port;
    uint16_t protocol_version;
    uint8_t max_players;
    char motd[64];
} net_server_config_t;

esp_err_t net_server_start(const net_server_config_t *config);
void net_server_stop(void);
bool net_server_is_running(void);
uint8_t net_server_online_players(void);
/* Register the console task handle so health logs can report its stack HWM. */
void net_server_set_console_task(TaskHandle_t task_handle);
esp_err_t net_server_broadcast_chat(const char *message);
esp_err_t net_server_give_item(const char *target,
                               const char *item_name,
                               uint16_t amount,
                               uint16_t *players_affected_out,
                               uint32_t *items_granted_out);
