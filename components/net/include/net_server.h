#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "esp_err.h"

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
esp_err_t net_server_broadcast_chat(const char *message);
