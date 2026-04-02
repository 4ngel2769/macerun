#include "net_server.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "lwip/inet.h"
#include "lwip/sockets.h"
#include "lwip/tcp.h"

#include "proto_framing.h"
#include "proto_profile.h"
#include "proto_server.h"
#include "server_limits.h"

#define NET_ATTACK_COOLDOWN_MS 500ULL

typedef struct
{
    bool in_use;
    int socket_fd;
    uint8_t stream_buffer[SERVER_RX_STREAM_BUFFER];
    size_t stream_length;
    proto_connection_t protocol;
} net_client_t;

typedef struct
{
    bool running;
    int listen_fd;
    uint64_t uptime_ms;
    net_server_config_t config;
    net_client_t clients[SERVER_MAX_PLAYERS];
    TaskHandle_t task;
} net_server_state_t;

static const char *TAG = "net_server";
static net_server_state_t s_server;

static bool username_equals_ignore_case(const char *left, const char *right);

static int count_online_players(const net_server_state_t *server)
{
    int count = 0;
    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        if (server->clients[i].in_use &&
            server->clients[i].protocol.state == PROTO_STATE_PLAY &&
            server->clients[i].protocol.joined_play)
        {
            count++;
        }
    }
    return count;
}

static bool set_non_blocking(int socket_fd)
{
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags < 0)
    {
        return false;
    }

    if (fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        return false;
    }

    int one = 1;
    setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    int send_buffer = 32 * 1024;
    if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer)) < 0)
    {
        ESP_LOGW(TAG, "failed to set SO_SNDBUF: fd=%d errno=%d", socket_fd, errno);
    }

    int recv_buffer = 8 * 1024;
    if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &recv_buffer, sizeof(recv_buffer)) < 0)
    {
        ESP_LOGW(TAG, "failed to set SO_RCVBUF: fd=%d errno=%d", socket_fd, errno);
    }

    return true;
}

static bool socket_send_all(void *context, int socket_fd, const uint8_t *data, size_t length)
{
    (void)context;

    size_t sent = 0;
    uint32_t stalled_retries = 0;
    uint32_t max_stalled_retries = SERVER_SOCKET_SEND_STALL_RETRIES * 5;
    TickType_t stall_started_ticks = 0;

    if (length > 2048)
    {
        uint32_t scaled_retries = (uint32_t)(length / 4);
        if (scaled_retries > max_stalled_retries)
        {
            max_stalled_retries = scaled_retries;
        }
        if (max_stalled_retries > 8000)
        {
            max_stalled_retries = 8000;
        }
    }

    while (sent < length)
    {
        ssize_t result = send(socket_fd, data + sent, length - sent, 0);
        if (result > 0)
        {
            sent += (size_t)result;
            stalled_retries = 0;
            stall_started_ticks = 0;
            continue;
        }

        if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            if (stall_started_ticks == 0)
            {
                stall_started_ticks = xTaskGetTickCount();
            }

            stalled_retries++;
            uint32_t stalled_ms = (uint32_t)((xTaskGetTickCount() - stall_started_ticks) * portTICK_PERIOD_MS);
            if (stalled_retries > max_stalled_retries ||
                stalled_ms >= SERVER_SOCKET_SEND_STALL_TIMEOUT_MS)
            {
                ESP_LOGW(TAG,
                         "send stalled: fd=%d sent=%u/%u retries=%u stalled_ms=%u",
                         socket_fd,
                         (unsigned int)sent,
                         (unsigned int)length,
                         (unsigned int)stalled_retries,
                         (unsigned int)stalled_ms);
                return false;
            }

            fd_set write_fds;
            FD_ZERO(&write_fds);
            FD_SET(socket_fd, &write_fds);

            struct timeval wait_timeout = {
                .tv_sec = 0,
                .tv_usec = (int)(SERVER_SOCKET_SEND_WAIT_SLICE_MS * 1000),
            };

            int ready = select(socket_fd + 1, NULL, &write_fds, NULL, &wait_timeout);
            if (ready > 0)
            {
                continue;
            }
            if (ready == 0)
            {
                continue;
            }
            if (errno == EINTR)
            {
                continue;
            }

            ESP_LOGW(TAG,
                     "send wait failed: fd=%d sent=%u/%u errno=%d",
                     socket_fd,
                     (unsigned int)sent,
                     (unsigned int)length,
                     errno);
            return false;
        }

        if (result < 0 && errno == EINTR)
        {
            continue;
        }

        if (result < 0)
        {
            ESP_LOGW(TAG,
                     "send failed: fd=%d sent=%u/%u errno=%d",
                     socket_fd,
                     (unsigned int)sent,
                     (unsigned int)length,
                     errno);
        }
        else
        {
            ESP_LOGW(TAG,
                     "send returned zero: fd=%d sent=%u/%u",
                     socket_fd,
                     (unsigned int)sent,
                     (unsigned int)length);
        }

        return false;
    }

    return true;
}

static void close_client_with_reason(net_server_state_t *server,
                                     net_client_t *client,
                                     const char *reason)
{
    if (!client->in_use)
    {
        return;
    }

    if (client->protocol.joined_play)
    {
        for (size_t k = 0; k < SERVER_MAX_PLAYERS; k++)
        {
            net_client_t *peer = &server->clients[k];
            if (peer == client)
            {
                continue;
            }
            if (!peer->in_use)
            {
                continue;
            }
            if (!peer->protocol.joined_play)
            {
                continue;
            }
            if (peer->protocol.state != PROTO_STATE_PLAY)
            {
                continue;
            }

            proto_send_player_remove(peer->socket_fd,
                                     &client->protocol,
                                     socket_send_all,
                                     server);
        }
    }

    const char *safe_reason = (reason != NULL) ? reason : "unspecified";
    if (client->protocol.username[0] != '\0')
    {
        ESP_LOGW(TAG, "client disconnected: user=%s fd=%d reason=%s",
                 client->protocol.username, client->socket_fd, safe_reason);
    }
    else
    {
        ESP_LOGW(TAG, "client disconnected: fd=%d reason=%s",
                 client->socket_fd, safe_reason);
    }

    close(client->socket_fd);
    memset(client, 0, sizeof(*client));

    if (count_online_players(server) == 0)
    {
        proto_server_save_world();
    }
}

static bool socket_broadcast_except(void *context,
                                    int source_socket_fd,
                                    const uint8_t *data,
                                    size_t length)
{
    net_server_state_t *server = (net_server_state_t *)context;
    bool all_sent = true;

    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        net_client_t *client = &server->clients[i];
        if (!client->in_use ||
            client->socket_fd == source_socket_fd ||
            client->protocol.state != PROTO_STATE_PLAY ||
            !client->protocol.joined_play)
        {
            continue;
        }

        if (!socket_send_all(server, client->socket_fd, data, length))
        {
            ESP_LOGW(TAG,
                     "broadcast delivery failed: fd=%d len=%u",
                     client->socket_fd,
                     (unsigned int)length);
            all_sent = false;
        }
    }

    return all_sent;
}

static void fill_server_info(const net_server_state_t *server, proto_server_info_t *server_info)
{
    server_info->protocol_version = server->config.protocol_version;
    server_info->max_players = server->config.max_players;
    server_info->online_players = (uint8_t)count_online_players(server);
    snprintf(server_info->motd, sizeof(server_info->motd), "%s", server->config.motd);
}

static net_client_t *find_client_by_entity_id(net_server_state_t *server, int32_t entity_id)
{
    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        net_client_t *client = &server->clients[i];
        if (!client->in_use ||
            !client->protocol.joined_play ||
            client->protocol.state != PROTO_STATE_PLAY)
        {
            continue;
        }

        if (client->protocol.entity_id == entity_id)
        {
            return client;
        }
    }

    return NULL;
}

static void broadcast_entity_animation(net_server_state_t *server,
                                       int32_t entity_id,
                                       uint8_t animation_id)
{
    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        net_client_t *observer = &server->clients[i];
        if (!observer->in_use ||
            !observer->protocol.joined_play ||
            observer->protocol.state != PROTO_STATE_PLAY)
        {
            continue;
        }

        if (!proto_send_entity_animation(observer->socket_fd,
                                         entity_id,
                                         animation_id,
                                         socket_send_all,
                                         server))
        {
            ESP_LOGW(TAG,
                     "entity animation delivery failed: fd=%d entity=%ld anim=%u",
                     observer->socket_fd,
                     (long)entity_id,
                     (unsigned int)animation_id);
        }
    }
}

static void process_pending_interaction_events(net_server_state_t *server,
                                               net_client_t *client,
                                               uint64_t now_ms)
{
    if (!client->in_use ||
        !client->protocol.joined_play ||
        client->protocol.state != PROTO_STATE_PLAY)
    {
        return;
    }

    if (client->protocol.pending_swing_animation)
    {
        if (now_ms >= client->protocol.next_swing_allowed_ms)
        {
            broadcast_entity_animation(server, client->protocol.entity_id, 0);
            client->protocol.pending_swing_animation = false;
            client->protocol.next_swing_allowed_ms = now_ms + 125ULL;
        }
    }

    if (!client->protocol.pending_attack_event)
    {
        return;
    }

    int32_t target_entity_id = client->protocol.pending_attack_target_entity_id;
    client->protocol.pending_attack_event = false;
    client->protocol.pending_attack_target_entity_id = 0;

    if (target_entity_id == client->protocol.entity_id)
    {
        return;
    }

    if (now_ms < client->protocol.next_attack_allowed_ms)
    {
        return;
    }

    client->protocol.next_attack_allowed_ms = now_ms + NET_ATTACK_COOLDOWN_MS;

    net_client_t *target = find_client_by_entity_id(server, target_entity_id);
    if (target == NULL)
    {
        return;
    }

    if (target->protocol.health > 1.0f)
    {
        target->protocol.health -= 1.0f;
        if (target->protocol.health < 1.0f)
        {
            target->protocol.health = 1.0f;
        }
    }

    if (!proto_send_health_update(target->socket_fd,
                                  &target->protocol,
                                  socket_send_all,
                                  server))
    {
        ESP_LOGW(TAG,
                 "health update delivery failed: target_fd=%d target_entity=%ld",
                 target->socket_fd,
                 (long)target->protocol.entity_id);
        target->protocol.close_requested = true;
    }
}

static void accept_new_clients(net_server_state_t *server)
{
    while (true)
    {
        struct sockaddr_in6 client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server->listen_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }
            ESP_LOGW(TAG, "accept failed: errno=%d", errno);
            return;
        }

        if (!set_non_blocking(client_fd))
        {
            ESP_LOGW(TAG, "failed to set client socket non-blocking");
            close(client_fd);
            continue;
        }

        bool assigned = false;
        for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
        {
            if (!server->clients[i].in_use)
            {
                server->clients[i].in_use = true;
                server->clients[i].socket_fd = client_fd;
                server->clients[i].stream_length = 0;
                proto_connection_reset(&server->clients[i].protocol);
                assigned = true;
                ESP_LOGW(TAG, "client connected: slot=%u fd=%d", (unsigned int)i, client_fd);
                break;
            }
        }

        if (!assigned)
        {
            ESP_LOGW(TAG, "connection rejected: full (%u players)", SERVER_MAX_PLAYERS);
            close(client_fd);
        }
    }
}

static bool send_system_chat_to_client(net_server_state_t *server,
                                       int socket_fd,
                                       const char *message)
{
    if (message == NULL || message[0] == '\0')
    {
        return true;
    }

    uint8_t framed_packet[SERVER_MAX_INBOUND_PACKET_SIZE + 8];
    size_t framed_packet_length = 0;
    if (!proto_build_chat_packet(message,
                                 0,
                                 0,
                                 framed_packet,
                                 sizeof(framed_packet),
                                 &framed_packet_length))
    {
        return false;
    }

    return socket_send_all(server, socket_fd, framed_packet, framed_packet_length);
}

static bool try_handle_player_chat_command(net_server_state_t *server,
                                           net_client_t *issuer,
                                           const uint8_t *packet,
                                           size_t packet_length,
                                           uint64_t now_ms)
{
    if (!issuer->in_use ||
        issuer->protocol.state != PROTO_STATE_PLAY ||
        !issuer->protocol.joined_play)
    {
        return false;
    }

    proto_reader_t reader;
    proto_reader_init(&reader, packet, packet_length);

    int32_t packet_id = 0;
    if (!proto_read_varint(&reader, &packet_id))
    {
        return false;
    }

    const proto_profile_t *profile = proto_profile_default();
    if (packet_id != profile->c2s_play_chat)
    {
        return false;
    }

    char chat_message[SERVER_MAX_CHAT_MESSAGE_LENGTH + 1];
    if (!proto_read_string(&reader, chat_message, sizeof(chat_message)))
    {
        return false;
    }

    const char *args = NULL;
    if (strncmp(chat_message, "/give ", 6) == 0)
    {
        args = chat_message + 6;
    }
    else if (strncmp(chat_message, "give ", 5) == 0)
    {
        args = chat_message + 5;
    }
    else
    {
        return false;
    }

    while (*args == ' ' || *args == '\t')
    {
        args++;
    }

    issuer->protocol.last_activity_ms = now_ms;

    if (*args == '\0')
    {
        if (!send_system_chat_to_client(server,
                                        issuer->socket_fd,
                                        "Usage: /give <target|@a|@s> <item_name> <amount>"))
        {
            issuer->protocol.close_requested = true;
        }
        return true;
    }

    char args_copy[SERVER_MAX_CHAT_MESSAGE_LENGTH + 1];
    snprintf(args_copy, sizeof(args_copy), "%s", args);

    char *save_ptr = NULL;
    char *target = strtok_r(args_copy, " \t", &save_ptr);
    char *item_name = strtok_r(NULL, " \t", &save_ptr);
    char *amount_text = strtok_r(NULL, " \t", &save_ptr);
    char *extra = strtok_r(NULL, " \t", &save_ptr);

    if (target == NULL || item_name == NULL || amount_text == NULL || extra != NULL)
    {
        if (!send_system_chat_to_client(server,
                                        issuer->socket_fd,
                                        "Usage: /give <target|@a|@s> <item_name> <amount>"))
        {
            issuer->protocol.close_requested = true;
        }
        return true;
    }

    char *amount_end = NULL;
    long parsed_amount = strtol(amount_text, &amount_end, 10);
    if (amount_end == amount_text || *amount_end != '\0' || parsed_amount <= 0 || parsed_amount > 65535)
    {
        if (!send_system_chat_to_client(server,
                                        issuer->socket_fd,
                                        "Invalid amount. Expected 1..65535"))
        {
            issuer->protocol.close_requested = true;
        }
        return true;
    }

    const char *resolved_target = target;
    if (username_equals_ignore_case(target, "@s"))
    {
        resolved_target = issuer->protocol.username;
    }

    uint16_t players_affected = 0;
    uint32_t items_granted = 0;
    esp_err_t err = net_server_give_item(resolved_target,
                                         item_name,
                                         (uint16_t)parsed_amount,
                                         &players_affected,
                                         &items_granted);

    char feedback[160];
    if (err == ESP_OK)
    {
        snprintf(feedback,
                 sizeof(feedback),
                 "Given %u x %s to %u player(s)",
                 (unsigned int)parsed_amount,
                 item_name,
                 (unsigned int)players_affected);
    }
    else if (err == ESP_ERR_INVALID_ARG)
    {
        snprintf(feedback,
                 sizeof(feedback),
                 "Unknown item or invalid args. Usage: /give <target|@a|@s> <item_name> <amount>");
    }
    else if (err == ESP_ERR_NOT_FOUND)
    {
        snprintf(feedback,
                 sizeof(feedback),
                 "No matching online player for target '%s'",
                 resolved_target);
    }
    else if (err == ESP_ERR_NO_MEM)
    {
        snprintf(feedback,
                 sizeof(feedback),
                 "Partial give: granted %lu item(s) to %u player(s)",
                 (unsigned long)items_granted,
                 (unsigned int)players_affected);
    }
    else
    {
        snprintf(feedback,
                 sizeof(feedback),
                 "Give failed: %s",
                 esp_err_to_name(err));
    }

    if (!send_system_chat_to_client(server, issuer->socket_fd, feedback))
    {
        issuer->protocol.close_requested = true;
    }

    return true;
}

static void process_stream_packets(net_server_state_t *server,
                                   net_client_t *client,
                                   uint64_t now_ms)
{
    uint8_t packet[SERVER_MAX_INBOUND_PACKET_SIZE];
    size_t packet_length = 0;

    while (client->in_use)
    {
        proto_extract_result_t result = proto_extract_packet(client->stream_buffer,
                                                             &client->stream_length,
                                                             packet,
                                                             sizeof(packet),
                                                             &packet_length);

        if (result == PROTO_EXTRACT_NONE)
        {
            return;
        }

        if (result == PROTO_EXTRACT_ERROR)
        {
            ESP_LOGW(TAG, "invalid packet framing, disconnecting client");
            close_client_with_reason(server, client, "invalid packet framing");
            return;
        }

        if (try_handle_player_chat_command(server, client, packet, packet_length, now_ms))
        {
            if (client->protocol.close_requested)
            {
                close_client_with_reason(server, client, "protocol requested close");
                return;
            }

            continue;
        }

        proto_server_info_t server_info;
        fill_server_info(server, &server_info);

        bool was_joined_play = client->protocol.joined_play;

        proto_handle_packet(&client->protocol,
                            packet,
                            packet_length,
                            client->socket_fd,
                            &server_info,
                            socket_send_all,
                            socket_broadcast_except,
                            server,
                            now_ms);

        if (!was_joined_play && client->protocol.joined_play)
        {
            ESP_LOGW(TAG,
                     "player joined: user=%s fd=%d",
                     client->protocol.username[0] != '\0' ? client->protocol.username : "(unknown)",
                     client->socket_fd);

            for (size_t j = 0; j < SERVER_MAX_PLAYERS; j++)
            {
                net_client_t *peer = &server->clients[j];

                if (peer == client)
                {
                    continue;
                }
                if (!peer->in_use)
                {
                    continue;
                }
                if (!peer->protocol.joined_play)
                {
                    continue;
                }
                if (peer->protocol.state != PROTO_STATE_PLAY)
                {
                    continue;
                }

                if (!proto_send_player_presence(client->socket_fd,
                                                &peer->protocol,
                                                socket_send_all,
                                                server))
                {
                    client->protocol.close_requested = true;
                }

                if (!proto_send_player_presence(peer->socket_fd,
                                                &client->protocol,
                                                socket_send_all,
                                                server))
                {
                    peer->protocol.close_requested = true;
                }
            }

            client->protocol.prev_pos_x = client->protocol.pos_x;
            client->protocol.prev_pos_y = client->protocol.pos_y;
            client->protocol.prev_pos_z = client->protocol.pos_z;
        }

        process_pending_interaction_events(server, client, now_ms);

        if (client->protocol.close_requested)
        {
            close_client_with_reason(server, client, "protocol requested close");
            return;
        }
    }
}

static void process_client_rx(net_server_state_t *server,
                              net_client_t *client,
                              uint64_t now_ms)
{
    uint8_t rx_tmp[512];

    while (client->in_use)
    {
        ssize_t received = recv(client->socket_fd, rx_tmp, sizeof(rx_tmp), 0);
        if (received > 0)
        {
            size_t incoming = (size_t)received;
            if (client->stream_length + incoming > sizeof(client->stream_buffer))
            {
                ESP_LOGW(TAG, "stream buffer overflow; disconnecting client");
                close_client_with_reason(server, client, "stream buffer overflow");
                return;
            }

            memcpy(client->stream_buffer + client->stream_length, rx_tmp, incoming);
            client->stream_length += incoming;
            process_stream_packets(server, client, now_ms);
            continue;
        }

        if (received == 0)
        {
            close_client_with_reason(server, client, "peer closed connection");
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }

        ESP_LOGW(TAG, "recv failed, disconnecting client: errno=%d", errno);
        close_client_with_reason(server, client, "recv error");
        return;
    }
}

static void server_task(void *arg)
{
    net_server_state_t *server = (net_server_state_t *)arg;

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(server->config.bind_port),
        .sin_addr = {.s_addr = htonl(INADDR_ANY)},
    };

    server->listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if (server->listen_fd < 0)
    {
        ESP_LOGE(TAG, "failed to create socket");
        server->running = false;
        vTaskDelete(NULL);
        return;
    }

    int reuse = 1;
    setsockopt(server->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    if (bind(server->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        ESP_LOGE(TAG, "bind failed errno=%d", errno);
        close(server->listen_fd);
        server->listen_fd = -1;
        server->running = false;
        vTaskDelete(NULL);
        return;
    }

    if (listen(server->listen_fd, SERVER_MAX_PLAYERS) < 0)
    {
        ESP_LOGE(TAG, "listen failed errno=%d", errno);
        close(server->listen_fd);
        server->listen_fd = -1;
        server->running = false;
        vTaskDelete(NULL);
        return;
    }

    if (!set_non_blocking(server->listen_fd))
    {
        ESP_LOGE(TAG, "failed to set listen socket non-blocking");
        close(server->listen_fd);
        server->listen_fd = -1;
        server->running = false;
        vTaskDelete(NULL);
        return;
    }

    ESP_LOGI(TAG, "listening on port %u", (unsigned int)server->config.bind_port);

    while (server->running)
    {
        uint64_t now_ms = server->uptime_ms;

        accept_new_clients(server);

        for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
        {
            if (server->clients[i].in_use)
            {
                process_client_rx(server, &server->clients[i], now_ms);
            }
        }

        proto_server_info_t server_info;
        fill_server_info(server, &server_info);
        for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
        {
            if (server->clients[i].in_use)
            {
                proto_tick_connection(&server->clients[i].protocol,
                                      server->clients[i].socket_fd,
                                      &server_info,
                                      socket_send_all,
                                      server,
                                      now_ms);

                if (server->clients[i].protocol.close_requested)
                {
                    close_client_with_reason(server, &server->clients[i], "tick requested close");
                }
            }
        }

        for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
        {
            net_client_t *mover = &server->clients[i];
            if (!mover->in_use)
            {
                continue;
            }
            if (!mover->protocol.joined_play)
            {
                continue;
            }
            if (mover->protocol.state != PROTO_STATE_PLAY)
            {
                continue;
            }

            double dx = mover->protocol.pos_x - mover->protocol.prev_pos_x;
            double dy = mover->protocol.pos_y - mover->protocol.prev_pos_y;
            double dz = mover->protocol.pos_z - mover->protocol.prev_pos_z;
            bool moved = (dx * dx + dy * dy + dz * dz) > 1e-8;

            if (moved)
            {
                for (size_t j = 0; j < SERVER_MAX_PLAYERS; j++)
                {
                    net_client_t *observer = &server->clients[j];
                    if (observer == mover)
                    {
                        continue;
                    }
                    if (!observer->in_use)
                    {
                        continue;
                    }
                    if (!observer->protocol.joined_play)
                    {
                        continue;
                    }
                    if (observer->protocol.state != PROTO_STATE_PLAY)
                    {
                        continue;
                    }

                    if (!proto_send_entity_pos_rot(observer->socket_fd,
                                                   &mover->protocol,
                                                   socket_send_all,
                                                   server))
                    {
                        observer->protocol.close_requested = true;
                    }
                }
            }

            mover->protocol.prev_pos_x = mover->protocol.pos_x;
            mover->protocol.prev_pos_y = mover->protocol.pos_y;
            mover->protocol.prev_pos_z = mover->protocol.pos_z;
        }

        server->uptime_ms += SERVER_TICK_MS;

        proto_tick_server(now_ms);

        vTaskDelay(pdMS_TO_TICKS(SERVER_TICK_MS));
    }

    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        close_client_with_reason(server, &server->clients[i], "server stopping");
    }

    proto_server_save_world();

    if (server->listen_fd >= 0)
    {
        close(server->listen_fd);
        server->listen_fd = -1;
    }

    server->task = NULL;
    vTaskDelete(NULL);
}

bool net_server_is_running(void)
{
    return s_server.running;
}

uint8_t net_server_online_players(void)
{
    return (uint8_t)count_online_players(&s_server);
}

static bool username_equals_ignore_case(const char *left, const char *right)
{
    if (left == NULL || right == NULL)
    {
        return false;
    }

    while (*left != '\0' && *right != '\0')
    {
        char lhs = (char)tolower((unsigned char)*left);
        char rhs = (char)tolower((unsigned char)*right);
        if (lhs != rhs)
        {
            return false;
        }

        left++;
        right++;
    }

    return *left == '\0' && *right == '\0';
}

esp_err_t net_server_give_item(const char *target,
                               const char *item_name,
                               uint16_t amount,
                               uint16_t *players_affected_out,
                               uint32_t *items_granted_out)
{
    if (players_affected_out != NULL)
    {
        *players_affected_out = 0;
    }

    if (items_granted_out != NULL)
    {
        *items_granted_out = 0;
    }

    if (target == NULL || target[0] == '\0' || item_name == NULL || item_name[0] == '\0' || amount == 0)
    {
        return ESP_ERR_INVALID_ARG;
    }

    if (!s_server.running)
    {
        return ESP_ERR_INVALID_STATE;
    }

    uint16_t item_id = 0;
    if (!proto_resolve_item_name(item_name, &item_id))
    {
        return ESP_ERR_INVALID_ARG;
    }

    bool target_all_players = username_equals_ignore_case(target, "@a");
    uint16_t affected = 0;
    uint32_t total_granted = 0;
    bool all_complete = true;

    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        net_client_t *client = &s_server.clients[i];
        if (!client->in_use ||
            client->protocol.state != PROTO_STATE_PLAY ||
            !client->protocol.joined_play)
        {
            continue;
        }

        if (!target_all_players && !username_equals_ignore_case(client->protocol.username, target))
        {
            continue;
        }

        uint16_t granted_for_player = 0;
        bool complete = proto_give_item(&client->protocol,
                                        client->socket_fd,
                                        item_id,
                                        amount,
                                        socket_send_all,
                                        &s_server,
                                        &granted_for_player);

        affected++;
        total_granted += granted_for_player;

        if (!complete)
        {
            all_complete = false;
        }
    }

    if (players_affected_out != NULL)
    {
        *players_affected_out = affected;
    }

    if (items_granted_out != NULL)
    {
        *items_granted_out = total_granted;
    }

    if (affected == 0)
    {
        return ESP_ERR_NOT_FOUND;
    }

    if (total_granted == 0)
    {
        return ESP_ERR_NO_MEM;
    }

    return all_complete ? ESP_OK : ESP_ERR_NO_MEM;
}

esp_err_t net_server_broadcast_chat(const char *message)
{
    if (message == NULL || message[0] == '\0')
    {
        return ESP_ERR_INVALID_ARG;
    }

    if (!s_server.running)
    {
        return ESP_ERR_INVALID_STATE;
    }

    uint8_t framed_packet[SERVER_MAX_INBOUND_PACKET_SIZE + 8];
    size_t framed_packet_length = 0;
    if (!proto_build_chat_packet(message, 0, 0,
                                 framed_packet, sizeof(framed_packet),
                                 &framed_packet_length))
    {
        return ESP_FAIL;
    }

    bool any_client = false;
    bool delivered = false;

    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        net_client_t *client = &s_server.clients[i];
        if (!client->in_use ||
            client->protocol.state != PROTO_STATE_PLAY ||
            !client->protocol.joined_play)
        {
            continue;
        }

        any_client = true;
        if (socket_send_all(&s_server, client->socket_fd,
                            framed_packet, framed_packet_length))
        {
            delivered = true;
        }
        else
        {
            ESP_LOGW(TAG,
                     "chat broadcast delivery failed: fd=%d len=%u",
                     client->socket_fd,
                     (unsigned int)framed_packet_length);
        }
    }

    if (!any_client)
    {
        return ESP_ERR_NOT_FOUND;
    }

    return delivered ? ESP_OK : ESP_FAIL;
}

esp_err_t net_server_start(const net_server_config_t *config)
{
    if (config == NULL)
    {
        return ESP_ERR_INVALID_ARG;
    }

    if (s_server.running)
    {
        return ESP_ERR_INVALID_STATE;
    }

    memset(&s_server, 0, sizeof(s_server));
    s_server.listen_fd = -1;
    s_server.running = true;
    s_server.config = *config;

    BaseType_t ok = xTaskCreatePinnedToCore(server_task,
                                "net_server",
                                SERVER_NET_TASK_STACK,
                                &s_server,
                                SERVER_NET_TASK_PRIORITY,
                                &s_server.task,
                                1);
    if (ok != pdPASS)
    {
        memset(&s_server, 0, sizeof(s_server));
        s_server.listen_fd = -1;
        return ESP_ERR_NO_MEM;
    }

    return ESP_OK;
}

void net_server_stop(void)
{
    s_server.running = false;
}
