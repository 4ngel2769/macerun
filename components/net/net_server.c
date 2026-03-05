#include "net_server.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
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
#include "proto_server.h"
#include "server_limits.h"

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
    return true;
}

static bool socket_send_all(void *context, int socket_fd, const uint8_t *data, size_t length)
{
    (void)context;

    size_t sent = 0;
    while (sent < length)
    {
        ssize_t result = send(socket_fd, data + sent, length - sent, 0);
        if (result <= 0)
        {
            return false;
        }
        sent += (size_t)result;
    }

    return true;
}

static void close_client(net_client_t *client)
{
    if (!client->in_use)
    {
        return;
    }

    close(client->socket_fd);
    memset(client, 0, sizeof(*client));
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
            close_client(client);
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
                ESP_LOGI(TAG, "client connected in slot %u", (unsigned int)i);
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

static void process_stream_packets(net_server_state_t *server, net_client_t *client, uint64_t now_ms)
{
    uint8_t packet[SERVER_MAX_PACKET_SIZE];
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
            close_client(client);
            return;
        }

        proto_server_info_t server_info;
        fill_server_info(server, &server_info);

        proto_handle_packet(&client->protocol,
                            packet,
                            packet_length,
                            client->socket_fd,
                            &server_info,
                            socket_send_all,
                            socket_broadcast_except,
                            server,
                            now_ms);

        if (client->protocol.close_requested)
        {
            close_client(client);
            return;
        }
    }
}

static void process_client_rx(net_server_state_t *server, net_client_t *client, uint64_t now_ms)
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
                close_client(client);
                return;
            }

            memcpy(client->stream_buffer + client->stream_length, rx_tmp, incoming);
            client->stream_length += incoming;
            process_stream_packets(server, client, now_ms);
            continue;
        }

        if (received == 0)
        {
            close_client(client);
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }

        close_client(client);
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
                    close_client(&server->clients[i]);
                }
            }
        }

        server->uptime_ms += SERVER_TICK_MS;

        vTaskDelay(pdMS_TO_TICKS(SERVER_TICK_MS));
    }

    for (size_t i = 0; i < SERVER_MAX_PLAYERS; i++)
    {
        close_client(&server->clients[i]);
    }

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

    uint8_t framed_packet[SERVER_MAX_PACKET_SIZE + 8];
    size_t framed_packet_length = 0;
    if (!proto_build_chat_packet(message,
                                 0,
                                 0,
                                 framed_packet,
                                 sizeof(framed_packet),
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
        if (socket_send_all(&s_server, client->socket_fd, framed_packet, framed_packet_length))
        {
            delivered = true;
        }
        else
        {
            close_client(client);
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

    BaseType_t ok = xTaskCreate(server_task,
                                "net_server",
                                SERVER_NET_TASK_STACK,
                                &s_server,
                                SERVER_NET_TASK_PRIORITY,
                                &s_server.task);
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
