#include "proto_server.h"

#include <stdio.h>
#include <string.h>

#include "proto_framing.h"
#include "server_limits.h"

static bool send_packet(int socket_fd,
                        const uint8_t *packet_body,
                        size_t packet_length,
                        proto_send_callback_t send_fn,
                        void *send_context)
{
    uint8_t framed[SERVER_MAX_PACKET_SIZE + 8];
    size_t framed_length = 0;

    if (!proto_wrap_packet(packet_body, packet_length, framed, sizeof(framed), &framed_length))
    {
        return false;
    }

    return send_fn(send_context, socket_fd, framed, framed_length);
}

static bool send_status_response(int socket_fd,
                                 const proto_server_info_t *server,
                                 proto_send_callback_t send_fn,
                                 void *send_context)
{
    char json[320];
    int written = snprintf(json,
                           sizeof(json),
                           "{\"version\":{\"name\":\"1.16.5\",\"protocol\":%u},"
                           "\"players\":{\"max\":%u,\"online\":%u},"
                           "\"description\":{\"text\":\"%s\"}}",
                           (unsigned int)server->protocol_version,
                           (unsigned int)server->max_players,
                           (unsigned int)server->online_players,
                           server->motd);
    if (written <= 0 || written >= (int)sizeof(json))
    {
        return false;
    }

    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x00))
    {
        return false;
    }
    if (!proto_write_string(&writer, json))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_pong(int socket_fd,
                      int64_t payload,
                      proto_send_callback_t send_fn,
                      void *send_context)
{
    uint8_t packet[16];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x01))
    {
        return false;
    }
    if (!proto_write_i64_be(&writer, payload))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_login_disconnect(int socket_fd,
                                  const char *message,
                                  proto_send_callback_t send_fn,
                                  void *send_context)
{
    char json[192];
    int written = snprintf(json, sizeof(json), "{\"text\":\"%s\"}", message);
    if (written <= 0 || written >= (int)sizeof(json))
    {
        return false;
    }

    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x00))
    {
        return false;
    }
    if (!proto_write_string(&writer, json))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

void proto_connection_reset(proto_connection_t *connection)
{
    memset(connection, 0, sizeof(*connection));
    connection->state = PROTO_STATE_HANDSHAKE;
}

static void handle_handshake(proto_connection_t *connection, proto_reader_t *reader)
{
    int32_t protocol_version = 0;
    int32_t next_state = 0;
    char server_address[256];
    uint16_t server_port = 0;

    if (!proto_read_varint(reader, &protocol_version) ||
        !proto_read_string(reader, server_address, sizeof(server_address)) ||
        !proto_read_u16_be(reader, &server_port) ||
        !proto_read_varint(reader, &next_state))
    {
        connection->close_requested = true;
        return;
    }

    (void)protocol_version;
    (void)server_address;
    (void)server_port;

    if (next_state == 1)
    {
        connection->state = PROTO_STATE_STATUS;
    }
    else if (next_state == 2)
    {
        connection->state = PROTO_STATE_LOGIN;
    }
    else
    {
        connection->close_requested = true;
    }
}

static void handle_status(proto_connection_t *connection,
                          int32_t packet_id,
                          proto_reader_t *reader,
                          int socket_fd,
                          const proto_server_info_t *server,
                          proto_send_callback_t send_fn,
                          void *send_context)
{
    if (packet_id == 0x00)
    {
        if (!send_status_response(socket_fd, server, send_fn, send_context))
        {
            connection->close_requested = true;
        }
        return;
    }

    if (packet_id == 0x01)
    {
        int64_t payload = 0;
        if (!proto_read_i64_be(reader, &payload))
        {
            connection->close_requested = true;
            return;
        }

        if (!send_pong(socket_fd, payload, send_fn, send_context))
        {
            connection->close_requested = true;
            return;
        }

        connection->close_requested = true;
        return;
    }

    connection->close_requested = true;
}

static void handle_login(proto_connection_t *connection,
                         int32_t packet_id,
                         proto_reader_t *reader,
                         int socket_fd,
                         proto_send_callback_t send_fn,
                         void *send_context)
{
    if (packet_id != 0x00)
    {
        connection->close_requested = true;
        return;
    }

    if (!proto_read_string(reader, connection->username, sizeof(connection->username)))
    {
        connection->close_requested = true;
        return;
    }

    if (!send_login_disconnect(socket_fd,
                               "Login pipeline in progress. Status ping and protocol core are online.",
                               send_fn,
                               send_context))
    {
        connection->close_requested = true;
        return;
    }

    connection->close_requested = true;
}

void proto_handle_packet(proto_connection_t *connection,
                         const uint8_t *packet,
                         size_t packet_length,
                         int socket_fd,
                         const proto_server_info_t *server,
                         proto_send_callback_t send_fn,
                         void *send_context)
{
    proto_reader_t reader;
    proto_reader_init(&reader, packet, packet_length);

    int32_t packet_id = 0;
    if (!proto_read_varint(&reader, &packet_id))
    {
        connection->close_requested = true;
        return;
    }

    switch (connection->state)
    {
    case PROTO_STATE_HANDSHAKE:
        if (packet_id != 0x00)
        {
            connection->close_requested = true;
            return;
        }
        handle_handshake(connection, &reader);
        break;

    case PROTO_STATE_STATUS:
        handle_status(connection, packet_id, &reader, socket_fd, server, send_fn, send_context);
        break;

    case PROTO_STATE_LOGIN:
        handle_login(connection, packet_id, &reader, socket_fd, send_fn, send_context);
        break;

    case PROTO_STATE_PLAY:
    default:
        connection->close_requested = true;
        break;
    }
}
