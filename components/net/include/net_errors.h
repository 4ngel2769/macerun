#pragma once

/* Classification of socket outcomes so every caller reacts the same way to a
 * client leaving, a protocol violation, or a resource problem. Portable C: no
 * ESP-IDF headers, so it can be compiled and tested off-target. */
typedef enum
{
    NET_RETRY = 0,          /* EAGAIN, EWOULDBLOCK, EINTR */
    NET_PEER_LEFT,          /* EOF, ECONNRESET, EPIPE, ENOTCONN, ... */
    NET_PROTOCOL_ERROR,     /* malformed/oversized packet, invalid state */
    NET_RESOURCE_PRESSURE,  /* ENOMEM, ENOBUFS */
    NET_INTERNAL_FAULT      /* anything else */
} net_socket_result_t;

net_socket_result_t net_classify_socket_error(int err);
