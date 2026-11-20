#include "net_errors.h"

#include <errno.h>

net_packet_priority_t net_packet_priority(uint8_t packet_id)
{
    switch (packet_id)
    {
    case 0x20: /* chunk data */
    case 0x23: /* update light */
    case 0x1F: /* keepalive */
    case 0x02: /* login success */
    case 0x24: /* join game */
    case 0x39: /* respawn */
    case 0x34: /* player position/look */
        return NET_PACKET_HIGH;
    default:
        return NET_PACKET_LOW;
    }
}

uint64_t net_ticks_to_ms(uint32_t ticks, uint32_t tick_period_ms)
{
    return (uint64_t)ticks * (uint64_t)tick_period_ms;
}

net_socket_result_t net_classify_socket_error(int err)
{
    if (err == 0)
    {
        /* recv() returning zero is an orderly peer close. */
        return NET_PEER_LEFT;
    }

#if defined(EAGAIN)
    if (err == EAGAIN)
    {
        return NET_RETRY;
    }
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || EWOULDBLOCK != EAGAIN)
    if (err == EWOULDBLOCK)
    {
        return NET_RETRY;
    }
#endif
    if (err == EINTR)
    {
        return NET_RETRY;
    }

#if defined(ECONNRESET)
    if (err == ECONNRESET)
    {
        return NET_PEER_LEFT;
    }
#endif
#if defined(EPIPE)
    if (err == EPIPE)
    {
        return NET_PEER_LEFT;
    }
#endif
#if defined(ENOTCONN)
    if (err == ENOTCONN)
    {
        return NET_PEER_LEFT;
    }
#endif
#if defined(ECONNABORTED)
    if (err == ECONNABORTED)
    {
        return NET_PEER_LEFT;
    }
#endif
#if defined(ENETRESET)
    if (err == ENETRESET)
    {
        return NET_PEER_LEFT;
    }
#endif

#if defined(ENOMEM)
    if (err == ENOMEM)
    {
        return NET_RESOURCE_PRESSURE;
    }
#endif
#if defined(ENOBUFS)
    if (err == ENOBUFS)
    {
        return NET_RESOURCE_PRESSURE;
    }
#endif

#if defined(EMSGSIZE)
    if (err == EMSGSIZE)
    {
        return NET_PROTOCOL_ERROR;
    }
#endif
#if defined(EPROTO)
    if (err == EPROTO)
    {
        return NET_PROTOCOL_ERROR;
    }
#endif
    if (err == EINVAL)
    {
        return NET_PROTOCOL_ERROR;
    }

    return NET_INTERNAL_FAULT;
}
