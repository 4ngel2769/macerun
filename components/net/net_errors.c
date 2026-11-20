#include "net_errors.h"

#include <errno.h>

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
