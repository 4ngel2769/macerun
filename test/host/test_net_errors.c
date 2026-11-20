/* Host self-check for the socket-error taxonomy. No ESP-IDF dependencies. */
#include <assert.h>
#include <errno.h>
#include <stdio.h>

#include "net_errors.h"

static void test_eof_is_peer_left(void)
{
    assert(net_classify_socket_error(0) == NET_PEER_LEFT);
}

static void test_retryable_errors(void)
{
    assert(net_classify_socket_error(EAGAIN) == NET_RETRY);
    assert(net_classify_socket_error(EWOULDBLOCK) == NET_RETRY);
    assert(net_classify_socket_error(EINTR) == NET_RETRY);
}

static void test_reset_errors_are_peer_left(void)
{
    assert(net_classify_socket_error(ECONNRESET) == NET_PEER_LEFT);
    assert(net_classify_socket_error(EPIPE) == NET_PEER_LEFT);
    assert(net_classify_socket_error(ENOTCONN) == NET_PEER_LEFT);
    assert(net_classify_socket_error(ECONNABORTED) == NET_PEER_LEFT);
}

static void test_resource_pressure(void)
{
    assert(net_classify_socket_error(ENOMEM) == NET_RESOURCE_PRESSURE);
    assert(net_classify_socket_error(ENOBUFS) == NET_RESOURCE_PRESSURE);
}

static void test_protocol_error(void)
{
    assert(net_classify_socket_error(EMSGSIZE) == NET_PROTOCOL_ERROR);
}

static void test_unknown_is_internal_fault(void)
{
    assert(net_classify_socket_error(EDOM) == NET_INTERNAL_FAULT);
}

static void test_two_peers_left(void)
{
    assert(net_classify_socket_error(ECONNRESET) == NET_PEER_LEFT);
    assert(net_classify_socket_error(EPIPE) == NET_PEER_LEFT);
    assert(net_classify_socket_error(ECONNRESET) == NET_PEER_LEFT);
    assert(net_classify_socket_error(EPIPE) == NET_PEER_LEFT);
}

static void test_monotonic_ms_formula(void)
{
    assert(net_ticks_to_ms(0u, 1u) == 0u);
    assert(net_ticks_to_ms(40u, 1u) == 40u);
    assert(net_ticks_to_ms(4000u, 1u) == 4000u);
    assert(net_ticks_to_ms(100u, 10u) == 1000u);
}

int main(void)
{
    test_monotonic_ms_formula();
    test_eof_is_peer_left();
    test_retryable_errors();
    test_reset_errors_are_peer_left();
    test_resource_pressure();
    test_protocol_error();
    test_unknown_is_internal_fault();
    test_two_peers_left();
    printf("test_net_errors: all assertions passed\n");
    return 0;
}
