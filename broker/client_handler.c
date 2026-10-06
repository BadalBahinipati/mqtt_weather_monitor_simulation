#include "broker.h"

#include "../common/mqtt_packet.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

/*
 * Check whether the topic name is one of the topics supported
 * by this broker.
 *
 * Only temperature and humidity sensor topics are accepted.
 */
static int valid_topic_name(const char *t)
{
    return !strcmp(t, "sensor/temperature") ||
           !strcmp(t, "sensor/humidity");
}

/*
 * Validate a subscription filter.
 *
 * This broker currently supports only exact topic names and
 * does not support wildcard subscriptions.
 */
static int valid_subscription_filter(const char *t)
{
    return valid_topic_name(t);
}

/*
 * Determine the MQTT reason code for an invalid subscription filter.
 *
 * '#' and '+' are detected separately so that malformed wildcard
 * placement can be reported as an invalid topic filter.
 *
 * Properly positioned wildcards are recognized, but wildcard
 * subscriptions themselves are not supported by this broker.
 */
static int subscription_reason(const char *t)
{
    const char *h = strchr(t, '#');
    const char *p = strchr(t, '+');

    /* '#' is valid only as the final character and must follow '/'. */
    if (h && (h[1] != '\0' || (h != t && h[-1] != '/')))
        return MQTT_RC_TOPIC_FILTER_INVALID;

    /* '+' must occupy an entire topic level. */
    if (p && ((p != t && p[-1] != '/') ||
              (p[1] != '\0' && p[1] != '/')))
        return MQTT_RC_TOPIC_FILTER_INVALID;

    /*
     * The wildcard syntax itself may be valid, but this broker
     * does not implement wildcard subscriptions.
     */
    if (h || p)
        return MQTT_RC_WILDCARD_SUBS_NOT_SUPPORTED;

    /* No supported topic/filter was found. */
    return MQTT_RC_TOPIC_FILTER_INVALID;
}

/*
 * Close a client connection and remove the client from the
 * broker's active client state.
 *
 * The socket is detached from the client structure while the
 * broker mutex is held, then shutdown/close is performed outside
 * the mutex to avoid holding the global lock during system calls.
 */
static void client_close(MQTTClient *c)
{
    int fd = -1;
    int was_mqtt;

    pthread_mutex_lock(&g_broker.mutex);

    /* Remember whether this client completed the MQTT CONNECT step. */
    was_mqtt = c->mqtt_connected;

    /*
     * Mark the socket as invalid before releasing the mutex.
     * This prevents other broker code from using the same descriptor.
     */
    if (c->socket_fd >= 0)
    {
        fd = c->socket_fd;
        c->socket_fd = -1;
    }

    pthread_mutex_unlock(&g_broker.mutex);

    /* Terminate the TCP connection if a valid descriptor was present. */
    if (fd >= 0)
    {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }

    /* Remove the client and update broker counters/state. */
    broker_state_remove_client(c);

    /* Refresh the dashboard only for an MQTT client. */
    if (was_mqtt)
        broker_display_dashboard();
}

/*
 * Forward a published message to every subscriber that has
 * subscribed to the corresponding topic.
 *
 * The subscriber list is copied while holding the broker mutex.
 * The actual network transmission is performed after releasing
 * the mutex so that a slow subscriber does not block other clients.
 */
static int route_to_subscribers(const char *topic,
                                const uint8_t *payload,
                                size_t plen)
{
    MQTTClient *targets[BROKER_MAX_CLIENTS];
    int n = 0;

    pthread_mutex_lock(&g_broker.mutex);

    /*
     * Find all active subscribers interested in the published topic.
     * Temperature and humidity subscriptions are tracked separately.
     */
    for (int i = 0; i < BROKER_MAX_CLIENTS; i++)
    {
        MQTTClient *c = &g_broker.clients[i];

        if (c->connected &&
            c->is_subscriber &&
            c->subscription_active &&
            ((!strcmp(topic, "sensor/temperature") &&
              c->subscribed_temperature) ||
             (!strcmp(topic, "sensor/humidity") &&
              c->subscribed_humidity)))
        {
            targets[n++] = c;
        }
    }

    pthread_mutex_unlock(&g_broker.mutex);

    /* Display how many subscribers will receive the message. */
    if (n)
    {
        printf("[BROKER] forwarding %s to %d subscriber(s)\n", topic, n);
        fflush(stdout);
    }

    /*
     * Send the PUBLISH packet to each matching subscriber.
     * QoS 1 requires the broker to wait for a PUBACK.
     */
    for (int i = 0; i < n; i++)
    {
        MQTTClient *c = targets[i];

        pthread_mutex_lock(&g_broker.mutex);

        /*
         * Allocate the next packet identifier used by the broker
         * for its outgoing QoS 1 PUBLISH.
         */
        uint16_t pid = ++c->next_server_pid;

        /* Packet identifier 0 is not valid, so skip it. */
        if (!pid)
            pid = ++c->next_server_pid;

        /* Store information needed while waiting for PUBACK. */
        c->pending_pid = pid;
        c->pending_ack = 0;

        int fd = c->socket_fd;

        pthread_mutex_unlock(&g_broker.mutex);

        /* Send the published sensor value to the subscriber. */
        if (mqtt_send_publish(fd, topic, pid, payload, plen) != 0)
            continue;

        /*
         * Wait until the subscriber sends PUBACK, disconnects,
         * or the client is being stopped.
         */
        pthread_mutex_lock(&g_broker.mutex);

        while (c->connected &&
               !c->pending_ack &&
               !c->stopping)
        {
            pthread_cond_wait(&c->ack_cond, &g_broker.mutex);
        }

        int ok = c->pending_ack;

        /* Clear the acknowledgement state for the next message. */
        c->pending_ack = 0;

        pthread_mutex_unlock(&g_broker.mutex);

        /* Continue with the next subscriber if no PUBACK was received. */
        if (!ok)
            continue;
    }

    return 0;
}

/*
 * Thread responsible for handling one connected MQTT client.
 *
 * The same handler processes CONNECT, SUBSCRIBE, UNSUBSCRIBE,
 * PUBLISH, PUBACK and DISCONNECT packets.
 */
void *broker_client_thread(void *arg)
{
    MQTTClient *c = arg;

    /* Store the client's socket locally for packet processing. */
    int fd = c->socket_fd;

    /* MQTT CONNECT must be received before other MQTT packets. */
    int connected = 0;

    for (;;)
    {
        uint8_t h, *body = NULL;
        uint32_t blen = 0;

        /*
         * Read one complete MQTT packet from the client.
         * A non-zero return indicates a read/packet error or disconnect.
         */
        if (mqtt_read_packet(fd, &h, &body, &blen))
        {
            free(body);
            break;
        }

        /*
         * Split the first byte into the MQTT packet type and flags.
         * blen is the already-decoded Remaining Length.
         */
        MQTT_FixedHeader fh =
            {
                (uint8_t)(h >> 4),
                (uint8_t)(h & 15),
                blen};

        uint8_t type = fh.packet_type;
        uint8_t flags = fh.flags;

        /* rc is used to indicate that the connection should be closed. */
        int rc = 0;

        /*
         * -------------------------------------------------------------
         * CONNECT
         * -------------------------------------------------------------
         * MQTT requires CONNECT to be the first packet sent by a client.
         */
        if (!connected)
        {
            /* CONNECT must have packet type 1 and flags 0000. */
            if (type != MQTT_CONNECT || flags != 0)
            {
                mqtt_send_connack(fd, MQTT_RC_PROTOCOL_ERROR);
                free(body);
                break;
            }

            MQTT_Connect cp = {0};
            char cid[128];

            /* Decode the MQTT CONNECT variable header and payload. */
            int dcrc =
                mqtt_decode_connect(body, blen, &cp, cid, sizeof cid);

            /*
             * A return value of -2 indicates that the MQTT protocol
             * version requested by the client is not supported.
             */
            if (dcrc == -2)
            {
                mqtt_send_connack(fd, MQTT_RC_UNSUPPORTED_PROTOCOL_VER);
                free(body);
                break;
            }

            /* Any other decoding failure is treated as malformed data. */
            if (dcrc != 0)
            {
                mqtt_send_connack(fd, MQTT_RC_MALFORMED_PACKET);
                free(body);
                break;
            }

            /* This broker requires a non-empty Client Identifier. */
            if (!cid[0])
            {
                mqtt_send_connack(fd,
                                  MQTT_RC_CLIENT_IDENTIFIER_NOT_VALID);
                free(body);
                break;
            }

            /* Save the client's identifier in the broker state. */
            snprintf(c->client_id, sizeof c->client_id, "%s", cid);

            c->mqtt_connected = 1;
            connected = 1;

            /* Update the global MQTT client count. */
            pthread_mutex_lock(&g_broker.mutex);
            g_broker.connected_clients++;
            pthread_mutex_unlock(&g_broker.mutex);

            /*
             * Send successful CONNACK only after the CONNECT packet
             * has passed all validation checks.
             */
            if (mqtt_send_connack(fd, MQTT_RC_SUCCESS))
            {
                free(body);
                break;
            }

            printf("[BROKER] CONNECT from %s\n", c->client_id);
            fflush(stdout);

            /* Reflect the newly connected client on the dashboard. */
            broker_display_dashboard();
        }

        /*
         * -------------------------------------------------------------
         * SUBSCRIBE
         * -------------------------------------------------------------
         * Register the client's requested topic subscription.
         */
        else if (type == MQTT_SUBSCRIBE)
        {
            /* SUBSCRIBE requires fixed-header flags 0010. */
            if (flags != 2)
                rc = 1;
            else
            {
                uint16_t pid;
                char topic[128];
                uint8_t qos;

                /* Decode the packet identifier, topic and requested QoS. */
                if (mqtt_parse_subscribe(body, blen,
                                         &pid, topic,
                                         sizeof topic, &qos) ||
                    !pid)
                {
                    rc = 1;
                }
                /*
                 * This broker supports QoS 0 and QoS 1 subscriptions
                 * only; higher requested QoS values are rejected.
                 */
                else if (qos > 1)
                {
                    if (mqtt_send_suback(fd,
                                         pid,
                                         MQTT_RC_QOS_NOT_SUPPORTED))
                        rc = 1;
                    else
                    {
                        printf("[BROKER] SUBSCRIBE rejected "
                               "(QoS not supported): %s\n",
                               topic);
                        fflush(stdout);
                    }
                }
                /*
                 * Reject unsupported topic names and wildcard filters.
                 */
                else if (!valid_subscription_filter(topic))
                {
                    int subrc = subscription_reason(topic);

                    pthread_mutex_lock(&g_broker.mutex);
                    c->subscription_attempted = 1;
                    pthread_mutex_unlock(&g_broker.mutex);

                    if (mqtt_send_suback(fd, pid, (uint8_t)subrc))
                        rc = 1;
                    else
                    {
                        printf("[BROKER] SUBSCRIBE rejected: %s\n", topic);
                        fflush(stdout);
                    }
                }
                else
                {
                    int was;

                    /* Check whether this client was already a subscriber. */
                    pthread_mutex_lock(&g_broker.mutex);
                    was = c->is_subscriber;
                    pthread_mutex_unlock(&g_broker.mutex);

                    /* Add the requested topic to the client's state. */
                    broker_state_mark_subscriber(c, topic, qos);

                    if (!was)
                    {
                        printf("[BROKER] subscriber registered: %s\n",
                               c->client_id);
                        fflush(stdout);
                    }

                    printf("[BROKER] SUBSCRIBE accepted: %s\n", topic);
                    fflush(stdout);

                    /*
                     * Return the granted QoS. The broker grants QoS 0
                     * or QoS 1 according to the requested QoS.
                     */
                    uint8_t grant =
                        qos ? MQTT_RC_GRANTED_QOS_1
                            : MQTT_RC_GRANTED_QOS_0;

                    if (mqtt_send_suback(fd, pid, grant))
                        rc = 1;

                    broker_display_dashboard();
                }
            }
        }

        /*
         * -------------------------------------------------------------
         * UNSUBSCRIBE
         * -------------------------------------------------------------
         * Remove a previously registered topic subscription.
         */
        else if (type == MQTT_UNSUBSCRIBE)
        {
            /* UNSUBSCRIBE also requires fixed-header flags 0010. */
            if (flags != 2)
                rc = 1;
            else
            {
                uint16_t pid;
                char topic[128];

                if (mqtt_parse_unsubscribe(body, blen,
                                           &pid, topic,
                                           sizeof topic) ||
                    !pid)
                {
                    rc = 1;
                }
                else
                {
                    int had = 0;

                    pthread_mutex_lock(&g_broker.mutex);

                    /* Remove the temperature subscription if present. */
                    if (!strcmp(topic, "sensor/temperature") &&
                        c->subscribed_temperature)
                    {
                        c->subscribed_temperature = 0;
                        had = 1;
                    }

                    /* Remove the humidity subscription if present. */
                    if (!strcmp(topic, "sensor/humidity") &&
                        c->subscribed_humidity)
                    {
                        c->subscribed_humidity = 0;
                        had = 1;
                    }

                    /*
                     * If no topics remain, the client is no longer
                     * considered an active subscriber.
                     */
                    if (!c->subscribed_temperature &&
                        !c->subscribed_humidity)
                    {
                        c->subscription_active = 0;
                    }

                    pthread_mutex_unlock(&g_broker.mutex);

                    /*
                     * MQTT distinguishes between successfully removing
                     * a subscription and one that never existed.
                     */
                    uint8_t urc =
                        had ? MQTT_RC_SUCCESS
                            : MQTT_RC_NO_SUBSCRIPTION_EXISTED;

                    if (mqtt_send_unsuback(fd, pid, urc))
                        rc = 1;
                }
            }
        }

        /*
         * -------------------------------------------------------------
         * PUBLISH
         * -------------------------------------------------------------
         * Process sensor data received from a publisher.
         */
        else if (type == MQTT_PUBLISH)
        {
            /*
             * Extract QoS, DUP and RETAIN from the MQTT fixed header.
             */
            uint8_t qos = (fh.flags >> 1) & 3;
            int dup = (fh.flags >> 3) & 1;
            int retain = fh.flags & 1;

            MQTT_Publish pp = {0};
            char topic[128];

            /*
             * Decode the PUBLISH packet and obtain its topic,
             * packet identifier and payload.
             */
            int parsed =
                mqtt_decode_publish(body, blen, &pp,
                                    topic, sizeof topic, &fh) == 0;

            uint16_t pid = pp.packet_id;
            const uint8_t *payload = pp.payload;
            size_t plen = pp.payload_len;

            /* Reject a packet that could not be decoded correctly. */
            if (!parsed)
            {
                rc = 1;

                printf("[BROKER] malformed PUBLISH\n");
                fflush(stdout);
            }

            /*
             * Only the two application topics are accepted.
             */
            else if (!valid_topic_name(topic))
            {
                /*
                 * Return an MQTT PUBACK reason code indicating that
                 * the topic name is not valid.
                 */
                if (pid &&
                    mqtt_send_puback(fd,
                                     pid,
                                     MQTT_RC_TOPIC_NAME_INVALID) == 0)
                    ;
                else
                    rc = 1;

                printf("[BROKER] PUBLISH rejected "
                       "(invalid topic): %s\n",
                       topic);
                fflush(stdout);
            }

            /*
             * This project accepts QoS 1 PUBLISH packets without
             * DUP or RETAIN flags.
             */
            else if (!pid || retain || qos != 1 || dup)
            {
                if (pid &&
                    mqtt_send_puback(fd,
                                     pid,
                                     MQTT_RC_IMPLEMENTATION_SPECIFIC) == 0)
                    ;
                else
                    rc = 1;

                printf("[BROKER] PUBLISH rejected "
                       "(invalid request)\n");
                fflush(stdout);
            }

            /*
             * Reject empty payloads and payloads that exceed the
             * application's maximum sensor-value length.
             */
            else if (!plen || plen >= 64)
            {
                if (mqtt_send_puback(fd,
                                     pid,
                                     MQTT_RC_IMPLEMENTATION_SPECIFIC) == 0)
                    ;
                else
                    rc = 1;

                printf("[BROKER] PUBLISH rejected "
                       "(incomplete data)\n");
                fflush(stdout);
            }

            else
            {
                char tmp[64];

                /*
                 * Copy the payload into a null-terminated buffer so
                 * that it can safely be processed by strtof().
                 */
                memcpy(tmp, payload, plen);
                tmp[plen] = 0;

                char *end = NULL;

                /* Convert the sensor value from text to floating point. */
                float v = strtof(tmp, &end);

                /*
                 * If conversion did not consume the entire payload,
                 * the sensor value is not a valid numeric value.
                 */
                if (end == tmp || *end != '\0')
                {
                    if (mqtt_send_puback(fd,
                                         pid,
                                         MQTT_RC_PAYLOAD_FORMAT_INVALID) == 0)
                        ;
                    else
                        rc = 1;

                    printf("[BROKER] PUBLISH rejected "
                           "(invalid payload): %s\n",
                           tmp);
                    fflush(stdout);
                }
                else
                {
                    int first = 0;

                    /*
                     * The first valid PUBLISH from a client identifies
                     * that client as a publisher.
                     */
                    pthread_mutex_lock(&g_broker.mutex);

                    if (!c->is_publisher)
                    {
                        c->is_publisher = 1;
                        g_broker.connected_publishers++;
                        first = 1;
                    }

                    pthread_mutex_unlock(&g_broker.mutex);

                    if (first)
                    {
                        printf("[BROKER] publisher registered: %s\n",
                               c->client_id);
                        fflush(stdout);
                    }

                    printf("[BROKER] PUBLISH %s = %s\n", topic, tmp);
                    fflush(stdout);

                    /*
                     * Store the latest sensor value in the broker state
                     * so that it can be shown on the dashboard.
                     */
                    broker_state_record_value(topic, v);

                    /*
                     * A successful PUBACK confirms that the broker
                     * accepted the publisher's message.
                     *
                     * Only after successful acknowledgement is the
                     * message forwarded to matching subscribers.
                     */
                    if (mqtt_send_puback(fd, pid, MQTT_RC_SUCCESS) == 0)
                        (void)route_to_subscribers(topic, payload, plen);
                    else
                        rc = 1;

                    /* Refresh the dashboard with the latest sensor data. */
                    broker_display_dashboard();
                }
            }
        }

        /*
         * -------------------------------------------------------------
         * PUBACK
         * -------------------------------------------------------------
         * Handle the acknowledgement sent by a subscriber after
         * receiving a QoS 1 PUBLISH from the broker.
         */
        else if (type == MQTT_PUBACK)
        {
            /* PUBACK must use fixed-header flags 0000. */
            if (flags)
                rc = 1;
            else
            {
                uint16_t pid;
                uint8_t reason;

                /*
                 * Decode the packet identifier and reason code.
                 * This broker expects a successful acknowledgement.
                 */
                if (mqtt_parse_puback(body, blen,
                                      &pid, &reason) ||
                    reason != MQTT_RC_SUCCESS)
                {
                    rc = 1;
                }
                else
                {
                    pthread_mutex_lock(&g_broker.mutex);

                    /*
                     * Confirm that the PUBACK corresponds to the
                     * message currently awaiting acknowledgement.
                     */
                    if (c->pending_pid == pid)
                    {
                        c->pending_ack = 1;

                        /*
                         * Wake route_to_subscribers(), which is waiting
                         * for this acknowledgement.
                         */
                        pthread_cond_signal(&c->ack_cond);
                    }

                    pthread_mutex_unlock(&g_broker.mutex);
                }
            }
        }

        /*
         * -------------------------------------------------------------
         * DISCONNECT
         * -------------------------------------------------------------
         * Handle a normal MQTT client disconnection.
         */
        else if (type == MQTT_DISCONNECT)
        {
            uint8_t reason = MQTT_RC_NORMAL_DISCONNECTION;

            /*
             * DISCONNECT must have zero fixed-header flags and contain
             * a valid MQTT reason code if one is present.
             */
            if (flags ||
                mqtt_parse_disconnect(body, blen, &reason))
            {
                rc = 1;
            }

            free(body);
            break;
        }

        /*
         * Any packet type not explicitly supported by this broker
         * is treated as a protocol error.
         */
        else
        {
            rc = 1;
        }

        free(body);

        /*
         * If processing failed, send a protocol-error DISCONNECT
         * before closing an already-established MQTT connection.
         */
        if (rc)
        {
            if (connected)
                mqtt_send_disconnect(fd, MQTT_RC_PROTOCOL_ERROR);

            break;
        }
    }

    /*
     * Save the client's role information before removing its state.
     * These values are used for the final disconnect log message.
     */
    int was_sub, was_pub, attempted;

    pthread_mutex_lock(&g_broker.mutex);

    was_sub = c->is_subscriber;
    was_pub = c->is_publisher;
    attempted = c->subscription_attempted;

    pthread_mutex_unlock(&g_broker.mutex);

    /*
     * Close the socket and update all broker-side counters/state.
     */
    client_close(c);

    /*
     * Identify the client according to the role it had during
     * the connection. A client that attempted a subscription but
     * never successfully subscribed is still reported as subscriber.
     */
    printf("[BROKER] %s disconnected\n",
           (was_sub || attempted)
               ? "subscriber"
               : (was_pub ? "publisher" : "client"));

    fflush(stdout);

    return NULL;
}