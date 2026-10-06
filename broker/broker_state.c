#include "broker.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>

/*
 * Global broker state.
 *
 * This structure stores the broker-wide information such as:
 * - Connected client counters
 * - Current temperature and humidity values
 * - Client connection information
 * - Synchronization objects used by multiple broker threads
 */
BrokerState g_broker;

/*
 * Initialize the broker state once before the broker starts accepting clients.
 *
 * The complete BrokerState structure is cleared first so that all counters,
 * flags, sensor values, and client fields start from a known state.
 */
void broker_state_init(void)
{
    memset(&g_broker, 0, sizeof g_broker);

    /* No listening socket exists until the broker creates one. */
    g_broker.listen_fd = -1;

    /*
     * The broker state is accessed by multiple client threads.
     * This mutex protects shared broker data from simultaneous access.
     */
    pthread_mutex_init(&g_broker.mutex, NULL);

    /*
     * Initialize every client slot.
     *
     * socket_fd = -1 means that the slot does not currently contain
     * a valid client socket.
     *
     * Each client also has an ACK condition variable. It is used when
     * one thread has to wait for an MQTT acknowledgement associated
     * with that client.
     */
    for (int i = 0; i < BROKER_MAX_CLIENTS; i++)
    {
        g_broker.clients[i].socket_fd = -1;
        pthread_cond_init(&g_broker.clients[i].ack_cond, NULL);
    }
}

/*
 * Reset the runtime state before starting a new broker listening session.
 *
 * This is different from broker_state_init():
 * - broker_state_init() performs the one-time initialization of the
 *   broker structure and synchronization objects.
 * - broker_state_reset_for_start() resets values that belong to a
 *   particular broker run.
 *
 * The dashboard frame itself is not recreated here.
 */
void broker_state_reset_for_start(void)
{
    pthread_mutex_lock(&g_broker.mutex);

    /*
     * A new listening session starts with no connected clients.
     * Publisher and subscriber counts are also reset because the
     * previous session's clients are no longer considered active.
     */
    g_broker.connected_clients = 0;
    g_broker.connected_subscribers = 0;
    g_broker.connected_publishers = 0;

    /*
     * Clear the last known sensor values.
     *
     * The *_received flags distinguish an actual received value from
     * the initial zero value.
     */
    g_broker.temperature = g_broker.humidity = 0;
    g_broker.temperature_received = g_broker.humidity_received = 0;

    pthread_mutex_unlock(&g_broker.mutex);
}

/*
 * Find an unused client slot.
 *
 * This function expects the caller to already hold g_broker.mutex.
 * It therefore does not lock the mutex itself.
 *
 * Returns:
 *   >= 0 : index of a free client slot
 *   -1   : all client slots are currently occupied
 */
int broker_find_free_client_locked(void)
{
    for (int i = 0; i < BROKER_MAX_CLIENTS; i++)
        if (!g_broker.clients[i].connected)
            return i;

    return -1;
}

/*
 * Record that a client has successfully subscribed to a topic.
 *
 * A single MQTT client can subscribe to more than one topic. Therefore,
 * connected_subscribers is incremented only when the client becomes a
 * subscriber for the first time.
 */
void broker_state_mark_subscriber(MQTTClient *c, const char *t, uint8_t qos)
{
    pthread_mutex_lock(&g_broker.mutex);

    /*
     * Count the client only once as a subscriber.
     *
     * For example, a client subscribing first to temperature and then
     * humidity is still one subscriber, not two subscribers.
     */
    if (!c->is_subscriber)
    {
        c->is_subscriber = 1;
        g_broker.connected_subscribers++;
    }

    /* Store the subscription information associated with this client. */
    c->subscription_active = 1;
    c->subscription_qos = qos;

    /*
     * Keep the most recently accepted topic filter in the client's
     * topic_filter field.
     */
    snprintf(c->topic_filter, sizeof c->topic_filter, "%s", t);

    /*
     * Maintain separate flags because one client may subscribe to
     * both supported sensor topics.
     */
    if (!strcmp(t, "sensor/temperature"))
        c->subscribed_temperature = 1;

    if (!strcmp(t, "sensor/humidity"))
        c->subscribed_humidity = 1;

    pthread_mutex_unlock(&g_broker.mutex);
}

/*
 * Store the latest sensor value received by the broker.
 *
 * The topic determines whether the value belongs to temperature
 * or humidity. The corresponding *_received flag is set so that
 * the dashboard can distinguish "not received yet" from a value
 * that happens to be zero.
 */
void broker_state_record_value(const char *t, float v)
{
    pthread_mutex_lock(&g_broker.mutex);

    if (!strcmp(t, "sensor/temperature"))
    {
        g_broker.temperature = v;
        g_broker.temperature_received = 1;
    }
    else if (!strcmp(t, "sensor/humidity"))
    {
        g_broker.humidity = v;
        g_broker.humidity_received = 1;
    }

    pthread_mutex_unlock(&g_broker.mutex);
}

/*
 * Remove a client from the broker's active state.
 *
 * This function is called when a client sends DISCONNECT or when its
 * socket connection is lost.
 *
 * All counters are updated according to the roles previously assigned
 * to the client.
 */
void broker_state_remove_client(MQTTClient *c)
{
    pthread_mutex_lock(&g_broker.mutex);

    /*
     * Only decrement the global counters if the MQTT CONNECT was
     * successfully completed for this client.
     *
     * This prevents an incomplete connection from incorrectly changing
     * the broker's client/publisher/subscriber counters.
     */
    if (c->mqtt_connected)
    {
        if (g_broker.connected_clients > 0)
            g_broker.connected_clients--;

        if (c->is_subscriber && g_broker.connected_subscribers > 0)
            g_broker.connected_subscribers--;

        if (c->is_publisher && g_broker.connected_publishers > 0)
            g_broker.connected_publishers--;
    }

    /*
     * Clear the client's runtime state so that its slot can be reused
     * by another connection.
     */
    c->connected = 0;
    c->mqtt_connected = 0;
    c->is_subscriber = 0;
    c->is_publisher = 0;
    c->subscription_active = 0;
    c->subscription_attempted = 0;
    c->subscribed_temperature = 0;
    c->subscribed_humidity = 0;

    /*
     * No acknowledgement should remain pending after the client
     * has been removed.
     */
    c->pending_ack = 0;

    /*
     * Mark the client as stopping and wake any thread waiting on
     * this client's ACK condition variable.
     */
    c->stopping = 1;
    pthread_cond_broadcast(&c->ack_cond);

    pthread_mutex_unlock(&g_broker.mutex);
}

/*
 * Stop all currently connected clients.
 *
 * This is normally used when the broker itself is shutting down.
 * Setting stopping causes client threads to terminate, while shutdown()
 * interrupts blocking socket operations such as recv().
 */
void broker_stop_clients(void)
{
    pthread_mutex_lock(&g_broker.mutex);

    for (int i = 0; i < BROKER_MAX_CLIENTS; i++)
    {
        if (g_broker.clients[i].connected &&
            g_broker.clients[i].socket_fd >= 0)
        {
            /*
             * Tell the client thread that it should terminate.
             */
            g_broker.clients[i].stopping = 1;

            /*
             * Wake the thread if it is blocked in a socket operation.
             * SHUT_RDWR disables both receiving and sending on the socket.
             */
            shutdown(g_broker.clients[i].socket_fd, SHUT_RDWR);
        }
    }

    pthread_mutex_unlock(&g_broker.mutex);
}

/*
 * Wait for all client threads that were successfully created.
 *
 * pthread_join() ensures that the thread resources are released and
 * that the broker does not leave terminated client threads behind.
 *
 * thread_started is cleared after a successful join so the client slot
 * no longer appears to contain an active thread.
 */
void broker_cleanup_threads(void)
{
    for (int i = 0; i < BROKER_MAX_CLIENTS; i++)
    {
        if (g_broker.clients[i].thread_started)
        {
            pthread_join(g_broker.clients[i].thread, NULL);
            g_broker.clients[i].thread_started = 0;
        }
    }
}