#include "../common/mqtt_packet.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

/*
 * Global publisher socket descriptor.
 * -1 indicates that no socket is currently open.
 */
static int g_fd = -1;

/*
 * Indicates whether the MQTT connection has been
 * successfully established with the broker.
 */
static int g_connected = 0;

/*
 * Clean up the publisher connection and socket.
 *
 * If the MQTT connection is active, send a normal
 * MQTT DISCONNECT packet before closing the TCP socket.
 */
static void publisher_cleanup(void)
{
    if (g_fd >= 0)
    {
        /*
         * Send MQTT DISCONNECT only if the MQTT
         * connection was successfully established.
         */
        if (g_connected)
        {
            mqtt_send_disconnect(g_fd, MQTT_RC_NORMAL_DISCONNECTION);
            mqtt_print_reason("DISCONNECT", MQTT_RC_NORMAL_DISCONNECTION);
            g_connected = 0;
        }

        /*
         * Shut down both directions of the TCP connection
         * and then release the socket descriptor.
         */
        shutdown(g_fd, SHUT_RDWR);
        close(g_fd);
        g_fd = -1;
    }
}

/*
 * Signal handler used for signals such as:
 * SIGINT  -> Ctrl+C
 * SIGTERM -> termination request
 *
 * The publisher performs its normal cleanup before exiting.
 */
static void on_signal(int sig)
{
    /*
     * The signal value is intentionally unused.
     */
    (void)sig;

    publisher_cleanup();

    /*
     * _exit() terminates immediately without executing
     * additional exit handlers.
     */
    _exit(0);
}

/*
 * Establish a TCP connection with the MQTT broker.
 *
 * ip:
 *     Broker IPv4 address.
 *
 * Returns:
 *     Socket descriptor on success.
 *     -1 on failure.
 */
static int tcp_connect(const char *ip)
{
    /*
     * Create an IPv4 TCP socket.
     */
    int f = socket(AF_INET, SOCK_STREAM, 0);

    if (f < 0)
        return -1;

    /*
     * Prepare the broker's IPv4 socket address.
     */
    struct sockaddr_in a = {0};

    a.sin_family = AF_INET;
    a.sin_port = htons(MQTT_PORT);

    /*
     * Convert the supplied IP address from text form
     * to binary form and connect to the broker.
     */
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1 ||
        connect(f, (struct sockaddr *)&a, sizeof a) < 0)
    {
        /*
         * Close the socket if address conversion or
         * TCP connection establishment fails.
         */
        close(f);
        return -1;
    }

    return f;
}

/*
 * Perform the MQTT CONNECT / CONNACK exchange.
 *
 * f:
 *     Connected TCP socket.
 *
 * cid:
 *     MQTT client identifier.
 *
 * Returns:
 *     0 when the broker accepts the connection.
 *    -1 when any step fails or the broker rejects it.
 */
static int do_connect(int f, const char *cid)
{
    printf("--- CONNECT ---\n");
    printf("  client_id   : %s\n", cid);
    printf("  protocol    : MQTT 5.0\n");
    fflush(stdout);

    /*
     * Build and send the MQTT CONNECT packet.
     */
    if (mqtt_send_connect(f, cid))
    {
        printf("  [ERROR] failed to send CONNECT\n");
        fflush(stdout);
        return -1;
    }

    mqtt_print_reason("CONNECT", MQTT_RC_SUCCESS);

    /*
     * Read the broker's response packet.
     *
     * The response is expected to be a CONNACK packet.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read CONNACK\n");
        fflush(stdout);
        return -1;
    }

    /*
     * Parse the CONNACK packet.
     *
     * session contains the Session Present flag.
     * reason contains the MQTT CONNACK reason code.
     */
    uint8_t session = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    int ok = (h == 0x20 &&
              !mqtt_parse_connack(b, l, &session, &reason));

    /*
     * The packet buffer was dynamically allocated by
     * mqtt_read_packet(), so release it after parsing.
     */
    free(b);

    mqtt_print_reason("CONNACK", reason);

    /*
     * The connection is accepted only when:
     * 1. The packet is a CONNACK packet.
     * 2. CONNACK parsing succeeds.
     * 3. The broker returns MQTT_RC_SUCCESS.
     */
    if (!ok || reason != MQTT_RC_SUCCESS)
    {
        printf("  [ERROR] connection rejected\n");
        fflush(stdout);
        return -1;
    }

    printf("  session_present : %u\n", session);
    printf("  [OK] connected\n");
    fflush(stdout);

    return 0;
}

/*
 * Publish one message using QoS 1 and wait for its PUBACK.
 *
 * pid:
 *     MQTT packet identifier used for the PUBLISH.
 *
 * topic:
 *     MQTT topic on which the message is published.
 *
 * value:
 *     Payload to be published.
 *
 * Returns:
 *     0 only when the broker returns a successful PUBACK.
 *    -1 when publishing or acknowledgement fails.
 */
static int pub_one(int f,
                   uint16_t pid,
                   const char *topic,
                   const char *value)
{
    printf("\n--- PUBLISH ---\n");
    printf("  topic     : %s\n", topic);
    printf("  payload   : %s\n", value);
    printf("  qos       : 1\n");
    printf("  packet_id : %u\n", pid);
    fflush(stdout);

    /*
     * Send the MQTT PUBLISH packet.
     *
     * QoS 1 is used, therefore a packet identifier is
     * included and the publisher expects a PUBACK.
     */
    if (mqtt_send_publish(f,
                          topic,
                          pid,
                          (const uint8_t *)value,
                          strlen(value)))
    {
        printf("  [ERROR] failed to send PUBLISH\n");
        fflush(stdout);
        return -1;
    }

    mqtt_print_reason("PUBLISH", MQTT_RC_SUCCESS);

    /*
     * Wait for the broker's PUBACK response.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read PUBACK\n");
        fflush(stdout);
        return -1;
    }

    /*
     * Parse the PUBACK and extract:
     * - ap     : acknowledged packet identifier
     * - reason : PUBACK reason code
     */
    uint16_t ap = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    int ok = (h == 0x40 &&
              !mqtt_parse_puback(b, l, &ap, &reason) &&
              ap == pid);

    /*
     * The received packet buffer is no longer required
     * after PUBACK parsing.
     */
    free(b);

    mqtt_print_reason("PUBACK", reason);

    /*
     * Verify that the received packet is actually a
     * PUBACK corresponding to the PUBLISH packet sent.
     */
    if (!ok)
    {
        printf("  [ERROR] unexpected PUBACK\n");
        fflush(stdout);
        return -1;
    }

    /*
     * MQTT_RC_SUCCESS means that the broker accepted
     * the published message.
     */
    if (reason == MQTT_RC_SUCCESS)
    {
        printf("  [OK] accepted (packet_id=%u)\n", ap);
        fflush(stdout);
        return 0;
    }

    /*
     * Any other PUBACK reason code indicates that the
     * broker rejected the publication.
     */
    printf("  [ERROR] rejected by broker\n");
    fflush(stdout);

    return -1;
}

/*
 * Display the publisher's interactive menu.
 */
static void print_menu(void)
{
    printf("\n========== MQTT PUBLISHER MENU ==========\n");
    printf("  1. Send valid sensor data\n");
    printf("  2. Send invalid sensor data\n");
    printf("  3. Exit\n");
    printf("=========================================\n");
    printf("Select: ");

    fflush(stdout);
}

int main(int argc, char **argv)
{
    /*
     * Use the broker IP supplied through the command line.
     *
     * Example:
     *     ./publisher 192.168.1.10
     *
     * If no IP is supplied, use localhost.
     */
    const char *ip = argc > 1 ? argv[1] : "127.0.0.1";

    /*
     * Register handlers for graceful termination.
     */
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /*
     * Ignore SIGPIPE so that a failed socket write does
     * not terminate the publisher unexpectedly.
     */
    signal(SIGPIPE, SIG_IGN);

    /*
     * Register cleanup to be executed when the program
     * terminates normally through exit/return from main.
     */
    atexit(publisher_cleanup);

    printf("========== MQTT PUBLISHER ==========\n");
    printf("Broker : %s:%d\n", ip, MQTT_PORT);
    printf("====================================\n");
    fflush(stdout);

    /*
     * Establish the TCP connection with the broker.
     */
    g_fd = tcp_connect(ip);

    if (g_fd < 0)
    {
        perror("publisher connect");
        return 1;
    }

    /*
     * Generate a unique client ID using the process ID.
     */
    char cid[64];

    snprintf(cid,
             sizeof cid,
             "publisher-%ld",
             (long)getpid());

    /*
     * Perform the MQTT CONNECT / CONNACK handshake.
     */
    if (do_connect(g_fd, cid))
        return 1;

    /*
     * MQTT connection has now been successfully established.
     */
    g_connected = 1;

    /*
     * Seed the random number generator using the current
     * time and process ID.
     */
    srand((unsigned)(time(NULL) ^ getpid()));

    /*
     * Initial packet identifier for QoS 1 PUBLISH packets.
     * It is incremented after every publication.
     */
    uint16_t pid = 1;

    /*
     * Display the publisher menu and read the user's choice.
     */
    print_menu();

    char line[32];

    if (!fgets(line, sizeof line, stdin))
    {
        printf("\n[PUBLISHER] done\n");
        return 0;
    }

    int choice = atoi(line);

    /*
     * Option 1:
     * Generate and publish valid temperature and humidity data.
     */
    if (choice == 1)
    {
        char t[32];
        char h[32];

        /*
         * Generate a random temperature between
         * approximately 20.00 and 34.99.
         */
        snprintf(t,
                 sizeof t,
                 "%.2f",
                 20.0f + (float)(rand() % 1500) / 100.0f);

        /*
         * Generate a random humidity between
         * approximately 40.00 and 89.99.
         */
        snprintf(h,
                 sizeof h,
                 "%.2f",
                 40.0f + (float)(rand() % 5000) / 100.0f);

        /*
         * Publish temperature data.
         */
        (void)pub_one(g_fd,
                      pid++,
                      "sensor/temperature",
                      t);

        /*
         * Publish humidity data.
         */
        (void)pub_one(g_fd,
                      pid++,
                      "sensor/humidity",
                      h);
    }

    /*
     * Option 2:
     * Send intentionally invalid sensor data.
     *
     * The payload is non-numeric, allowing the broker to
     * detect the invalid payload format and return the
     * corresponding MQTT reason code.
     */
    else if (choice == 2)
    {
        /* Non-numeric payload -> broker PUBACK 0x99 Payload format invalid */
        (void)pub_one(g_fd,
                      pid++,
                      "sensor/temperature",
                      "INVALID_SENSOR_DATA");
    }

    /*
     * Option 3:
     * Exit without publishing any sensor data.
     */
    else if (choice == 3)
    {
        /* Exit without publishing */
    }

    /*
     * Handle any input other than 1, 2, or 3.
     */
    else
    {
        printf("Invalid selection. Enter 1-3.\n");
        fflush(stdout);
    }

    /*
     * Perform a normal MQTT DISCONNECT before closing
     * the underlying TCP connection.
     */
    printf("\n--- DISCONNECT ---\n");
    fflush(stdout);

    if (g_connected)
    {
        mqtt_send_disconnect(g_fd, MQTT_RC_NORMAL_DISCONNECTION);
        mqtt_print_reason("DISCONNECT", MQTT_RC_NORMAL_DISCONNECTION);
        g_connected = 0;
    }

    /*
     * Shut down and close the TCP socket.
     */
    if (g_fd >= 0)
    {
        shutdown(g_fd, SHUT_RDWR);
        close(g_fd);
        g_fd = -1;
    }

    printf("[PUBLISHER] done\n");
    fflush(stdout);

    return 0;
}