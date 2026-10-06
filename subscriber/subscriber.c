#include "../common/mqtt_packet.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/select.h>

/*
 * Global TCP socket used by the subscriber.
 * -1 means that no socket is currently open.
 */
static int g_fd = -1;

/*
 * Indicates whether the MQTT CONNECT / CONNACK
 * exchange has completed successfully.
 */
static int g_connected = 0;

/*
 * IP address of the MQTT broker.
 * localhost is used when no address is supplied
 * through the command line.
 */
static const char *g_broker_ip = "127.0.0.1";

/*
 * Clean up the subscriber connection.
 *
 * If the MQTT session is active, send a normal
 * DISCONNECT packet before closing the TCP socket.
 */
static void subscriber_cleanup(void)
{
    if (g_fd >= 0)
    {
        /*
         * Send MQTT DISCONNECT only when the MQTT
         * connection has already been established.
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
 * Handle termination signals such as Ctrl+C.
 *
 * The connection is cleaned up before the process exits.
 */
static void on_signal(int sig)
{
    /*
     * The signal number is not required here.
     */
    (void)sig;

    subscriber_cleanup();

    /*
     * Exit immediately after cleanup.
     */
    _exit(0);
}

/*
 * Establish a TCP connection with the MQTT broker.
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
     * Prepare the broker's IPv4 address and MQTT port.
     */
    struct sockaddr_in a = {0};

    a.sin_family = AF_INET;
    a.sin_port = htons(MQTT_PORT);

    /*
     * Convert the textual IPv4 address to binary form
     * and establish the TCP connection.
     */
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1 ||
        connect(f, (struct sockaddr *)&a, sizeof a) < 0)
    {
        /*
         * Close the socket if either address conversion
         * or connection establishment fails.
         */
        close(f);
        return -1;
    }

    return f;
}

/*
 * Perform the MQTT CONNECT / CONNACK handshake.
 *
 * cid:
 *     MQTT client identifier.
 *
 * Returns:
 *     0 when the broker accepts the connection.
 *    -1 when the connection fails or is rejected.
 */
static int do_connect(int f, const char *cid)
{
    printf("--- CONNECT ---\n");
    printf("  client_id   : %s\n", cid);
    printf("  protocol    : MQTT 5.0\n");
    fflush(stdout);

    /*
     * Send the MQTT CONNECT packet.
     */
    if (mqtt_send_connect(f, cid))
    {
        printf("  [ERROR] failed to send CONNECT\n");
        fflush(stdout);
        return -1;
    }

    /*
     * Locally report that the CONNECT request was
     * successfully sent.
     */
    mqtt_print_reason("CONNECT", MQTT_RC_SUCCESS);

    /*
     * Read the broker's CONNACK packet.
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
     * reason contains the CONNACK reason code.
     */
    uint8_t session = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    int ok = (h == 0x20 &&
              !mqtt_parse_connack(b, l, &session, &reason));

    /*
     * Release the packet buffer after parsing.
     */
    free(b);

    mqtt_print_reason("CONNACK", reason);

    /*
     * The connection is accepted only if the packet
     * is a valid CONNACK and its reason code is success.
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
 * Send an MQTT SUBSCRIBE packet and wait for its SUBACK.
 *
 * Returns:
 *     The SUBACK reason code.
 *     0xFF if sending or receiving the SUBACK fails.
 */
static uint8_t do_subscribe(int f,
                            uint16_t pid,
                            const char *topic,
                            uint8_t qos)
{
    printf("\n--- SUBSCRIBE ---\n");
    printf("  topic     : %s\n", topic);
    printf("  qos       : %u\n", qos);
    printf("  packet_id : %u\n", pid);
    fflush(stdout);

    /*
     * Send the subscription request to the broker.
     */
    if (mqtt_send_subscribe(f, pid, topic, qos))
    {
        printf("  [ERROR] failed to send SUBSCRIBE\n");
        fflush(stdout);
        return 0xFF;
    }

    mqtt_print_reason("SUBSCRIBE", MQTT_RC_SUCCESS);

    /*
     * Wait for the broker's SUBACK packet.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read SUBACK\n");
        fflush(stdout);
        return 0xFF;
    }

    /*
     * Default to an unspecified error until a valid
     * SUBACK reason code is extracted.
     */
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;
    int ok = 0;

    /*
     * Verify:
     * 1. Packet type is SUBACK (0x90).
     * 2. Packet is large enough to contain the packet ID
     *    and reason code.
     * 3. Returned packet ID matches the SUBSCRIBE packet.
     */
    if (h == 0x90 &&
        l >= 4 &&
        b[0] == (uint8_t)(pid >> 8) &&
        b[1] == (uint8_t)pid)
    {
        /*
         * For this project's SUBACK format, the reason
         * code is stored at byte 3.
         */
        reason = b[3];
        ok = 1;
    }

    /*
     * Release the received packet buffer.
     */
    free(b);

    mqtt_print_reason("SUBACK", reason);

    if (!ok)
    {
        printf("  [ERROR] unexpected SUBACK\n");
    }
    else if (reason <= MQTT_RC_GRANTED_QOS_2)
    {
        /*
         * A reason code from Granted QoS 0 through
         * Granted QoS 2 indicates successful subscription.
         */
        printf("  [OK] subscription granted\n");
    }
    else
    {
        /*
         * Any other reason code represents a rejected
         * subscription request.
         */
        printf("  [ERROR] subscription rejected\n");
    }

    fflush(stdout);

    return reason;
}

/*
 * Wait for one incoming PUBLISH packet.
 *
 * timeout_sec:
 *     Maximum number of seconds to wait for data.
 *
 * Returns:
 *      0 on successful message reception.
 *     -1 on timeout or packet/error condition.
 *
 * The received sensor message is printed and a PUBACK
 * is sent when the incoming PUBLISH uses QoS 1.
 */
static int receive_one(int f, int timeout_sec)
{
    /*
     * Prepare an fd_set containing the subscriber socket.
     */
    fd_set rfds;

    FD_ZERO(&rfds);
    FD_SET(f, &rfds);

    /*
     * Configure the maximum waiting time for select().
     */
    struct timeval tv = {
        .tv_sec = timeout_sec,
        .tv_usec = 0};

    /*
     * Wait until the socket becomes readable or the
     * timeout expires.
     */
    int sel = select(f + 1, &rfds, NULL, NULL, &tv);

    if (sel <= 0)
    {
        printf("\n--- WAIT ---\n");
        printf("  [ERROR] no message within %d second(s)\n",
               timeout_sec);
        printf("  (publisher may have sent invalid data that was not forwarded)\n");
        fflush(stdout);
        return -1;
    }

    /*
     * Read the complete MQTT packet from the broker.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read PUBLISH\n");
        fflush(stdout);
        return -1;
    }

    /*
     * Verify that the received packet is a PUBLISH packet.
     *
     * The packet type is stored in the upper four bits
     * of the MQTT fixed header.
     */
    if ((h >> 4) != MQTT_PUBLISH)
    {
        printf("  [ERROR] unexpected packet type 0x%02X\n", h >> 4);
        free(b);
        fflush(stdout);
        return -1;
    }

    /*
     * Build the common MQTT fixed-header structure.
     *
     * h >> 4:
     *     Packet type.
     *
     * h & 15:
     *     Flags.
     *
     * l:
     *     Remaining Length.
     */
    MQTT_FixedHeader fh = {
        (uint8_t)(h >> 4),
        (uint8_t)(h & 15),
        l};

    /*
     * Structure used to hold the decoded PUBLISH fields.
     */
    MQTT_Publish p;

    /*
     * Buffer for the decoded topic name.
     */
    char t[128];

    /*
     * Decode the MQTT PUBLISH packet.
     */
    if (mqtt_decode_publish(b, l, &p, t, sizeof t, &fh))
    {
        printf("  [ERROR] decode failed\n");
        free(b);
        fflush(stdout);
        return -1;
    }

    /*
     * Display the received sensor message.
     */
    printf("\n--- MESSAGE ---\n");
    printf("  topic     : %s\n", t);
    printf("  qos       : %u\n", p.qos);
    printf("  packet_id : %u\n", p.packet_id);

    printf("  payload   : ");

    /*
     * Payload is not necessarily null-terminated, so use
     * fwrite() with the exact payload length.
     */
    fwrite(p.payload, 1, p.payload_len, stdout);

    printf("\n");
    fflush(stdout);

    /*
     * The subscriber successfully received the PUBLISH.
     */
    mqtt_print_reason("PUBLISH", MQTT_RC_SUCCESS);

    /*
     * QoS 1 requires the receiver to send PUBACK.
     */
    if (p.qos == 1)
    {
        if (mqtt_send_puback(f,
                             p.packet_id,
                             MQTT_RC_SUCCESS) == 0)
        {
            mqtt_print_reason("PUBACK", MQTT_RC_SUCCESS);
        }
    }

    /*
     * Release the raw MQTT packet buffer.
     */
    free(b);

    return 0;
}

/*
 * Test an invalid PUBLISH request.
 *
 * This manually constructs a PUBLISH packet with:
 *     QoS = 1
 *     RETAIN = 1
 *
 * The broker does not support retained messages and
 * therefore is expected to return reason code 0x83.
 */
static void test_invalid_request(int f, uint16_t pid)
{
    printf("\n--- PUBLISH (invalid request) ---\n");
    printf("  topic     : sensor/temperature\n");
    printf("  payload   : 25.00\n");
    printf("  note      : retain=1 (not supported)\n");
    printf("  packet_id : %u\n", pid);
    fflush(stdout);

    /*
     * Buffer containing the variable header and payload
     * of the manually constructed PUBLISH packet.
     */
    uint8_t body[128];

    size_t pos = 0;

    const char *t = "sensor/temperature";
    size_t n = strlen(t);

    /*
     * Encode the two-byte topic length.
     */
    body[pos++] = 0;
    body[pos++] = (uint8_t)n;

    /*
     * Copy the topic name.
     */
    memcpy(body + pos, t, n);
    pos += n;

    /*
     * Add the two-byte packet identifier required for QoS 1.
     */
    body[pos++] = (uint8_t)(pid >> 8);
    body[pos++] = (uint8_t)pid;

    /*
     * Add the payload.
     */
    body[pos++] = 0;

    memcpy(body + pos, "25.00", 5);
    pos += 5;

    /*
     * Encode the MQTT Remaining Length field.
     */
    uint8_t rl[4];
    size_t rn;

    if (mqtt_encode_remaining_length((uint32_t)pos, rl, &rn))
        return;

    /*
     * 0x33:
     *
     * Packet type = PUBLISH
     * QoS        = 1
     * RETAIN     = 1
     *
     * The RETAIN flag makes this an intentionally
     * unsupported request for this project.
     */
    uint8_t first = 0x33;

    /*
     * Write the complete MQTT packet:
     *     Fixed header
     *     Remaining Length
     *     Variable header + payload
     */
    if (mqtt_write_full(f, &first, 1) ||
        mqtt_write_full(f, rl, rn) ||
        mqtt_write_full(f, body, pos))
    {
        printf("  [ERROR] send failed\n");
        fflush(stdout);
        return;
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
        return;
    }

    uint16_t ap = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    /*
     * Parse the PUBACK if the received packet has
     * the expected packet type.
     */
    if (h == 0x40)
        mqtt_parse_puback(b, l, &ap, &reason);

    free(b);

    mqtt_print_reason("PUBACK", reason);

    if (reason == MQTT_RC_IMPLEMENTATION_SPECIFIC)
        printf("  [ERROR] rejected (Implementation specific error)\n");
    else
        printf("  [ERROR] reason code 0x%02X\n", reason);

    fflush(stdout);
}

/*
 * Test an invalid topic name.
 *
 * The broker is expected to return:
 *     0x90 = Topic Name invalid
 */
static void test_invalid_topic(int f, uint16_t pid)
{
    printf("\n--- PUBLISH (invalid topic) ---\n");
    printf("  topic     : sensor/unknown\n");
    printf("  payload   : 1.23\n");
    printf("  packet_id : %u\n", pid);
    fflush(stdout);

    /*
     * Send a PUBLISH using a topic that is not supported
     * by the broker.
     */
    if (mqtt_send_publish(f,
                          "sensor/unknown",
                          pid,
                          (const uint8_t *)"1.23",
                          4))
    {
        printf("  [ERROR] send failed\n");
        fflush(stdout);
        return;
    }

    mqtt_print_reason("PUBLISH", MQTT_RC_SUCCESS);

    /*
     * Wait for the broker's PUBACK.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read PUBACK\n");
        fflush(stdout);
        return;
    }

    uint16_t ap = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    if (h == 0x40)
        mqtt_parse_puback(b, l, &ap, &reason);

    free(b);

    mqtt_print_reason("PUBACK", reason);

    if (reason == MQTT_RC_TOPIC_NAME_INVALID)
        printf("  [ERROR] rejected (Topic Name invalid)\n");
    else
        printf("  [ERROR] reason code 0x%02X\n", reason);

    fflush(stdout);
}

/*
 * Test an invalid sensor payload.
 *
 * The payload is intentionally non-numeric.
 * The broker is expected to return:
 *     0x99 = Payload format invalid
 */
static void test_invalid_payload(int f, uint16_t pid)
{
    printf("\n--- PUBLISH (invalid payload) ---\n");
    printf("  topic     : sensor/temperature\n");
    printf("  payload   : NOT_A_NUMBER\n");
    printf("  packet_id : %u\n", pid);
    fflush(stdout);

    /*
     * Send intentionally invalid sensor data.
     */
    if (mqtt_send_publish(f,
                          "sensor/temperature",
                          pid,
                          (const uint8_t *)"NOT_A_NUMBER",
                          12))
    {
        printf("  [ERROR] send failed\n");
        fflush(stdout);
        return;
    }

    mqtt_print_reason("PUBLISH", MQTT_RC_SUCCESS);

    /*
     * Wait for the broker's PUBACK.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read PUBACK\n");
        fflush(stdout);
        return;
    }

    uint16_t ap = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    if (h == 0x40)
        mqtt_parse_puback(b, l, &ap, &reason);

    free(b);

    mqtt_print_reason("PUBACK", reason);

    if (reason == MQTT_RC_PAYLOAD_FORMAT_INVALID)
        printf("  [ERROR] rejected (Payload format invalid)\n");
    else
        printf("  [ERROR] reason code 0x%02X\n", reason);

    fflush(stdout);
}

/*
 * Test an unsupported MQTT protocol version.
 *
 * A separate TCP/MQTT connection is created so that the
 * subscriber's main MQTT session remains unaffected.
 *
 * Protocol level 3 represents MQTT 3.1.
 * The broker is expected to reject it with:
 *     0x84 = Unsupported Protocol Version
 */
static void test_unsupported_version(const char *ip)
{
    printf("\n--- CONNECT (unsupported version) ---\n");
    printf("  protocol level : 3 (MQTT 3.1) — broker requires 5\n");
    fflush(stdout);

    /*
     * Create a separate TCP connection for this test.
     */
    int f = tcp_connect(ip);

    if (f < 0)
    {
        perror("  connect");
        return;
    }

    /*
     * Generate a client ID specifically for the
     * unsupported-version test.
     */
    char cid[64];

    snprintf(cid,
             sizeof cid,
             "badver-%ld",
             (long)getpid());

    /*
     * Send a CONNECT packet using protocol level 3.
     */
    if (mqtt_send_connect_version(f, cid, 3))
    {
        printf("  [ERROR] failed to send CONNECT\n");
        fflush(stdout);
        close(f);
        return;
    }

    mqtt_print_reason("CONNECT", MQTT_RC_SUCCESS);

    /*
     * Read the broker's CONNACK response.
     */
    uint8_t h, *b = NULL;
    uint32_t l;

    if (mqtt_read_packet(f, &h, &b, &l))
    {
        printf("  [ERROR] failed to read CONNACK\n");
        fflush(stdout);
        close(f);
        return;
    }

    uint8_t session = 0;
    uint8_t reason = MQTT_RC_UNSPECIFIED_ERROR;

    if (h == 0x20)
        mqtt_parse_connack(b, l, &session, &reason);

    free(b);

    mqtt_print_reason("CONNACK", reason);

    if (reason == MQTT_RC_UNSUPPORTED_PROTOCOL_VER)
        printf("  [ERROR] rejected (Unsupported Protocol Version)\n");
    else
        printf("  [ERROR] unexpected reason code\n");

    fflush(stdout);

    /*
     * Close the temporary test connection.
     */
    close(f);
}

/*
 * Display the subscriber's menu.
 */
static void print_menu(void)
{
    printf("\n========== MQTT SUBSCRIBER MENU ==========\n");
    printf("  1.  Request Temperature\n");
    printf("  2.  Request Humidity\n");
    printf("  3.  Request Temperature & Humidity\n");
    printf("  4.  Invalid Request          (0x83)\n");
    printf("  5.  Invalid Topic             (0x90)\n");
    printf("  6.  Invalid Topic Filter      (0x8F)\n");
    printf("  7.  Unsupported Version       (0x84)\n");
    printf("  8.  Invalid Payload           (0x99)\n");
    printf("  9.  Disconnect\n");
    printf("  10. Exit\n");
    printf("==========================================\n");
    printf("Select: ");

    fflush(stdout);
}

/*
 * Send a normal MQTT DISCONNECT and close the
 * subscriber's TCP connection.
 */
static void do_disconnect_and_close(void)
{
    /*
     * Send DISCONNECT only when an active MQTT
     * connection exists.
     */
    if (g_connected && g_fd >= 0)
    {
        mqtt_send_disconnect(g_fd, MQTT_RC_NORMAL_DISCONNECTION);
        mqtt_print_reason("DISCONNECT", MQTT_RC_NORMAL_DISCONNECTION);
        g_connected = 0;
    }

    /*
     * Close the underlying TCP connection.
     */
    if (g_fd >= 0)
    {
        shutdown(g_fd, SHUT_RDWR);
        close(g_fd);
        g_fd = -1;
    }
}

int main(int argc, char **argv)
{
    /*
     * Use the broker IP supplied through the command line.
     * If no address is supplied, use localhost.
     */
    g_broker_ip = argc > 1 ? argv[1] : "127.0.0.1";

    /*
     * Register handlers for graceful termination.
     */
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /*
     * Ignore SIGPIPE so that a closed broker connection
     * does not terminate the subscriber unexpectedly.
     */
    signal(SIGPIPE, SIG_IGN);

    /*
     * Register cleanup for normal program termination.
     */
    atexit(subscriber_cleanup);

    printf("========== MQTT SUBSCRIBER ==========\n");
    printf("Broker : %s:%d\n", g_broker_ip, MQTT_PORT);
    printf("=====================================\n");
    fflush(stdout);

    /*
     * Establish the TCP connection with the broker.
     */
    g_fd = tcp_connect(g_broker_ip);

    if (g_fd < 0)
    {
        perror("subscriber connect");
        return 1;
    }

    /*
     * Generate a unique subscriber client ID using
     * the process ID.
     */
    char cid[64];

    snprintf(cid,
             sizeof cid,
             "subscriber-%ld",
             (long)getpid());

    /*
     * Perform the MQTT CONNECT / CONNACK handshake.
     */
    if (do_connect(g_fd, cid))
        return 1;

    /*
     * The MQTT session is now active.
     */
    g_connected = 1;

    /*
     * Initial packet identifier used for SUBSCRIBE
     * and test PUBLISH packets.
     */
    uint16_t pid = 1;

    /*
     * Display the subscriber menu.
     */
    print_menu();

    char line[32];

    /*
     * Read the user's menu selection.
     */
    if (!fgets(line, sizeof line, stdin))
    {
        printf("\n[SUBSCRIBER] done\n");
        return 0;
    }

    int choice = atoi(line);

    /*
     * Option 1:
     * Subscribe to temperature data and wait for one
     * corresponding PUBLISH message.
     */
    if (choice == 1)
    {
        if (do_subscribe(g_fd,
                         pid++,
                         "sensor/temperature",
                         1) <= MQTT_RC_GRANTED_QOS_2)
        {
            /*
             * Wait up to 20 seconds for a temperature message.
             */
            if (receive_one(g_fd, 20) != 0)
            {
                /*
                 * No valid data was received, for example
                 * because the publisher sent invalid data.
                 */
                printf("[SUBSCRIBER] terminating (no valid data received)\n");
            }
        }
    }

    /*
     * Option 2:
     * Subscribe to humidity data and wait for one message.
     */
    else if (choice == 2)
    {
        if (do_subscribe(g_fd,
                         pid++,
                         "sensor/humidity",
                         1) <= MQTT_RC_GRANTED_QOS_2)
        {
            if (receive_one(g_fd, 20) != 0)
                printf("[SUBSCRIBER] terminating (no valid data received)\n");
        }
    }

    /*
     * Option 3:
     * Subscribe to both temperature and humidity topics.
     */
    else if (choice == 3)
    {
        /*
         * Create separate subscriptions for the two topics.
         */
        uint8_t r1 = do_subscribe(g_fd,
                                  pid++,
                                  "sensor/temperature",
                                  1);

        uint8_t r2 = do_subscribe(g_fd,
                                  pid++,
                                  "sensor/humidity",
                                  1);

        /*
         * Only wait for sensor messages if both
         * subscriptions were granted.
         */
        if (r1 <= MQTT_RC_GRANTED_QOS_2 &&
            r2 <= MQTT_RC_GRANTED_QOS_2)
        {
            /*
             * Wait for one temperature and one humidity message.
             */
            int ok1 = receive_one(g_fd, 20);
            int ok2 = receive_one(g_fd, 20);

            if (ok1 != 0 || ok2 != 0)
                printf("[SUBSCRIBER] terminating (no valid data received)\n");
        }
    }

    /*
     * Option 4:
     * Send a PUBLISH with an unsupported request/flag
     * combination and verify the broker's reason code.
     */
    else if (choice == 4)
    {
        test_invalid_request(g_fd, pid++);
    }

    /*
     * Option 5:
     * Publish to an invalid topic name.
     */
    else if (choice == 5)
    {
        test_invalid_topic(g_fd, pid++);
    }

    /*
     * Option 6:
     * Send an invalid topic filter.
     *
     * The broker is expected to return reason code 0x8F.
     */
    else if (choice == 6)
    {
        /* Topic Filter invalid 0x8F */
        (void)do_subscribe(g_fd,
                           pid++,
                           "sensor/#/invalid",
                           1);
    }

    /*
     * Option 7:
     * Test an unsupported MQTT protocol version.
     *
     * A separate connection is used so the main
     * subscriber session remains open.
     */
    else if (choice == 7)
    {
        /* Dedicated connection for unsupported version; main session stays open */
        test_unsupported_version(g_broker_ip);
    }

    /*
     * Option 8:
     * Send a non-numeric payload to a sensor topic.
     */
    else if (choice == 8)
    {
        test_invalid_payload(g_fd, pid++);
    }

    /*
     * Option 9:
     * Immediately disconnect from the broker.
     */
    else if (choice == 9)
    {
        printf("\n--- DISCONNECT ---\n");
        fflush(stdout);

        do_disconnect_and_close();

        printf("[SUBSCRIBER] disconnected from broker\n");
        fflush(stdout);

        return 0;
    }

    /*
     * Option 10:
     * Exit normally without performing another operation.
     */
    else if (choice == 10)
    {
        /* Exit */
    }

    /*
     * Handle an invalid menu selection.
     */
    else
    {
        printf("Invalid selection. Enter 1-10.\n");
        fflush(stdout);
    }

    /*
     * Perform normal MQTT disconnection after completing
     * the selected operation.
     */
    printf("\n--- DISCONNECT ---\n");
    fflush(stdout);

    do_disconnect_and_close();

    printf("[SUBSCRIBER] done\n");
    fflush(stdout);

    return 0;
}