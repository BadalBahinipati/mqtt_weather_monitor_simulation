#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#include "broker.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static pthread_t accept_tid;
static int accept_started;
static int dashboard_ready;

/*
 * Broker dashboard (rows 1-7 pinned once).
 * Shows: status, publishers, subscribers, temperature, humidity.
 * Live fields are rewritten in place; the frame is never appended again.
 */
enum
{
    DASH_ROW_TITLE = 1,
    DASH_ROW_STATUS = 2,
    DASH_ROW_PUBS = 3,
    DASH_ROW_SUBS = 4,
    DASH_ROW_TEMP = 5,
    DASH_ROW_HUM = 6,
    DASH_ROW_FOOTER = 7,
    DASH_LOG_ROW = 8
};

static void dashboard_put_line(int row, const char *text)
{
    printf("\033[%d;1H\033[2K%s", row, text);
}

static void dashboard_render_fields_unlocked(void)
{
    char line[96];

    snprintf(line, sizeof line, "Broker status  : %s",
             g_broker.broker_running ? "RUNNING" : "STOPPED");
    dashboard_put_line(DASH_ROW_STATUS, line);

    snprintf(line, sizeof line, "Publishers     : %d", g_broker.connected_publishers);
    dashboard_put_line(DASH_ROW_PUBS, line);

    snprintf(line, sizeof line, "Subscribers    : %d", g_broker.connected_subscribers);
    dashboard_put_line(DASH_ROW_SUBS, line);

    if (g_broker.temperature_received)
        snprintf(line, sizeof line, "Temperature    : %.2f C", g_broker.temperature);
    else
        snprintf(line, sizeof line, "Temperature    : --");
    dashboard_put_line(DASH_ROW_TEMP, line);

    if (g_broker.humidity_received)
        snprintf(line, sizeof line, "Humidity       : %.2f %%", g_broker.humidity);
    else
        snprintf(line, sizeof line, "Humidity       : --");
    dashboard_put_line(DASH_ROW_HUM, line);
}

static int dashboard_term_rows(void)
{
    int rows = 24;
#ifdef TIOCGWINSZ
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_row >= DASH_LOG_ROW + 3)
        rows = ws.ws_row;
#endif
    return rows;
}

static void dashboard_print_plain_unlocked(void)
{
    printf("\n========== MQTT BROKER ==========\n");
    printf("Broker status  : %s\n", g_broker.broker_running ? "RUNNING" : "STOPPED");
    printf("Publishers     : %d\n", g_broker.connected_publishers);
    printf("Subscribers    : %d\n", g_broker.connected_subscribers);
    if (g_broker.temperature_received)
        printf("Temperature    : %.2f C\n", g_broker.temperature);
    else
        printf("Temperature    : --\n");
    if (g_broker.humidity_received)
        printf("Humidity       : %.2f %%\n", g_broker.humidity);
    else
        printf("Humidity       : --\n");
    printf("=================================\n");
}

void broker_display_dashboard(void)
{
    pthread_mutex_lock(&g_broker.mutex);

    if (!isatty(STDOUT_FILENO))
    {
        /* Non-TTY: print status block only once to avoid append spam. */
        if (!dashboard_ready)
        {
            dashboard_print_plain_unlocked();
            dashboard_ready = 1;
            fflush(stdout);
        }
        pthread_mutex_unlock(&g_broker.mutex);
        return;
    }

    if (!dashboard_ready)
    {
        printf("\033[H");
        dashboard_put_line(DASH_ROW_TITLE, "========== MQTT BROKER ==========");
        dashboard_render_fields_unlocked();
        dashboard_put_line(DASH_ROW_FOOTER, "=================================");
        printf("\033[%d;%dr", DASH_LOG_ROW, dashboard_term_rows());
        printf("\033[%d;1H", DASH_LOG_ROW);
        dashboard_ready = 1;
        fflush(stdout);
        pthread_mutex_unlock(&g_broker.mutex);
        return;
    }

    printf("\0337");
    dashboard_render_fields_unlocked();
    printf("\0338");
    fflush(stdout);
    pthread_mutex_unlock(&g_broker.mutex);
}

void broker_dashboard_shutdown(void)
{
    if (!dashboard_ready)
        return;
    if (isatty(STDOUT_FILENO))
    {
        printf("\033[r");
        printf("\n");
        fflush(stdout);
    }
    dashboard_ready = 0;
}

static int start_listener(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        return -1;
    }
    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons(MQTT_PORT);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) < 0)
    {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 16) < 0)
    {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

void *broker_accept_thread(void *arg)
{
    (void)arg;
    for (;;)
    {
        pthread_mutex_lock(&g_broker.mutex);
        int running = g_broker.broker_running, lfd = g_broker.listen_fd;
        pthread_mutex_unlock(&g_broker.mutex);
        if (!running || lfd < 0)
            break;
        struct sockaddr_in ca;
        socklen_t cl = sizeof ca;
        int fd = accept(lfd, (struct sockaddr *)&ca, &cl);
        if (fd < 0)
        {
            if (errno == EINTR)
                continue;
            pthread_mutex_lock(&g_broker.mutex);
            running = g_broker.broker_running;
            pthread_mutex_unlock(&g_broker.mutex);
            if (!running)
                break;
            perror("accept");
            continue;
        }
        pthread_mutex_lock(&g_broker.mutex);
        int idx = broker_find_free_client_locked();
        if (idx < 0)
        {
            pthread_mutex_unlock(&g_broker.mutex);
            close(fd);
            continue;
        }
        MQTTClient *c = &g_broker.clients[idx];
        memset(c->client_id, 0, sizeof c->client_id);
        c->socket_fd = fd;
        c->connected = 1;
        c->mqtt_connected = 0;
        c->is_subscriber = 0;
        c->is_publisher = 0;
        c->subscription_active = 0;
        c->subscription_attempted = 0;
        c->subscribed_temperature = 0;
        c->subscribed_humidity = 0;
        c->next_server_pid = 100;
        c->pending_pid = 0;
        c->pending_ack = 0;
        c->stopping = 0;
        c->thread_started = 1;
        if (pthread_create(&c->thread, NULL, broker_client_thread, c) != 0)
        {
            c->thread_started = 0;
            c->connected = 0;
            c->socket_fd = -1;
            pthread_mutex_unlock(&g_broker.mutex);
            close(fd);
            continue;
        }
        pthread_mutex_unlock(&g_broker.mutex);
    }
    return NULL;
}

static int start_broker(void)
{
    pthread_mutex_lock(&g_broker.mutex);
    if (g_broker.broker_running)
    {
        pthread_mutex_unlock(&g_broker.mutex);
        return 0;
    }
    pthread_mutex_unlock(&g_broker.mutex);
    int fd = start_listener();
    if (fd < 0)
    {
        printf("Failed to start broker on port %d.\n", MQTT_PORT);
        fflush(stdout);
        return -1;
    }
    broker_state_reset_for_start();
    pthread_mutex_lock(&g_broker.mutex);
    g_broker.listen_fd = fd;
    g_broker.broker_running = 1;
    pthread_mutex_unlock(&g_broker.mutex);
    if (pthread_create(&accept_tid, NULL, broker_accept_thread, NULL) != 0)
    {
        perror("pthread_create");
        close(fd);
        pthread_mutex_lock(&g_broker.mutex);
        g_broker.listen_fd = -1;
        g_broker.broker_running = 0;
        pthread_mutex_unlock(&g_broker.mutex);
        return -1;
    }
    accept_started = 1;
    broker_display_dashboard();
    printf("[BROKER] listening on port %d\n", MQTT_PORT);
    fflush(stdout);
    return 0;
}

static void stop_broker(void)
{
    pthread_mutex_lock(&g_broker.mutex);
    int was = g_broker.broker_running;
    g_broker.broker_running = 0;
    int fd = g_broker.listen_fd;
    g_broker.listen_fd = -1;
    pthread_mutex_unlock(&g_broker.mutex);
    if (!was)
        return;
    if (fd >= 0)
    {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
    if (accept_started)
    {
        pthread_join(accept_tid, NULL);
        accept_started = 0;
    }
    broker_stop_clients();
    broker_cleanup_threads();
    pthread_mutex_lock(&g_broker.mutex);
    g_broker.connected_clients = 0;
    g_broker.connected_subscribers = 0;
    g_broker.connected_publishers = 0;
    g_broker.temperature = 0;
    g_broker.humidity = 0;
    g_broker.temperature_received = 0;
    g_broker.humidity_received = 0;
    pthread_mutex_unlock(&g_broker.mutex);
    broker_display_dashboard();
    printf("[BROKER] stopped\n");
    fflush(stdout);
}

static volatile sig_atomic_t g_exit_requested;

static void on_signal(int sig)
{
    (void)sig;
    g_exit_requested = 1;
    pthread_mutex_lock(&g_broker.mutex);
    int fd = g_broker.listen_fd;
    g_broker.broker_running = 0;
    g_broker.listen_fd = -1;
    pthread_mutex_unlock(&g_broker.mutex);
    if (fd >= 0)
    {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    broker_state_init();
    broker_display_dashboard();

    for (;;)
    {
        if (g_exit_requested)
            break;
        printf("\n1. %s broker\n2. Exit\nSelect: ",
               g_broker.broker_running ? "Stop" : "Start");
        fflush(stdout);
        char line[32];
        if (!fgets(line, sizeof line, stdin))
            break;
        if (g_exit_requested)
            break;
        int ch = atoi(line);
        if (ch == 1)
        {
            pthread_mutex_lock(&g_broker.mutex);
            int running = g_broker.broker_running;
            pthread_mutex_unlock(&g_broker.mutex);
            if (running)
                stop_broker();
            else
                (void)start_broker();
        }
        else if (ch == 2)
        {
            stop_broker();
            break;
        }
        else
            printf("Invalid selection. Enter 1 or 2.\n");
    }
    stop_broker();
    broker_dashboard_shutdown();
    for (int i = 0; i < BROKER_MAX_CLIENTS; i++)
        pthread_cond_destroy(&g_broker.clients[i].ack_cond);
    pthread_mutex_destroy(&g_broker.mutex);
    return 0;
}
