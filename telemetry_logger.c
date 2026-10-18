/*
 * telemetry_logger.c
 * Real-time multithreaded telemetry logger for Bluesky Jetstream.
 *
 * Thread topology
 *   T1 Producer  – libwebsockets event loop, pushes raw JSON into circular buffer
 *   T2 Consumer  – wakes on cond-var, parses JSON, increments kind counters
 *   T3 Logger    – fires every 1 s via clock_nanosleep TIMER_ABSTIME,
 *                  snapshots counters + CPU + buffer fill → metrics_log.txt
 *
 * Build:  see Makefile
 * Deps:   libwebsockets (≥3.x), cJSON
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <libwebsockets.h>
#include <cjson/cJSON.h>

/* Configuration */

#define BUFFER_SLOTS    256             /* circular buffer capacity              */
#define MAX_JSON_LEN    32768           /* max bytes per WebSocket message       */
#define LOG_FILE        "metrics_log.txt"
#define WS_HOST         "jetstream1.us-east.bsky.network"
#define WS_PATH         "/subscribe?wantedCollections=app.bsky.feed.post"
#define WS_PORT         443
#define RECONNECT_DELAY 3               /* seconds between reconnect attempts    */

/* Circular Buffer */

typedef struct {
    char   data[MAX_JSON_LEN];
    size_t len;
} Slot;

typedef struct {
    Slot            slots[BUFFER_SLOTS];
    int             head;           /* next write index      */
    int             tail;           /* next read index       */
    int             count;          /* items currently held  */
    long long       dropped;        /* frames lost (buf full)*/
    pthread_mutex_t mtx;
    pthread_cond_t  not_empty;
} CircularBuffer;

/* Per-kind counters */

typedef struct {
    long            commit;
    long            identity;
    long            account;
    long            info;           /* catch-all for unknown kinds */
    pthread_mutex_t mtx;
} Counters;

/* CPU snapshot (for /proc/stat delta method) */

typedef struct {
    unsigned long long total;
    unsigned long long idle;
} CpuSnap;

/* Global state */

static CircularBuffer  g_buf;
static Counters        g_cnt;
static CpuSnap         g_cpu_prev;
static volatile sig_atomic_t g_running = 1;

/* lws handles - written only from the producer thread */
static struct lws_context *g_lws_ctx = NULL;
static struct lws         *g_wsi     = NULL;

/* fragment reassembly buffer – only touched inside lws callback (producer thread) */
static char   g_frag[MAX_JSON_LEN];
static size_t g_frag_len = 0;

/* Signal handler */

static void sig_handler(int sig)
{
    (void)sig;
    g_running = 0;
}

/* Buffer helpers */

static void buf_init(CircularBuffer *b)
{
    memset(b, 0, sizeof *b);
    pthread_mutex_init(&b->mtx, NULL);
    pthread_cond_init(&b->not_empty, NULL);
}

/*
 * Called from the WebSocket callback (producer thread, no mutex held).
 * Non-blocking: drops message silently if buffer is full.
 */
static void buf_push(CircularBuffer *b, const char *data, size_t len)
{
    if (len > MAX_JSON_LEN - 1) len = MAX_JSON_LEN - 1;

    pthread_mutex_lock(&b->mtx);

    if (b->count >= BUFFER_SLOTS) {
        b->dropped++;
        pthread_mutex_unlock(&b->mtx);
        return;
    }

    Slot *s = &b->slots[b->head];
    memcpy(s->data, data, len);
    s->data[len] = '\0';
    s->len = len;

    b->head = (b->head + 1) % BUFFER_SLOTS;
    b->count++;

    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mtx);
}

/*
 * Called from the consumer thread.
 * Blocks until data is available or g_running becomes 0.
 * Returns 0 on success, -1 on shutdown.
 */
static int buf_pop(CircularBuffer *b, char *out, size_t *out_len)
{
    pthread_mutex_lock(&b->mtx);

    while (b->count == 0 && g_running)
        pthread_cond_wait(&b->not_empty, &b->mtx);

    if (b->count == 0) {            /* shutdown path */
        pthread_mutex_unlock(&b->mtx);
        return -1;
    }

    const Slot *s = &b->slots[b->tail];
    *out_len = s->len;
    memcpy(out, s->data, s->len + 1);   /* +1 for NUL */

    b->tail = (b->tail + 1) % BUFFER_SLOTS;
    b->count--;

    pthread_mutex_unlock(&b->mtx);
    return 0;
}

/* Returns current occupancy count (briefly locks buffer). */
static int buf_count(CircularBuffer *b)
{
    pthread_mutex_lock(&b->mtx);
    int c = b->count;
    pthread_mutex_unlock(&b->mtx);
    return c;
}

/* CPU measurement */

static void cpu_read(CpuSnap *s)
{
    FILE *f = fopen("/proc/stat", "r");
    if (!f) { s->total = s->idle = 0; return; }

    unsigned long long u, n, sys, id, io, irq, sirq, steal;
    /* format: cpu  user nice system idle iowait irq softirq steal … */
    if (fscanf(f, "cpu  %llu %llu %llu %llu %llu %llu %llu %llu",
               &u, &n, &sys, &id, &io, &irq, &sirq, &steal) == 8) {
        s->total = u + n + sys + id + io + irq + sirq + steal;
        s->idle  = id + io;     /* idle + iowait */
    } else {
        s->total = s->idle = 0;
    }
    fclose(f);
}

static double cpu_pct(const CpuSnap *prev, const CpuSnap *curr)
{
    unsigned long long total_d = curr->total - prev->total;
    unsigned long long idle_d  = curr->idle  - prev->idle;
    if (total_d == 0) return 0.0;
    return (1.0 - (double)idle_d / (double)total_d) * 100.0;
}

/* WebSocket callback (runs in producer thread) */

static int ws_callback(struct lws *wsi, enum lws_callback_reasons reason,
                       void *user, void *in, size_t len)
{
    (void)user;

    switch (reason) {

    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        g_frag_len = 0;
        break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
        if (!in || len == 0) break;

        /* Accumulate fragments. */
        if (g_frag_len + len < MAX_JSON_LEN) {
            memcpy(g_frag + g_frag_len, in, len);
            g_frag_len += len;
        } else {
            /* oversized: reset and discard this whole message */
            g_frag_len = 0;
            break;
        }

        if (lws_is_final_fragment(wsi)) {
            buf_push(&g_buf, g_frag, g_frag_len);
            g_frag_len = 0;
        }
        break;

    case LWS_CALLBACK_CLIENT_CLOSED:
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        g_wsi = NULL;
        break;

    default:
        break;
    }
    return 0;
}

static struct lws_protocols g_protocols[] = {
    {
        .name                  = "jetstream",
        .callback              = ws_callback,
        .per_session_data_size = 0,
        .rx_buffer_size        = MAX_JSON_LEN,
    },
    LWS_PROTOCOL_LIST_TERM
};

/* Thread 1: Producer */

static void *producer_thread(void *arg)
{
    (void)arg;

    struct lws_context_creation_info ctx_info;
    memset(&ctx_info, 0, sizeof ctx_info);
    ctx_info.port      = CONTEXT_PORT_NO_LISTEN;
    ctx_info.protocols = g_protocols;
    ctx_info.options   = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    ctx_info.gid       = -1;
    ctx_info.uid       = -1;

    g_lws_ctx = lws_create_context(&ctx_info);
    if (!g_lws_ctx) {
        fprintf(stderr, "[producer] lws_create_context failed\n");
        g_running = 0;
        return NULL;
    }

    /* Connection info – kept alive for reconnects */
    struct lws_client_connect_info conn;
    memset(&conn, 0, sizeof conn);
    conn.context        = g_lws_ctx;
    conn.address        = WS_HOST;
    conn.port           = WS_PORT;
    conn.path           = WS_PATH;
    conn.host           = WS_HOST;
    conn.origin         = WS_HOST;
    conn.protocol       = g_protocols[0].name;
    conn.ssl_connection = LCCSCF_USE_SSL;

    g_wsi = lws_client_connect_via_info(&conn);
    if (!g_wsi)
        fprintf(stderr, "[producer] initial connect failed, will retry\n");

    while (g_running) {
        lws_service(g_lws_ctx, 50);   /* 50 ms poll timeout */

        if (!g_wsi && g_running) {
            /* Reconnect after brief back-off */
            sleep(RECONNECT_DELAY);
            fprintf(stderr, "[producer] reconnecting …\n");
            g_wsi = lws_client_connect_via_info(&conn);
        }
    }

    lws_context_destroy(g_lws_ctx);
    g_lws_ctx = NULL;

    /* Wake any sleeping consumer so it can drain and exit */
    pthread_mutex_lock(&g_buf.mtx);
    pthread_cond_broadcast(&g_buf.not_empty);
    pthread_mutex_unlock(&g_buf.mtx);

    return NULL;
}

/* Thread 2: Consumer */

static void *consumer_thread(void *arg)
{
    (void)arg;

    /* Static-sized local buffer avoids per-message malloc */
    static char json_buf[MAX_JSON_LEN + 1];
    size_t      len;

    while (g_running || buf_count(&g_buf) > 0) {
        if (buf_pop(&g_buf, json_buf, &len) != 0)
            break;

        cJSON *root = cJSON_ParseWithLength(json_buf, (int)len);
        if (!root) continue;

        const cJSON *kind_item =
            cJSON_GetObjectItemCaseSensitive(root, "kind");

        if (cJSON_IsString(kind_item) && kind_item->valuestring) {
            const char *k = kind_item->valuestring;

            pthread_mutex_lock(&g_cnt.mtx);
            if      (strcmp(k, "commit")   == 0) g_cnt.commit++;
            else if (strcmp(k, "identity") == 0) g_cnt.identity++;
            else if (strcmp(k, "account")  == 0) g_cnt.account++;
            else                                  g_cnt.info++;
            pthread_mutex_unlock(&g_cnt.mtx);
        }

        cJSON_Delete(root);
    }
    return NULL;
}

/* Thread 3: Logger */

static void *logger_thread(void *arg)
{
    (void)arg;

    /* Open or create log file; write CSV header only if file is new / empty */
    int needs_header = (access(LOG_FILE, F_OK) != 0);
    FILE *log = fopen(LOG_FILE, "a");
    if (!log) {
        perror("[logger] fopen " LOG_FILE);
        g_running = 0;
        return NULL;
    }
    if (needs_header) {
        fprintf(log,
            "Seconds,Nanoseconds,"
            "Commit_Count,Identity_Count,Account_Count,Info_Count,"
            "Buffer_Occupancy_Pct,CPU_Pct\n");
        fflush(log);
    }

    /* Seed CPU snapshot */
    cpu_read(&g_cpu_prev);

    /*
     * Compute first absolute deadline: next whole second on CLOCK_MONOTONIC.
     * Advancing by exactly 1 s each tick avoids cumulative drift.
     */
    struct timespec next_wake;
    clock_gettime(CLOCK_MONOTONIC, &next_wake);
    next_wake.tv_nsec = 0;
    next_wake.tv_sec += 1;

    while (g_running) {
        int ret = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME,
                                  &next_wake, NULL);
        if (ret == EINTR && !g_running)
            break;

        /* Capture wall-clock timestamp for the CSV */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);

        /* Snapshot + reset counters (atomic from logger's perspective) */
        long commit, identity, account, info;
        pthread_mutex_lock(&g_cnt.mtx);
        commit   = g_cnt.commit;   g_cnt.commit   = 0;
        identity = g_cnt.identity; g_cnt.identity = 0;
        account  = g_cnt.account;  g_cnt.account  = 0;
        info     = g_cnt.info;     g_cnt.info      = 0;
        pthread_mutex_unlock(&g_cnt.mtx);

        /* Buffer occupancy */
        double buf_occ = (double)buf_count(&g_buf) / BUFFER_SLOTS * 100.0;

        /* CPU */
        CpuSnap cpu_now;
        cpu_read(&cpu_now);
        double c_pct = cpu_pct(&g_cpu_prev, &cpu_now);
        g_cpu_prev = cpu_now;

        /* Append CSV row */
        fprintf(log, "%ld,%ld,%ld,%ld,%ld,%ld,%.2f,%.2f\n",
                (long)ts.tv_sec, (long)ts.tv_nsec,
                commit, identity, account, info,
                buf_occ, c_pct);
        fflush(log);

        /* Advance deadline by exactly 1 s - zero drift by design */
        next_wake.tv_sec += 1;
    }

    fclose(log);
    return NULL;
}

/* main */

int main(void)
{
    /* Suppress noisy libwebsockets logs (set to LLL_ERR|LLL_WARN for debug) */
    lws_set_log_level(LLL_ERR, NULL);

    /* Clean shutdown on Ctrl-C / SIGTERM */
    struct sigaction sa = { .sa_handler = sig_handler };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Initialise shared state */
    buf_init(&g_buf);
    memset(&g_cnt, 0, sizeof g_cnt);
    pthread_mutex_init(&g_cnt.mtx, NULL);

    pthread_t t_prod, t_cons, t_log;
    pthread_create(&t_prod, NULL, producer_thread, NULL);
    pthread_create(&t_cons, NULL, consumer_thread, NULL);
    pthread_create(&t_log,  NULL, logger_thread,   NULL);

    pthread_join(t_prod, NULL);
    pthread_join(t_cons, NULL);
    pthread_join(t_log,  NULL);

    pthread_mutex_destroy(&g_cnt.mtx);
    pthread_mutex_destroy(&g_buf.mtx);
    pthread_cond_destroy(&g_buf.not_empty);

    fprintf(stderr, "Exiting cleanly. Dropped frames: %lld\n", g_buf.dropped);
    return 0;
}
