#include "persist_worker.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "esp_heap_caps.h"
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "nvs.h"

#define PERSIST_QUEUE_DEPTH 8
#define PERSIST_WORKER_STACK 4096
#define PERSIST_WORKER_PRIORITY 2

static const char *TAG = "persist_worker";

#define PERSIST_NVS_NAMESPACE "macerun"
#define PERSIST_WORLD_KEY "world_deltas"

typedef struct
{
    bool in_use;
    uint32_t id;
    bool is_world;
    char key[16];
    uint8_t *data;
    size_t len;
} persist_job_t;

static persist_job_t s_jobs[PERSIST_QUEUE_DEPTH];
static SemaphoreHandle_t s_mutex;
static TaskHandle_t s_task;
static uint32_t s_next_id = 1;
static uint32_t s_committed_id = 0;
static uint32_t s_coalesce_count = 0;
static uint32_t s_failure_count = 0;

static uint8_t *alloc_copy(const uint8_t *data, size_t len)
{
    uint8_t *copy = (uint8_t *)heap_caps_malloc(len ? len : 1, MALLOC_CAP_SPIRAM);
    if (copy == NULL)
    {
        copy = (uint8_t *)malloc(len ? len : 1);
    }
    if (copy != NULL && len > 0)
    {
        memcpy(copy, data, len);
    }
    return copy;
}

static uint32_t submit(bool is_world, const char *key, const uint8_t *data, size_t len)
{
    if (s_mutex == NULL || (len > 0 && data == NULL) || (key == NULL && !is_world))
    {
        return 0;
    }

    xSemaphoreTake(s_mutex, portMAX_DELAY);

    /* Coalesce into an already-queued job of the same kind/key so a burst of
     * edits produces one NVS commit, not one per edit. */
    for (size_t i = 0; i < PERSIST_QUEUE_DEPTH; i++)
    {
        persist_job_t *job = &s_jobs[i];
        if (!job->in_use || job->is_world != is_world)
        {
            continue;
        }
        if (!is_world && strcmp(job->key, key) != 0)
        {
            continue;
        }

        uint8_t *copy = alloc_copy(data, len);
        if (copy == NULL)
        {
            s_failure_count++;
            xSemaphoreGive(s_mutex);
            return 0;
        }
        free(job->data);
        job->data = copy;
        job->len = len;
        s_coalesce_count++;
        uint32_t id = job->id;
        xSemaphoreGive(s_mutex);
        return id;
    }

    for (size_t i = 0; i < PERSIST_QUEUE_DEPTH; i++)
    {
        persist_job_t *job = &s_jobs[i];
        if (job->in_use)
        {
            continue;
        }

        uint8_t *copy = alloc_copy(data, len);
        if (copy == NULL)
        {
            s_failure_count++;
            xSemaphoreGive(s_mutex);
            return 0;
        }
        job->in_use = true;
        job->id = s_next_id++;
        job->is_world = is_world;
        job->len = len;
        job->data = copy;
        if (!is_world)
        {
            snprintf(job->key, sizeof(job->key), "%s", key);
        }
        else
        {
            job->key[0] = '\0';
        }
        uint32_t id = job->id;
        xSemaphoreGive(s_mutex);
        if (s_task != NULL)
        {
            xTaskNotifyGive(s_task);
        }
        return id;
    }

    s_failure_count++;
    xSemaphoreGive(s_mutex);
    return 0;
}

uint32_t persist_world_deltas_submit(const void *entries, size_t count, size_t entry_size)
{
    return submit(true, NULL, (const uint8_t *)entries, count * entry_size);
}

uint32_t persist_player_data_submit(const char *nvs_key,
                                    const uint8_t *blob,
                                    size_t blob_len)
{
    return submit(false, nvs_key, blob, blob_len);
}

bool persist_job_committed(uint32_t job_id)
{
    if (job_id == 0)
    {
        return true;
    }
    xSemaphoreTake(s_mutex, portMAX_DELAY);
    bool done = s_committed_id >= job_id;
    xSemaphoreGive(s_mutex);
    return done;
}

uint32_t persist_coalesce_count(void)
{
    return s_coalesce_count;
}

uint32_t persist_failure_count(void)
{
    return s_failure_count;
}

static esp_err_t write_blob(const char *key, const uint8_t *data, size_t len)
{
    nvs_handle_t handle = 0;
    esp_err_t err = nvs_open(PERSIST_NVS_NAMESPACE, NVS_READWRITE, &handle);
    if (err != ESP_OK)
    {
        return err;
    }

    if (len == 0)
    {
        err = nvs_erase_key(handle, key);
        if (err == ESP_ERR_NVS_NOT_FOUND)
        {
            err = ESP_OK;
        }
    }
    else
    {
        err = nvs_set_blob(handle, key, data, len);
    }

    if (err == ESP_OK)
    {
        err = nvs_commit(handle);
    }
    nvs_close(handle);
    return err;
}

static void worker_task(void *arg)
{
    (void)arg;

    for (;;)
    {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

        for (;;)
        {
            persist_job_t local;
            bool found = false;

            xSemaphoreTake(s_mutex, portMAX_DELAY);
            for (size_t i = 0; i < PERSIST_QUEUE_DEPTH; i++)
            {
                if (s_jobs[i].in_use)
                {
                    local = s_jobs[i];
                    memset(&s_jobs[i], 0, sizeof(s_jobs[i]));
                    found = true;
                    break;
                }
            }
            xSemaphoreGive(s_mutex);

            if (!found)
            {
                break;
            }

            const char *key = local.is_world ? PERSIST_WORLD_KEY : local.key;
            esp_err_t err = write_blob(key, local.data, local.len);
            if (err != ESP_OK)
            {
                ESP_LOGW(TAG, "commit failed for %s: %s", key, esp_err_to_name(err));
                xSemaphoreTake(s_mutex, portMAX_DELAY);
                s_failure_count++;
                xSemaphoreGive(s_mutex);
            }

            free(local.data);
            xSemaphoreTake(s_mutex, portMAX_DELAY);
            if (s_committed_id < local.id)
            {
                s_committed_id = local.id;
            }
            xSemaphoreGive(s_mutex);
        }
    }
}

esp_err_t persist_worker_init(void)
{
    memset(s_jobs, 0, sizeof(s_jobs));
    s_mutex = xSemaphoreCreateMutex();
    if (s_mutex == NULL)
    {
        return ESP_ERR_NO_MEM;
    }

    BaseType_t ok = xTaskCreate(worker_task,
                                "persist",
                                PERSIST_WORKER_STACK,
                                NULL,
                                PERSIST_WORKER_PRIORITY,
                                &s_task);
    if (ok != pdPASS)
    {
        vSemaphoreDelete(s_mutex);
        s_mutex = NULL;
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}
