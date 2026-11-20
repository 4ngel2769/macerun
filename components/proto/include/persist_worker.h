#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "esp_err.h"

/* NVS commits are erase+write and take far longer than a 25 ms server tick.
 * Callers hand the worker an owned snapshot; the worker performs all NVS
 * writes on its own low-priority task so the net task never blocks. */

esp_err_t persist_worker_init(void);

/* Copies `entries` into a queued job. `entry_size` is bytes per entry.
 * Returns a job id (nonzero) or 0 if the queue is full. */
uint32_t persist_world_deltas_submit(const void *entries, size_t count, size_t entry_size);

/* Copies `blob` under `nvs_key`. Returns a job id or 0 on failure. */
uint32_t persist_player_data_submit(const char *nvs_key,
                                    const uint8_t *blob,
                                    size_t blob_len);

/* True once all jobs with id <= job_id have been committed (or failed). */
bool persist_job_committed(uint32_t job_id);

uint32_t persist_coalesce_count(void);
uint32_t persist_failure_count(void);
