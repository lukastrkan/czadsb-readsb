// Part of readsb, a Mode-S/ADSB/TIS message decoder.
//
// readsb.c: main program & miscellany
//
// Copyright (c) 2019 Michael Wolf <michael@mictronics.de>
//
// This code is based on a detached fork of dump1090-fa.
//
// Copyright (c) 2014-2016 Oliver Jowett <oliver@mutability.co.uk>
//
// This file is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// any later version.
//
// This file is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
// This file incorporates work covered by the following copyright and
// license:
//
// Copyright (C) 2012 by Salvatore Sanfilippo <antirez@gmail.com>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//  *  Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//
//  *  Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in the
//     documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#define READSB
#include "readsb.h"
#include "help.h"

#include <stdarg.h>

struct _Modes Modes;

//
// ============================= Program options help ==========================
//
// This is a little silly, but that's how the preprocessor works..
#define _stringize(x) #x

static error_t parse_opt(int key, char *arg, struct argp_state *state);
const char *argp_program_version = MODES_READSB_VARIANT " " MODES_READSB_VERSION;
const char doc[] = "readsb Mode-S/ADSB/TIS Receiver   "
        MODES_READSB_VARIANT " " MODES_READSB_VERSION
        "\nBuild options: "
#ifdef ENABLE_RTLSDR
        "ENABLE_RTLSDR "
#endif
#ifdef ENABLE_BLADERF
        "ENABLE_BLADERF "
#endif
#ifdef ENABLE_PLUTOSDR
        "ENABLE_PLUTOSDR "
#endif
#ifdef SC16Q11_TABLE_BITS
#define stringize(x) _stringize(x)
        "SC16Q11_TABLE_BITS=" stringize(SC16Q11_TABLE_BITS)
#undef stringize
#endif
"\v"
"Debug mode flags: d = Log frames decoded with errors\n"
"                  D = Log frames decoded with zero errors\n"
"                  c = Log frames with bad CRC\n"
"                  C = Log frames with good CRC\n"
"                  p = Log frames with bad preamble\n"
"                  n = Log network debugging info\n"
"                  j = Log frames to frames.js, loadable by debug.html\n";

#undef _stringize
#undef verstring

const char args_doc[] = "";
static struct argp argp = {options, parse_opt, args_doc, doc, NULL, NULL, NULL};

//
// ============================= Utility functions ==========================
//
static void log_with_timestamp(const char *format, ...) __attribute__ ((format(printf, 1, 2)));

static void log_with_timestamp(const char *format, ...) {
    char timebuf[128];
    char msg[1024];
    time_t now;
    struct tm local;
    va_list ap;

    now = time(NULL);
    localtime_r(&now, &local);
    strftime(timebuf, 128, "%c %Z", &local);
    timebuf[127] = 0;

    va_start(ap, format);
    vsnprintf(msg, 1024, format, ap);
    va_end(ap);
    msg[1023] = 0;

    fprintf(stderr, "%s  %s\n", timebuf, msg);
}

static void sigintHandler(int dummy) {
    MODES_NOTUSED(dummy);
    signal(SIGINT, SIG_DFL); // reset signal handler - bit extra safety
    Modes.exit = 1; // Signal to threads that we are done
    log_with_timestamp("Caught SIGINT, shutting down..\n");
}

static void sigtermHandler(int dummy) {
    MODES_NOTUSED(dummy);
    signal(SIGTERM, SIG_DFL); // reset signal handler - bit extra safety
    Modes.exit = 1; // Signal to threads that we are done
    log_with_timestamp("Caught SIGTERM, shutting down..\n");
}

void receiverPositionChanged(float lat, float lon, float alt) {
    log_with_timestamp("Autodetected receiver location: %.5f, %.5f at %.0fm AMSL", lat, lon, alt);
    writeJsonToFile("receiver.json", generateReceiverJson()); // location changed
}


//
// =============================== Initialization ===========================
//
static void modesInitConfig(void) {
    // Default everything to zero/NULL
    memset(&Modes, 0, sizeof (Modes));

    // Now initialise things that should not be 0/NULL to their defaults
    Modes.gain = MODES_MAX_GAIN;
    Modes.freq = MODES_DEFAULT_FREQ;
    Modes.check_crc = 1;
    Modes.net_heartbeat_interval = MODES_NET_HEARTBEAT_INTERVAL;
    Modes.net_input_raw_ports = strdup("30001");
    Modes.net_output_raw_ports = strdup("30002");
    Modes.net_output_sbs_ports = strdup("30003");
    Modes.net_input_sbs_ports = strdup("0");
    Modes.net_input_beast_ports = strdup("30004,30104");
    Modes.net_output_beast_ports = strdup("30005");
    Modes.net_output_beast_reduce_ports = strdup("0");
    Modes.net_output_beast_reduce_interval = 125;
    Modes.net_output_vrs_ports = strdup("0");
    Modes.net_connector_delay = 30 * 1000;
    Modes.interactive_display_ttl = MODES_INTERACTIVE_DISPLAY_TTL;
    Modes.json_interval = 1000;
    Modes.json_location_accuracy = 2;
    Modes.maxRange = 1852 * 300; // 300NM default max range
    Modes.mode_ac_auto = 1;
    Modes.nfix_crc = 1;
    Modes.biastee = 0;
    Modes.filter_persistence = 2;
    Modes.net_sndbuf_size = 2; // Default to 256 kB network write buffers
    Modes.net_output_flush_size = 1200; // Default to 1200 Bytes
    Modes.net_output_flush_interval = 50; // Default to 50 ms
    Modes.basestation_is_mlat = 1;

    sdrInitConfig();
}
//
//=========================================================================
//
static void modesInit(void) {
    int i;

    pthread_mutex_init(&Modes.data_mutex, NULL);
    pthread_cond_init(&Modes.data_cond, NULL);

    Modes.sample_rate = (double)2400000.0;

    // Allocate the various buffers used by Modes
    Modes.trailing_samples = (MODES_PREAMBLE_US + MODES_LONG_MSG_BITS + 16) * 1e-6 * Modes.sample_rate;

    for (i = 0; i < MODES_MAG_BUFFERS; ++i) {
        if ((Modes.mag_buffers[i].data = calloc(MODES_MAG_BUF_SAMPLES + Modes.trailing_samples, sizeof (uint16_t))) == NULL) {
            fprintf(stderr, "Out of memory allocating magnitude buffer.\n");
            exit(1);
        }

        Modes.mag_buffers[i].length = 0;
        Modes.mag_buffers[i].dropped = 0;
        Modes.mag_buffers[i].sampleTimestamp = 0;
    }

    // Validate the users Lat/Lon home location inputs
    if ((Modes.fUserLat > 90.0) // Latitude must be -90 to +90
            || (Modes.fUserLat < -90.0) // and
            || (Modes.fUserLon > 360.0) // Longitude must be -180 to +360
            || (Modes.fUserLon < -180.0)) {
        Modes.fUserLat = Modes.fUserLon = 0.0;
    } else if (Modes.fUserLon > 180.0) { // If Longitude is +180 to +360, make it -180 to 0
        Modes.fUserLon -= 360.0;
    }
    // If both Lat and Lon are 0.0 then the users location is either invalid/not-set, or (s)he's in the
    // Atlantic ocean off the west coast of Africa. This is unlikely to be correct.
    // Set the user LatLon valid flag only if either Lat or Lon are non zero. Note the Greenwich meridian
    // is at 0.0 Lon,so we must check for either fLat or fLon being non zero not both.
    // Testing the flag at runtime will be much quicker than ((fLon != 0.0) || (fLat != 0.0))
    Modes.bUserFlags &= ~MODES_USER_LATLON_VALID;
    if ((Modes.fUserLat != 0.0) || (Modes.fUserLon != 0.0)) {
        Modes.bUserFlags |= MODES_USER_LATLON_VALID;
    }

    // Limit the maximum requested raw output size to less than one Ethernet Block
    // Set to default if 0
    if (Modes.net_output_flush_size > (MODES_OUT_FLUSH_SIZE) || Modes.net_output_flush_size == 0) {
        Modes.net_output_flush_size = MODES_OUT_FLUSH_SIZE;
    }
    if (Modes.net_output_flush_interval > (MODES_OUT_FLUSH_INTERVAL)) {
        Modes.net_output_flush_interval = MODES_OUT_FLUSH_INTERVAL;
    }
    if (Modes.net_sndbuf_size > (MODES_NET_SNDBUF_MAX)) {
        Modes.net_sndbuf_size = MODES_NET_SNDBUF_MAX;
    }

    if((Modes.net_connector_delay <= 0) || (Modes.net_connector_delay > 86400 * 1000)) {
        Modes.net_connector_delay = 30 * 1000;
    }

    // Prepare error correction tables
    modesChecksumInit(Modes.nfix_crc);
    icaoFilterInit();
    modeACInit();

    if (Modes.show_only)
        icaoFilterAdd(Modes.show_only);
}

// Set affinity of calling thread to specific core on a multi-core CPU
static int thread_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (core_id < 0 || core_id >= num_cores)
        return EINVAL;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    return pthread_setaffinity_np(current_thread, sizeof (cpu_set_t), &cpuset);
}

//
//=========================================================================
//
// We read data using a thread, so the main thread only handles decoding
// without caring about data acquisition
//
void *readerThreadEntryPoint(void *arg) {
    MODES_NOTUSED(arg);

    // Try sticking this thread to core 3
    thread_to_core(3);

    sdrRun();

    // Wake the main thread (if it's still waiting)
    pthread_mutex_lock(&Modes.data_mutex);
    if (!Modes.exit)
        Modes.exit = 2; // unexpected exit
    pthread_cond_signal(&Modes.data_cond);
    pthread_mutex_unlock(&Modes.data_mutex);

#ifndef _WIN32
    pthread_exit(NULL);
#else
    return NULL;
#endif
}
//
// ============================== Snip mode =================================
//
// Get raw IQ samples and filter everything is < than the specified level
// for more than 256 samples in order to reduce example file size
//
static void snipMode(int level) {
    int i, q;
    uint64_t c = 0;

    while ((i = getchar()) != EOF && (q = getchar()) != EOF) {
        if (abs(i - 127) < level && abs(q - 127) < level) {
            c++;
            if (c > MODES_PREAMBLE_SIZE) continue;
        } else {
            c = 0;
        }
        putchar(i);
        putchar(q);
    }
}

static void display_total_stats(void) {
    struct stats added;
    add_stats(&Modes.stats_alltime, &Modes.stats_current, &added);
    display_stats(&added);
}

//
//=========================================================================
//
// This function is called a few times every second by main in order to
// perform tasks we need to do continuously, like accepting new clients
// from the net, refreshing the screen in interactive mode, and so forth
//
static void backgroundTasks(void) {
    static uint64_t next_stats_display;
    static uint64_t next_stats_update;
    static uint64_t next_json, next_history;
    static uint64_t last_second;

    uint64_t now = mstime();

    icaoFilterExpire();
    trackPeriodicUpdate();

    if (Modes.net) {
        modesNetPeriodicWork();
        if (last_second + 1000 < now) {
            modesNetSecondWork();
            last_second = now;
        }
    }


    // Refresh screen when in interactive mode
    if (Modes.interactive) {
        interactiveShowData();
    }

    if (Modes.redis) {
        if(Modes.redisSingle)
        {
            redisSaveSingle();
        }
        if(Modes.redisAll || (!Modes.redisAll && !Modes.redisSingle))
        {
            redisSaveAll();
        }        
        
    }


    // always update end time so it is current when requests arrive
    Modes.stats_current.end = mstime();

    if (now >= next_stats_update) {
        int i;

        if (next_stats_update == 0) {
            next_stats_update = now + 60000;
        } else {
            Modes.stats_latest_1min = (Modes.stats_latest_1min + 1) % 15;
            Modes.stats_1min[Modes.stats_latest_1min] = Modes.stats_current;

            add_stats(&Modes.stats_current, &Modes.stats_alltime, &Modes.stats_alltime);
            add_stats(&Modes.stats_current, &Modes.stats_periodic, &Modes.stats_periodic);

            reset_stats(&Modes.stats_5min);
            for (i = 0; i < 5; ++i)
                add_stats(&Modes.stats_1min[(Modes.stats_latest_1min - i + 15) % 15], &Modes.stats_5min, &Modes.stats_5min);

            reset_stats(&Modes.stats_15min);
            for (i = 0; i < 15; ++i)
                add_stats(&Modes.stats_1min[i], &Modes.stats_15min, &Modes.stats_15min);

            reset_stats(&Modes.stats_current);
            Modes.stats_current.start = Modes.stats_current.end = now;

            if (Modes.json_dir)
                writeJsonToFile("stats.json", generateStatsJson());

            next_stats_update += 60000;
        }
    }

    if (Modes.stats && now >= next_stats_display) {
        if (next_stats_display == 0) {
            next_stats_display = now + Modes.stats;
        } else {
            add_stats(&Modes.stats_periodic, &Modes.stats_current, &Modes.stats_periodic);
            display_stats(&Modes.stats_periodic);
            reset_stats(&Modes.stats_periodic);

            next_stats_display += Modes.stats;
            if (next_stats_display <= now) {
                /* something has gone wrong, perhaps the system clock jumped */
                next_stats_display = now + Modes.stats;
            }
        }
    }

    if (Modes.json_dir && now >= next_json) {
        writeJsonToFile("aircraft.json", generateAircraftJson());
        next_json = now + Modes.json_interval;
        //writeJsonToFile("vrs.json", generateVRS(0, 1));
    }

    if (Modes.json_dir && now >= next_history) {
        char filebuf[PATH_MAX];
        snprintf(filebuf, PATH_MAX, "history_%d.json", Modes.json_aircraft_history_next);
        writeJsonToFile(filebuf, generateAircraftJson());

        if (!Modes.json_aircraft_history_full) {
            writeJsonToFile("receiver.json", generateReceiverJson()); // number of history entries changed
            if (Modes.json_aircraft_history_next == HISTORY_SIZE - 1)
                Modes.json_aircraft_history_full = 1;
        }

        Modes.json_aircraft_history_next = (Modes.json_aircraft_history_next + 1) % HISTORY_SIZE;
        next_history = now + HISTORY_INTERVAL;
    }
}

//=========================================================================
// Clean up memory prior to exit.
static void cleanup_and_exit(int code) {
    // Free any used memory
    interactiveCleanup();
    redisCleanup();
    free(Modes.redisHost);
    free(Modes.dev_name);
    free(Modes.filename);
    /* Free only when pointing to string in heap (strdup allocated when given as run parameter)
     * otherwise points to const string
     */
    free(Modes.json_dir);
    free(Modes.net_bind_address);
    free(Modes.net_input_beast_ports);
    free(Modes.net_output_beast_ports);
    free(Modes.net_output_beast_reduce_ports);
    free(Modes.net_output_vrs_ports);
    free(Modes.net_input_raw_ports);
    free(Modes.net_output_raw_ports);
    free(Modes.net_output_sbs_ports);
    free(Modes.net_input_sbs_ports);
    free(Modes.beast_serial);
    /* Go through tracked aircraft chain and free up any used memory */
    for (int j = 0; j < AIRCRAFTS_BUCKETS; j++) {
        struct aircraft *a = Modes.aircrafts[j], *na;
        while (a) {
            na = a->next;
            if (a) free(a);
            a = na;
        }
    }

    int i;
    for (i = 0; i < MODES_MAG_BUFFERS; ++i) {
        free(Modes.mag_buffers[i].data);
    }
    crcCleanupTables();

    /* Cleanup network setup */

    cleanupNetwork();


#ifndef _WIN32
    exit(code);
#else
    return (code);
#endif
}

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    switch (key) {
        case OptRedis:
            Modes.redis = 1;
            break;
        case OptRedisHost:
            Modes.redisHost = strdup(arg);
            break;
        case OptRedisPort:
            Modes.redisPort = atoi(arg);
            break;
        case OptRedisSingle:
            Modes.redisSingle = 1;
            break;
        case OptRedisAll:
            Modes.redisAll = 1;
            break;
        case OptDevice:
            Modes.dev_name = strdup(arg);
            break;
        case OptGain:
            Modes.gain = (int) (atof(arg)*10); // Gain is in tens of DBs
            break;
        case OptFreq:
            Modes.freq = (int) strtoll(arg, NULL, 10);
            break;
        case OptDcFilter:
            Modes.dc_filter = 1;
            break;
        case OptBiasTee:
            Modes.biastee = 1;
            break;
        case OptFix:
            Modes.nfix_crc = 1;
            break;
        case OptNoFix:
            Modes.nfix_crc = 0;
            break;
        case OptNoCrcCheck:
            Modes.check_crc = 0;
            break;
        case OptRaw:
            Modes.raw = 1;
            break;
        case OptNet:
            Modes.net = 1;
            break;
        case OptModeAc:
            Modes.mode_ac = 1;
            Modes.mode_ac_auto = 0;
            break;
        case OptNoModeAcAuto:
            Modes.mode_ac_auto = 0;
            break;
        case OptNetOnly:
            Modes.net = 1;
            Modes.sdr_type = SDR_NONE;
            break;
        case OptQuiet:
            Modes.quiet = 1;
            break;
        case OptShowOnly:
            Modes.show_only = (uint32_t) strtoul(arg, NULL, 16);
            break;
        case OptMlat:
            Modes.mlat = 1;
            break;
        case OptForwardMlat:
            Modes.forward_mlat = 1;
            break;
        case OptOnlyAddr:
            Modes.onlyaddr = 1;
            break;
        case OptMetric:
            Modes.metric = 1;
            break;
        case OptGnss:
            Modes.use_gnss = 1;
            break;
        case OptAggressive:
            Modes.nfix_crc = MODES_MAX_BITERRORS;
            break;
        case OptInteractive:
            Modes.interactive = 1;
            break;
        case OptInteractiveTTL:
            Modes.interactive_display_ttl = (uint64_t) (1000 * atof(arg));
            break;
        case OptLat:
            Modes.fUserLat = atof(arg);
            break;
        case OptLon:
            Modes.fUserLon = atof(arg);
            break;
        case OptMaxRange:
            Modes.maxRange = atof(arg) * 1852.0; // convert to metres
            break;
        case OptStats:
            if (!Modes.stats)
                Modes.stats = (uint64_t) 1 << 60; // "never"
            break;
        case OptStatsRange:
            Modes.stats_range_histo = 1;
            break;
        case OptStatsEvery:
            Modes.stats = (uint64_t) (1000 * atof(arg));
            break;
        case OptSnip:
            snipMode(atoi(arg));
            cleanup_and_exit(0);
            break;
#ifndef _WIN32
        case OptJsonDir:
            Modes.json_dir = strdup(arg);
            break;
        case OptJsonTime:
            Modes.json_interval = (uint64_t) (1000 * atof(arg));
            if (Modes.json_interval < 100) // 0.1s
                Modes.json_interval = 100;
            break;
        case OptJsonLocAcc:
            Modes.json_location_accuracy = atoi(arg);
            break;
#endif
        case OptNetHeartbeat:
            Modes.net_heartbeat_interval = (uint64_t) (1000 * atof(arg));
            break;
        case OptNetRoSize:
            Modes.net_output_flush_size = atoi(arg);
            break;
        case OptNetRoRate:
            Modes.net_output_flush_interval = 1000 * atoi(arg) / 15; // backwards compatibility
            break;
        case OptNetRoIntervall:
            Modes.net_output_flush_interval = (uint64_t) (1000 * atof(arg));
            break;
        case OptNetRoPorts:
            free(Modes.net_output_raw_ports);
            Modes.net_output_raw_ports = strdup(arg);
            break;
        case OptNetRiPorts:
            free(Modes.net_input_raw_ports);
            Modes.net_input_raw_ports = strdup(arg);
            break;
        case OptNetBoPorts:
            free(Modes.net_output_beast_ports);
            Modes.net_output_beast_ports = strdup(arg);
            break;
        case OptNetBiPorts:
            free(Modes.net_input_beast_ports);
            Modes.net_input_beast_ports = strdup(arg);
            break;
        case OptNetBeastReducePorts:
            free(Modes.net_output_beast_reduce_ports);
            Modes.net_output_beast_reduce_ports = strdup(arg);
            break;
        case OptNetBeastReduceInterval:
            if (atof(arg) >= 0)
                Modes.net_output_beast_reduce_interval = (uint64_t) (1000 * atof(arg));
            if (Modes.net_output_beast_reduce_interval > 15000)
                Modes.net_output_beast_reduce_interval = 15000;
            break;
        case OptNetBindAddr:
            free(Modes.net_bind_address);
            Modes.net_bind_address = strdup(arg);
            break;
        case OptNetSbsPorts:
            free(Modes.net_output_sbs_ports);
            Modes.net_output_sbs_ports = strdup(arg);
            break;
        case OptNetSbsInPorts:
            free(Modes.net_input_sbs_ports);
            Modes.net_input_sbs_ports = strdup(arg);
            break;
        case OptNetVRSPorts:
            free(Modes.net_output_vrs_ports);
            Modes.net_output_vrs_ports = strdup(arg);
            break;
        case OptNetBuffer:
            Modes.net_sndbuf_size = atoi(arg);
            break;
        case OptNetVerbatim:
            Modes.net_verbatim = 1;
            break;
        case OptNetConnector:
            if (!Modes.net_connectors || Modes.net_connectors_count + 1 > Modes.net_connectors_size) {
                Modes.net_connectors_size = Modes.net_connectors_count * 2 + 8;
                Modes.net_connectors = realloc(Modes.net_connectors,
                        sizeof(struct net_connector *) * Modes.net_connectors_size);
                if (!Modes.net_connectors)
                    return 1;
            }
            struct net_connector *con = calloc(1, sizeof(struct net_connector));
            Modes.net_connectors[Modes.net_connectors_count++] = con;
            char *connect_string = strdup(arg);
            con->address = strtok(connect_string, ",");
            con->port = strtok(NULL, ",");
            con->protocol = strtok(NULL, ",");
            //fprintf(stderr, "%d %s\n", Modes.net_connectors_count, con->protocol);
            if (!con->address || !con->port || !con->protocol) {
                fprintf(stderr, "--net-connector: Wrong format: %s\n", arg);
                fprintf(stderr, "Correct syntax: --net-connector=ip,port,protocol\n");
                return 1;
            }
            if (strcmp(con->protocol, "beast_out") != 0
                    && strcmp(con->protocol, "beast_reduce_out") != 0
                    && strcmp(con->protocol, "beast_in") != 0
                    && strcmp(con->protocol, "raw_out") != 0
                    && strcmp(con->protocol, "raw_in") != 0
                    && strcmp(con->protocol, "vrs_out") != 0
                    && strcmp(con->protocol, "sbs_in") != 0
                    && strcmp(con->protocol, "sbs_out") != 0) {
                fprintf(stderr, "--net-connector: Unknown protocol: %s\n", con->protocol);
                fprintf(stderr, "Supported protocols: beast_out, beast_in, beast_reduce_out, raw_out, raw_in, sbs_out, sbs_in, vrs_out\n");
                return 1;
            }
            if (strcmp(con->address, "") == 0 || strcmp(con->address, "") == 0) {
                fprintf(stderr, "--net-connector: ip and port can't be empty!\n");
                fprintf(stderr, "Correct syntax: --net-connector=ip,port,protocol\n");
                return 1;
            }
            if (atol(con->port) > (1<<16) || atol(con->port) < 1) {
                fprintf(stderr, "--net-connector: port must be in range 1 to 65536\n");
                return 1;
            }
            break;
        case OptNetConnectorDelay:
            Modes.net_connector_delay = (uint64_t) 1000 * atof(arg);
            break;
        case OptDebug:
            while (*arg) {
                switch (*arg) {
                    case 'D': Modes.debug |= MODES_DEBUG_DEMOD;
                        break;
                    case 'd': Modes.debug |= MODES_DEBUG_DEMODERR;
                        break;
                    case 'C': Modes.debug |= MODES_DEBUG_GOODCRC;
                        break;
                    case 'c': Modes.debug |= MODES_DEBUG_BADCRC;
                        break;
                    case 'p': Modes.debug |= MODES_DEBUG_NOPREAMBLE;
                        break;
                    case 'n': Modes.debug |= MODES_DEBUG_NET;
                        break;
                    case 'j': Modes.debug |= MODES_DEBUG_JS;
                        break;
                    default:
                        fprintf(stderr, "Unknown debugging flag: %c\n", *arg);
                        return 1;
                        break;
                }
                arg++;
            }
            break;
#ifdef ENABLE_RTLSDR
        case OptRtlSdrEnableAgc:
        case OptRtlSdrPpm:
#endif
        case OptBeastSerial:
        case OptBeastDF1117:
        case OptBeastDF045:
        case OptBeastMlatTimeOff:
        case OptBeastCrcOff:
        case OptBeastFecOff:
        case OptBeastModeAc:
        case OptIfileName:
        case OptIfileFormat:
        case OptIfileThrottle:
#ifdef ENABLE_BLADERF
        case OptBladeFpgaDir:
        case OptBladeDecim:
        case OptBladeBw:
#endif
#ifdef ENABLE_PLUTOSDR
        case OptPlutoUri:
        case OptPlutoNetwork:
#endif
        case OptDeviceType:
            /* Forward interface option to the specific device handler */
            if (sdrHandleOption(key, arg) == false)
                return 1;
            break;
        case ARGP_KEY_END:
            if (state->arg_num > 0)
                /* We use only options but no arguments */
                argp_usage(state);
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

//
//=========================================================================
//

int main(int argc, char **argv) {
    int j;

    // Set sane defaults
    modesInitConfig();

    // signal handlers:
    signal(SIGINT, sigintHandler);
    signal(SIGTERM, sigtermHandler);

    /* On a multi-core CPU we run the main thread and reader thread on different cores.
     * Try sticking the main thread to core 1
     */
    thread_to_core(1);

    // Parse the command line options
    if (argp_parse(&argp, argc, argv, 0, 0, 0)) {
        fprintf(stderr, "Command line used:\n");
        for (int i = 0; i < argc; i++) {
            fprintf(stderr, "%s ", argv[i]);
        }
        fprintf(stderr, "\n");
        cleanup_and_exit(1);
    }

#ifdef _WIN32
    // Try to comply with the Copyright license conditions for binary distribution
    if (!Modes.quiet) {
        showCopyright();
    }
#endif

    // Initialization
    log_with_timestamp("%s %s starting up.", MODES_READSB_VARIANT, MODES_READSB_VERSION);
    modesInit();

    if (!sdrOpen()) {
        cleanup_and_exit(1);
    }

    if (Modes.net) {
        modesInitNet();
    }

    // init stats:
    Modes.stats_current.start = Modes.stats_current.end =
            Modes.stats_alltime.start = Modes.stats_alltime.end =
            Modes.stats_periodic.start = Modes.stats_periodic.end =
            Modes.stats_5min.start = Modes.stats_5min.end =
            Modes.stats_15min.start = Modes.stats_15min.end = mstime();

    for (j = 0; j < 15; ++j)
        Modes.stats_1min[j].start = Modes.stats_1min[j].end = Modes.stats_current.start;

    // write initial json files so they're not missing
    writeJsonToFile("receiver.json", generateReceiverJson());
    writeJsonToFile("stats.json", generateStatsJson());
    writeJsonToFile("aircraft.json", generateAircraftJson());

    interactiveInit();
    if(Modes.redisPort)
    {
        redisInit(Modes.redisHost, Modes.redisPort);
    }
    else
    {
        redisInit(Modes.redisHost, 6379);
    }


    /* If the user specifies --net-only, just run in order to serve network
     * clients without reading data from the RTL device.
     * This rules also in case a local Mode-S Beast is connected via USB.
     */
    if (Modes.sdr_type == SDR_NONE || Modes.sdr_type == SDR_MODESBEAST || Modes.sdr_type == SDR_GNS) {
        struct timespec slp = {0, 20 * 1000 * 1000};
        while (!Modes.exit) {
            int64_t sleep_millis = 100;
            struct timespec start_time;

            start_cpu_timing(&start_time);
            backgroundTasks();
            int64_t elapsed = end_cpu_timing(&start_time, &Modes.stats_current.background_cpu);

            sleep_millis = 100 - elapsed;
            sleep_millis = (sleep_millis < 10) ? 10 : sleep_millis;
            sleep_millis = (sleep_millis > 100) ? 100 : sleep_millis;

            //fprintf(stderr, "%ld\n", sleep_millis);

            slp.tv_nsec = sleep_millis * 1000 * 1000;
            nanosleep(&slp, NULL);
        }
    } else {
        int watchdogCounter = 10; // about 1 second

        // Create the thread that will read the data from the device.
        pthread_mutex_lock(&Modes.data_mutex);
        pthread_create(&Modes.reader_thread, NULL, readerThreadEntryPoint, NULL);

        while (!Modes.exit) {
            struct timespec start_time;

            if (Modes.first_free_buffer == Modes.first_filled_buffer) {
                /* wait for more data.
                 * we should be getting data every 50-60ms. wait for max 100ms before we give up and do some background work.
                 * this is fairly aggressive as all our network I/O runs out of the background work!
                 */

                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_nsec += 100000000;
                normalize_timespec(&ts);
                pthread_cond_timedwait(&Modes.data_cond, &Modes.data_mutex, &ts); // This unlocks Modes.data_mutex, and waits for Modes.data_cond
            }

            // Modes.data_mutex is locked, and possibly we have data.

            // copy out reader CPU time and reset it
            add_timespecs(&Modes.reader_cpu_accumulator, &Modes.stats_current.reader_cpu, &Modes.stats_current.reader_cpu);
            Modes.reader_cpu_accumulator.tv_sec = 0;
            Modes.reader_cpu_accumulator.tv_nsec = 0;

            if (Modes.first_free_buffer != Modes.first_filled_buffer) {
                // FIFO is not empty, process one buffer.

                struct mag_buf *buf;

                start_cpu_timing(&start_time);
                buf = &Modes.mag_buffers[Modes.first_filled_buffer];

                // Process data after releasing the lock, so that the capturing
                // thread can read data while we perform computationally expensive
                // stuff at the same time.
                pthread_mutex_unlock(&Modes.data_mutex);

                demodulate2400(buf);
                if (Modes.mode_ac) {
                    demodulate2400AC(buf);
                }

                Modes.stats_current.samples_processed += buf->length;
                Modes.stats_current.samples_dropped += buf->dropped;
                end_cpu_timing(&start_time, &Modes.stats_current.demod_cpu);

                // Mark the buffer we just processed as completed.
                pthread_mutex_lock(&Modes.data_mutex);
                Modes.first_filled_buffer = (Modes.first_filled_buffer + 1) % MODES_MAG_BUFFERS;
                pthread_cond_signal(&Modes.data_cond);
                pthread_mutex_unlock(&Modes.data_mutex);
                watchdogCounter = 10;
            } else {
                // Nothing to process this time around.
                pthread_mutex_unlock(&Modes.data_mutex);
                if (--watchdogCounter <= 0) {
                    log_with_timestamp("No data received from the SDR for a long time, it may have wedged");
                    watchdogCounter = 600;
                }
            }

            start_cpu_timing(&start_time);
            backgroundTasks();
            end_cpu_timing(&start_time, &Modes.stats_current.background_cpu);
            pthread_mutex_lock(&Modes.data_mutex);
        }

        pthread_mutex_unlock(&Modes.data_mutex);

        log_with_timestamp("Waiting for receive thread termination");
        pthread_join(Modes.reader_thread, NULL); // Wait on reader thread exit
        pthread_cond_destroy(&Modes.data_cond); // Thread cleanup - only after the reader thread is dead!
        pthread_mutex_destroy(&Modes.data_mutex);
    }

    // If --stats were given, print statistics
    if (Modes.stats) {
        display_total_stats();
    }
    sdrClose();
    if (Modes.exit != 1) {
        log_with_timestamp("Abnormal exit.");
        cleanup_and_exit(1);
    }

    log_with_timestamp("Normal exit.");
    cleanup_and_exit(0);
}
//
//=========================================================================
//
