#include "readsb.h"

#include "eredis.h"


//
//========================= Redis mode ===============================

static int convert_altitude(int ft) {
    if (Modes.metric)
        return (ft / 3.2828);
    else
        return ft;
}

static int convert_speed(int kts) {
    if (Modes.metric)
        return (kts * 1.852);
    else
        return kts;
}

eredis_t *e;
char *message;

void redisInit(char* host, int port) {
    if (!Modes.redis)
        return;

    e = eredis_new();
    eredis_host_add(e, host, port);
    eredis_run_thr(e);
    message = (char*) malloc(512*sizeof(char));
}

void redisCleanup(void) {
    if (Modes.redis) {
        free(message);
        eredis_shutdown(e);
        eredis_free(e);
    }
}

void redisSaveData(void) {
    static uint64_t next_update;
    uint64_t now = mstime();
    if (now < next_update)
        return;

    next_update = now + MODES_INTERACTIVE_REFRESH_TIME;

    for (int j = 0; j < AIRCRAFTS_BUCKETS; j++) {
        struct aircraft *a = Modes.aircrafts[j];
        while (a) {

            if ((now - a->seen) < Modes.interactive_display_ttl) {
                int msgs = a->messages;

                if (msgs > 1) {
                    char strSquawk[5] = " ";
                    char strFl[7] = " ";
                    char strTt[5] = " ";
                    char strGs[5] = " ";

                    if (trackDataValid(&a->squawk_valid)) {
                        snprintf(strSquawk, 5, "%04x", a->squawk);
                    }

                    if (trackDataValid(&a->gs_valid)) {
                        snprintf(strGs, 5, "%3d", convert_speed(a->gs));
                    }

                    if (trackDataValid(&a->track_valid)) {
                        snprintf(strTt, 5, "%03.0f", a->track);
                    }

                    if (msgs > 99999) {
                        msgs = 99999;
                    }

                    char strMode[5] = "    ";
                    char strLat[10] = " ";
                    char strLon[11] = " ";
                    double *pSig = a->signalLevel;
                    double signalAverage = (pSig[0] + pSig[1] + pSig[2] + pSig[3] +
                                            pSig[4] + pSig[5] + pSig[6] + pSig[7]) / 8.0;

                    strMode[0] = 'S';
                    if (a->modeA_hit) {
                        strMode[2] = 'a';
                    }
                    if (a->modeC_hit) {
                        strMode[3] = 'c';
                    }
                    if (trackDataValid(&a->position_valid)) {
                        snprintf(strLat, 10, "%7.06f", a->lat);
                        snprintf(strLon, 11, "%8.06f", a->lon);
                    }
                    if (trackDataValid(&a->airground_valid) && a->airground == AG_GROUND) {
                        snprintf(strFl, 7, " grnd");
                    } else if (Modes.use_gnss && trackDataValid(&a->altitude_geom_valid)) {
                        snprintf(strFl, 7, "%5dH", convert_altitude(a->altitude_geom));
                    } else if (trackDataValid(&a->altitude_baro_valid)) {
                        snprintf(strFl, 7, "%5d ", convert_altitude(a->altitude_baro));
                    }

                    sprintf(message,
                            "{"
                            "\"icao\":\"%s%06X\","
                            "\"mode\":\"%-4s\","
                            "\"sqwk\":\"%-4s\","
                            "\"flight\":\"%-8s\","
                            "\"alt\":\"%6s\","
                            "\"spd\":\"%3s\","
                            "\"hdg\":\"%3s\","
                            "\"lat\":\"%7s\","
                            "\"long\":\"%8s\","
                            "\"rssi\":\"%5.1f\","
                            "\"msgs\":\"%5d\","
                            "\"ti\":\"%2.0f\""
                            "}",
                            (a->addr & MODES_NON_ICAO_ADDRESS) ? "~" : " ", (a->addr & 0xffffff),
                            strMode, strSquawk, a->callsign, strFl, strGs, strTt,
                            strLat, strLon, 10 * log10(signalAverage), msgs, (now - a->seen) / 1000.0);
                    eredis_w_cmd(e, "SET %s%06X %s", (a->addr & MODES_NON_ICAO_ADDRESS) ? "~" : " ", (a->addr & 0xffffff), message);
                    eredis_w_cmd(e, "EXPIRE %s%06X %d", (a->addr & MODES_NON_ICAO_ADDRESS) ? "~" : " ", (a->addr & 0xffffff), 60);
                }
            }
            a = a->next;
        }
    }
}