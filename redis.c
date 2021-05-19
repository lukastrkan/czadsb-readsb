#include "readsb.h"

#include "eredis.h"


//
//========================= Redis mode ===============================

__attribute__ ((format(printf, 3, 4))) static char *safe_snprintf(char *p, char *end, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    p += vsnprintf(p < end ? p : NULL, p < end ? (size_t) (end - p) : 0, format, ap);
    va_end(ap);
    return p;
}

static const char *addrtype_enum_string(addrtype_t type) {
    switch (type) {
        case ADDR_ADSB_ICAO:
            return "adsb_icao";
        case ADDR_ADSB_ICAO_NT:
            return "adsb_icao_nt";
        case ADDR_ADSR_ICAO:
            return "adsr_icao";
        case ADDR_TISB_ICAO:
            return "tisb_icao";
        case ADDR_ADSB_OTHER:
            return "adsb_other";
        case ADDR_ADSR_OTHER:
            return "adsr_other";
        case ADDR_TISB_OTHER:
            return "tisb_other";
        case ADDR_TISB_TRACKFILE:
            return "tisb_trackfile";
        default:
            return "unknown";
    }
}

static const char *jsonEscapeString(const char *str) {
    static char buf[1024];
    const char *in = str;
    char *out = buf, *end = buf + sizeof (buf) - 10;

    for (; *in && out < end; ++in) {
        unsigned char ch = *in;
        if (ch == '"' || ch == '\\') {
            *out++ = '\\';
            *out++ = ch;
        } else if (ch < 32 || ch > 127) {
            out = safe_snprintf(out, end, "\\u%04x", ch);
        } else {
            *out++ = ch;
        }
    }

    *out++ = 0;
    return buf;
}

static const char *emergency_enum_string(emergency_t emergency) {
    switch (emergency) {
        case EMERGENCY_NONE: return "none";
        case EMERGENCY_GENERAL: return "general";
        case EMERGENCY_LIFEGUARD: return "lifeguard";
        case EMERGENCY_MINFUEL: return "minfuel";
        case EMERGENCY_NORDO: return "nordo";
        case EMERGENCY_UNLAWFUL: return "unlawful";
        case EMERGENCY_DOWNED: return "downed";
        default: return "reserved";
    }
}

static const char *sil_type_enum_string(sil_type_t type) {
    switch (type) {
        case SIL_UNKNOWN: return "unknown";
        case SIL_PER_HOUR: return "perhour";
        case SIL_PER_SAMPLE: return "persample";
        default: return "invalid";
    }
}

static char *append_flags(char *p, char *end, struct aircraft *a, datasource_t source) {
    p = safe_snprintf(p, end, "[");

    char *start = p;
    if (a->callsign_valid.source == source)
        p = safe_snprintf(p, end, "\"callsign\",");
    if (a->altitude_baro_valid.source == source)
        p = safe_snprintf(p, end, "\"altitude\",");
    if (a->altitude_geom_valid.source == source)
        p = safe_snprintf(p, end, "\"alt_geom\",");
    if (a->gs_valid.source == source)
        p = safe_snprintf(p, end, "\"gs\",");
    if (a->ias_valid.source == source)
        p = safe_snprintf(p, end, "\"ias\",");
    if (a->tas_valid.source == source)
        p = safe_snprintf(p, end, "\"tas\",");
    if (a->mach_valid.source == source)
        p = safe_snprintf(p, end, "\"mach\",");
    if (a->track_valid.source == source)
        p = safe_snprintf(p, end, "\"track\",");
    if (a->track_rate_valid.source == source)
        p = safe_snprintf(p, end, "\"track_rate\",");
    if (a->roll_valid.source == source)
        p = safe_snprintf(p, end, "\"roll\",");
    if (a->mag_heading_valid.source == source)
        p = safe_snprintf(p, end, "\"mag_heading\",");
    if (a->true_heading_valid.source == source)
        p = safe_snprintf(p, end, "\"true_heading\",");
    if (a->baro_rate_valid.source == source)
        p = safe_snprintf(p, end, "\"baro_rate\",");
    if (a->geom_rate_valid.source == source)
        p = safe_snprintf(p, end, "\"geom_rate\",");
    if (a->squawk_valid.source == source)
        p = safe_snprintf(p, end, "\"squawk\",");
    if (a->emergency_valid.source == source)
        p = safe_snprintf(p, end, "\"emergency\",");
    if (a->nav_qnh_valid.source == source)
        p = safe_snprintf(p, end, "\"nav_qnh\",");
    if (a->nav_altitude_mcp_valid.source == source)
        p = safe_snprintf(p, end, "\"nav_altitude_mcp\",");
    if (a->nav_altitude_fms_valid.source == source)
        p = safe_snprintf(p, end, "\"nav_altitude_fms\",");
    if (a->nav_heading_valid.source == source)
        p = safe_snprintf(p, end, "\"nav_heading\",");
    if (a->nav_modes_valid.source == source)
        p = safe_snprintf(p, end, "\"nav_modes\",");
    if (a->position_valid.source == source)
        p = safe_snprintf(p, end, "\"lat\",\"lon\",\"nic\",\"rc\",");
    if (a->nic_baro_valid.source == source)
        p = safe_snprintf(p, end, "\"nic_baro\",");
    if (a->nac_p_valid.source == source)
        p = safe_snprintf(p, end, "\"nac_p\",");
    if (a->nac_v_valid.source == source)
        p = safe_snprintf(p, end, "\"nac_v\",");
    if (a->sil_valid.source == source)
        p = safe_snprintf(p, end, "\"sil\",\"sil_type\",");
    if (a->gva_valid.source == source)
        p = safe_snprintf(p, end, "\"gva\",");
    if (a->sda_valid.source == source)
        p = safe_snprintf(p, end, "\"sda\",");
    if (p != start)
        --p;
    p = safe_snprintf(p, end, "]");
    return p;
}

static struct {
    nav_modes_t flag;
    const char *name;
} nav_modes_names[] = {
        { NAV_MODE_AUTOPILOT, "autopilot"},
        { NAV_MODE_VNAV, "vnav"},
        { NAV_MODE_ALT_HOLD, "althold"},
        { NAV_MODE_APPROACH, "approach"},
        { NAV_MODE_LNAV, "lnav"},
        { NAV_MODE_TCAS, "tcas"},
        { 0, NULL}
};

static char *append_nav_modes(char *p, char *end, nav_modes_t flags, const char *quote, const char *sep) {
    int first = 1;
    for (int i = 0; nav_modes_names[i].name; ++i) {
        if (!(flags & nav_modes_names[i].flag)) {
            continue;
        }

        if (!first) {
            p = safe_snprintf(p, end, "%s", sep);
        }

        first = 0;
        p = safe_snprintf(p, end, "%s%s%s", quote, nav_modes_names[i].name, quote);
    }

    return p;
}


static double convert_altitude(double ft) {
    if (Modes.metric)
        return (ft / 3.2828);
    else
        return ft;
}

static double convert_speed(double kts) {
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

void redisSaveData(void){
    uint64_t now = mstime();
    struct aircraft *a;
    int buflen = 256*2048; // The initial buffer is resized as needed
    char *buf = (char *) malloc(buflen), *p = buf, *end = buf + buflen;
    _messageNow = now;
    for (int j = 0; j < AIRCRAFTS_BUCKETS; j++) {
        for (a = Modes.aircrafts[j]; a; a = a->next) {
            if (a->messages < 2) { // basic filter for bad decodes
                continue;
            }
            if ((now - a->seen) > 90E3) // don't include stale aircraft in the JSON
                continue;
            p = safe_snprintf(p, end, "{\"hex\":\"%s%06x\"", (a->addr & MODES_NON_ICAO_ADDRESS) ? "~" : "", a->addr & 0xFFFFFF);
            if (a->addrtype != ADDR_ADSB_ICAO)
                p = safe_snprintf(p, end, ",\"type\":\"%s\"", addrtype_enum_string(a->addrtype));
            if (trackDataValid(&a->callsign_valid))
                p = safe_snprintf(p, end, ",\"flight\":\"%s\"", jsonEscapeString(a->callsign));
            if (trackDataValid(&a->airground_valid) && a->airground_valid.source >= SOURCE_MODE_S_CHECKED && a->airground == AG_GROUND)
                p = safe_snprintf(p, end, ",\"alt_baro\":\"ground\"");
            else {
                if (trackDataValid(&a->altitude_baro_valid) && a->altitude_baro_reliable >= 3)
                    p = safe_snprintf(p, end, ",\"alt_baro\":%d", (int)convert_altitude(a->altitude_baro));
                if (trackDataValid(&a->altitude_geom_valid))
                    p = safe_snprintf(p, end, ",\"alt_geom\":%d", (int) convert_altitude(a->altitude_geom));
            }
            if (trackDataValid(&a->gs_valid))
                p = safe_snprintf(p, end, ",\"gs\":%.1f", convert_speed(a->gs));
            if (trackDataValid(&a->ias_valid))
                p = safe_snprintf(p, end, ",\"ias\":%.1f", convert_speed(a->ias));
            if (trackDataValid(&a->tas_valid))
                p = safe_snprintf(p, end, ",\"tas\":%.1f", convert_speed(a->tas));
            if (trackDataValid(&a->mach_valid))
                p = safe_snprintf(p, end, ",\"mach\":%.3f", a->mach);
            if (trackDataValid(&a->track_valid))
                p = safe_snprintf(p, end, ",\"track\":%.1f", a->track);
            if (trackDataValid(&a->track_rate_valid))
                p = safe_snprintf(p, end, ",\"track_rate\":%.2f", a->track_rate);
            if (trackDataValid(&a->roll_valid))
                p = safe_snprintf(p, end, ",\"roll\":%.1f", a->roll);
            if (trackDataValid(&a->mag_heading_valid))
                p = safe_snprintf(p, end, ",\"mag_heading\":%.1f", a->mag_heading);
            if (trackDataValid(&a->true_heading_valid))
                p = safe_snprintf(p, end, ",\"true_heading\":%.1f", a->true_heading);
            if (trackDataValid(&a->baro_rate_valid))
                p = safe_snprintf(p, end, ",\"baro_rate\":%d", (int)convert_altitude(a->baro_rate));
            if (trackDataValid(&a->geom_rate_valid))
                p = safe_snprintf(p, end, ",\"geom_rate\":%d", (int)convert_altitude(a->geom_rate));
            if (trackDataValid(&a->squawk_valid))
                p = safe_snprintf(p, end, ",\"squawk\":\"%04x\"", a->squawk);
            if (trackDataValid(&a->emergency_valid))
                p = safe_snprintf(p, end, ",\"emergency\":\"%s\"", emergency_enum_string(a->emergency));
            if (a->category != 0)
                p = safe_snprintf(p, end, ",\"category\":\"%02X\"", a->category);
            if (trackDataValid(&a->nav_qnh_valid))
                p = safe_snprintf(p, end, ",\"nav_qnh\":%.1f", a->nav_qnh);
            if (trackDataValid(&a->nav_altitude_mcp_valid))
                p = safe_snprintf(p, end, ",\"nav_altitude_mcp\":%d", a->nav_altitude_mcp);
            if (trackDataValid(&a->nav_altitude_fms_valid))
                p = safe_snprintf(p, end, ",\"nav_altitude_fms\":%d", a->nav_altitude_fms);
            if (trackDataValid(&a->nav_heading_valid))
                p = safe_snprintf(p, end, ",\"nav_heading\":%.1f", a->nav_heading);
            if (trackDataValid(&a->nav_modes_valid)) {
                p = safe_snprintf(p, end, ",\"nav_modes\":[");
                p = append_nav_modes(p, end, a->nav_modes, "\"", ",");
                p = safe_snprintf(p, end, "]");
            }
            if (trackDataValid(&a->position_valid))
                p = safe_snprintf(p, end, ",\"lat\":%f,\"lon\":%f,\"nic\":%u,\"rc\":%u,\"seen_pos\":%.1f", a->lat, a->lon, a->pos_nic, a->pos_rc, (now - a->position_valid.updated) / 1000.0);
            if (a->adsb_version >= 0)
                p = safe_snprintf(p, end, ",\"version\":%d", a->adsb_version);
            if (trackDataValid(&a->nic_baro_valid))
                p = safe_snprintf(p, end, ",\"nic_baro\":%u", a->nic_baro);
            if (trackDataValid(&a->nac_p_valid))
                p = safe_snprintf(p, end, ",\"nac_p\":%u", a->nac_p);
            if (trackDataValid(&a->nac_v_valid))
                p = safe_snprintf(p, end, ",\"nac_v\":%u", a->nac_v);
            if (trackDataValid(&a->sil_valid))
                p = safe_snprintf(p, end, ",\"sil\":%u", a->sil);
            if (a->sil_type != SIL_INVALID)
                p = safe_snprintf(p, end, ",\"sil_type\":\"%s\"", sil_type_enum_string(a->sil_type));
            if (trackDataValid(&a->gva_valid))
                p = safe_snprintf(p, end, ",\"gva\":%u", a->gva);
            if (trackDataValid(&a->sda_valid))
                p = safe_snprintf(p, end, ",\"sda\":%u", a->sda);
            if (trackDataValid(&a->alert_valid))
                p = safe_snprintf(p, end, ",\"alert\":%u", a->alert);
            if (trackDataValid(&a->spi_valid))
                p = safe_snprintf(p, end, ",\"spi\":%u", a->spi);

            p = safe_snprintf(p, end, ",\"mlat\":");
            p = append_flags(p, end, a, SOURCE_MLAT);
            p = safe_snprintf(p, end, ",\"tisb\":");
            p = append_flags(p, end, a, SOURCE_TISB);

            p = safe_snprintf(p, end, ",\"messages\":%ld,\"seen\":%.1f,\"rssi\":%.1f}",
                              a->messages, (now - a->seen) / 1000.0,
                              10 * log10((a->signalLevel[0] + a->signalLevel[1] + a->signalLevel[2] + a->signalLevel[3] +
                                          a->signalLevel[4] + a->signalLevel[5] + a->signalLevel[6] + a->signalLevel[7] + 1e-5) / 8));

            eredis_w_cmd(e, "SET %s%06X %s", (a->addr & MODES_NON_ICAO_ADDRESS) ? "~" : " ", (a->addr & 0xffffff), buf);
            eredis_w_cmd(e, "EXPIRE %s%06X %d", (a->addr & MODES_NON_ICAO_ADDRESS) ? "~" : " ", (a->addr & 0xffffff), 60);
            free(buf);
            buf = (char *) malloc(buflen), p = buf, end = buf + buflen;
        }
    }
    free(buf);
}