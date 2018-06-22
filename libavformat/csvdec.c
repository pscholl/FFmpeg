/*
 * CSV demuxer
 * Copyright (c) 2016 Philipp M. Scholl
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <stdbool.h>
#include "avformat.h"
#include "internal.h"
#include "libavutil/log.h"
#include "libavutil/opt.h"
#include "libavutil/avassert.h"
#include "libavutil/parseutils.h"

/* for subtitle and time parsing */
#define TIMEBASE_US (1000*1000)

/* representing a single line of a CSV file */
typedef struct {
    int64_t timestamp; // sample pts
    char *ts,          // timestamp in string format
         *label,       // optional label of the sample
         *field;       // first fiels in the CSV file
} csv_pkt_t;

typedef struct CSVContext {
    AVClass *class;

    char     *delims;          // field separators
    uint32_t sample_rate;
    int      decode_subtitle;  // are there labels to decode?
    bool     decode_time,      // is there a timestamp to decode?
             flush;            // is there enough data to flush a block

    int64_t  pts;              // current pts

    struct {
        csv_pkt_t beg, end;    // store the current block of data
    } pkt_q;

    char   *buffer;            // space for the CSV data

    size_t numlines,           // number of lines/samples read
           numfields,          // number of samples per line
           allocated,          // allocated bytes in buffer
           used;               // used bytes in buffer
} CSVContext;

/**
 * parse a timestamp in the CSCV file, using av_parse_time.
 *
 * @param s csv context
 * @param ts store timestamp here
 * @param str timestamp to be parsed
 */
static int csv_parse_time(AVFormatContext *s, int64_t *ts, char *str)
{
    CSVContext *csv = s->priv_data;
    int ret = 0;

    if (csv->decode_time) {
        ret = av_parse_time(ts, str, false);

        if (ret)
            ret = av_parse_time(ts, str, true);

        if (ret)
            return ret;
    }
    else
        *ts = csv->pts;

    return 0;
}

/**
 * this function is just here to be called from csv_readfields for each read
 * and during startup from csv_header to fill in the q pointer after decode_*
 * has been properly set, which requires one call to the readfields.
 *
 * @param s csv context
 * @param q packet queue to fill
 * @param fields pointer to the fields of the csv line
 */
static int csv_fillq(AVFormatContext *s, csv_pkt_t *q, char *fields)
{
    CSVContext *csv = s->priv_data;
    q->ts    = csv->decode_time ? fields : "";
    fields  += csv->decode_time ? strlen(q->ts)+1 : 0;
    q->label = csv->decode_subtitle > 0 ? fields : "";
    fields  += csv->decode_subtitle > 0 ? strlen(q->label)+1 : 0;
    q->field = fields;
    return csv_parse_time(s, &q->timestamp, q->ts);
}

/**
 * read another line of the csv file and put it into the queue
 *
 * @param s csv context
 */
static int csv_readfields(AVFormatContext *s)
{
    CSVContext *csv = s->priv_data;
    size_t bytesread = 0,
           saveused  = csv->used,
           numfields = 0;
    bool   escape     = false,
           escone     = false,
           endline    = false,
           iscomment  = false;

    if (csv->buffer)
    /**
     *  do some buffer housekeeping and remove everything
     *  before csv->pkt_q.beg and move the buffer contents
     *  to the front. Also adjust all pointer accordingly.
     */
    {
        char *front = csv->decode_time ? csv->pkt_q.beg.ts :
                                    csv->decode_subtitle ? csv->pkt_q.beg.label :
                                    csv->pkt_q.beg.field;
        size_t movd = front - csv->buffer;

        if (movd)
        {
            csv->used -= movd;
            saveused    -= movd;
            memmove(csv->buffer, front, saveused);

            csv->pkt_q.beg.ts -= csv->decode_time ? movd : 0;
            csv->pkt_q.end.ts -= csv->decode_time ? movd : 0;
            csv->pkt_q.beg.label -= csv->decode_subtitle ? movd : 0;
            csv->pkt_q.end.label -= csv->decode_subtitle ? movd : 0;
            csv->pkt_q.beg.field -= movd;
            csv->pkt_q.end.field -= movd;
        }
    }

    while (!endline)
    {
        static char c;

        /**
         * now parse the CSV line:
         *  - ignore lines starting with an '#'
         *  - escape with '\' and enclosing in '"'
         *  - remove all delims (' ' and '\t')
         *  - let csv->buffer contain all fields separated
         *      by single '0' characters.
         */
        if (avio_feof(s->pb))
        {
            if (bytesread == 0)
                return AVERROR_EOF;

            endline = true;
            c = '\0';
            goto store;
        }

        c = avio_r8(s->pb);

        if (iscomment && c != '\n')
            continue;

        else if (escone) {
            escone = false;
            goto store;
        }

        else if (escape) {
            escape = c!='"';
            goto store;
        }

        else if (strchr(csv->delims, c)) {
            if (csv->buffer && csv->used && csv->buffer[csv->used-1] != '\0') {
                c = '\0'; goto store;
            } else
                continue;
        }

        switch(c)
        {
        case '#':
            iscomment = true;
            continue;

        case '\\':
            escone = true;
            break;

        case '"':
            escape = true;
            break;

        case '\r':
        case '\n':
            iscomment = false;
            if (bytesread == 0)
                continue;
            endline = true;

            if (csv->buffer[csv->used-1] == '\0')
                goto end;

            c = '\0';
            break;
        }

    store:
        if (csv->allocated < csv->used+1) {
            ssize_t ptrdiff;
            char *buf;

            csv->allocated += 4096;
            buf = (char*) realloc(csv->buffer, csv->allocated);

            if (buf == NULL) {
                av_log(s, AV_LOG_ERROR, "out of memory");
                return AVERROR(ENOMEM);
            }

            ptrdiff = buf - csv->buffer;
            csv->buffer = buf;

            csv->pkt_q.beg.ts += csv->decode_time ? ptrdiff : 0;
            csv->pkt_q.end.ts += csv->decode_time ? ptrdiff : 0;
            csv->pkt_q.beg.label += csv->decode_subtitle ? ptrdiff : 0;
            csv->pkt_q.end.label += csv->decode_subtitle ? ptrdiff : 0;
            csv->pkt_q.beg.field += ptrdiff;
            csv->pkt_q.end.field += ptrdiff;
        }

        bytesread += 1;
        numfields += c=='\0';
        csv->buffer[csv->used++] = c;
    end:
        ;
    }

    if (bytesread) {
        csv_fillq(s, &csv->pkt_q.end, csv->buffer + saveused);

        csv->numlines += 1;

        if (csv->numfields == 0)
            csv->numfields = numfields;
        else if (csv->numfields != numfields)
        {
            av_log(s, AV_LOG_ERROR, "number of fields (%d) on line %zd does not match number of channels (%d)\n",
                         numfields, csv->numlines, csv->numfields);
            return AVERROR(EINVAL);
        }

        return 0;
    }

    return AVERROR_EOF;
}


/**
 * read the first line of the csv file and check parameter given by the user.
 * The first line is tried to be parsed to set the number of channels that are
 * read and also detect whether labels have to be generated.
 *
 * @param s csv context
 */
static int csv_header(AVFormatContext *s)
{
    CSVContext *csv = s->priv_data;
    AVStream *st;
    int ret = 0;

    /**
     * initialize the structures.
     */
    csv->used   = csv->numfields = csv->allocated = csv->numlines = 0;
    csv->buffer = NULL;
    csv->flush  = false;

    /**
     *  read the first line of the input file to check if the input format
     *  is parseable.
     */
    if (ret = csv_readfields(s))
        return ret;

    if (csv->numfields == 0)
    {
        av_log(s, AV_LOG_WARNING, "empty file, nothing to decode");
        return AVERROR_EOF;
    }

    /**
     *  check whether we have time-coded input, in which case the first field
     *  designates a time-code. We assume that the sample rate is then given in
     *  nanoseconds.
     */
    if (csv->decode_time) {
        int64_t bla = 0;
        ret = csv_parse_time(s, &bla, csv->buffer);
        if (ret) return ret;
    } else
        csv->pts = 0;

    /**
     *  check if a subtitle is there and needs decoding by trying to parse the
     *  whole first field as a double. If it can be parsed no subtitle parsing
     *  is activated, otherwise it is.
     */
    if (csv->decode_subtitle == -1)
    {
        char *endptr, *label;
        if (csv->numfields > csv->decode_time) {
            label = csv->decode_time ?
                    strchr(csv->buffer, '\0')+1 :
                    csv->buffer;
            strtod(label, &endptr);
            csv->decode_subtitle = endptr == label;
        } else
            csv->decode_subtitle = false;
    }

    /**
     *  fill the queue again now that the decode_* values are known
     */
    csv_fillq(s, &csv->pkt_q.end, csv->buffer);
    csv_fillq(s, &csv->pkt_q.beg, csv->buffer);

    /**
     *  create the audiostream
     */
    if (csv->numfields - csv->decode_subtitle - csv->decode_time == 0) {
        av_log(s, AV_LOG_ERROR, "no data to decode, labels only are not supported");
        return AVERROR(EINVAL);
    }

    if (! (st = avformat_new_stream(s, NULL)) )
        return AVERROR(ENOMEM);

    st->codecpar->codec_type  = AVMEDIA_TYPE_AUDIO;
    st->codecpar->codec_id    = s->audio_codec_id == AV_CODEC_ID_NONE ?
                                                            AV_CODEC_ID_PCM_F64LE : s->audio_codec_id;
    st->codecpar->channels    = csv->numfields - csv->decode_subtitle - csv->decode_time;
    st->codecpar->sample_rate = csv->sample_rate;

    st->codecpar->bits_per_coded_sample =
        av_get_bits_per_sample(st->codecpar->codec_id);
    av_assert0(st->codecpar->bits_per_coded_sample > 0);

    st->codecpar->block_align =
        st->codecpar->bits_per_coded_sample * st->codecpar->channels / 8;

    av_assert0(st->codecpar->sample_rate > 0);
    avpriv_set_pts_info(st, 64, 1, csv->decode_time ?
            TIMEBASE_US : csv->sample_rate);

    /**
     *  create the optional subtitle stream. The sample rate is either in
     *  micro-seconds when decode_subtitle otherwise it is set to the sample
     *  rate of the audio stream.
     */
    if (csv->decode_subtitle)
    {
        if (! (st = avformat_new_stream(s, NULL)) )
            return AVERROR(ENOMEM);

        st->codecpar->codec_type = AVMEDIA_TYPE_SUBTITLE;
        st->codecpar->codec_id   = AV_CODEC_ID_WEBVTT;

        avpriv_set_pts_info(st, 64, 1, csv->decode_time ?
                TIMEBASE_US : csv->sample_rate);
    }

    return 0;
}

/**
 * test if the current block of data is enough to output a block. A block are n
 * samples that are labelled similarly, i.e. if decoding labels, lines have to
 * be read until the label changes. In this way the subtitle packet are
 * generated prior to the audio samples.
 *
 * @param s csv context
 */
static bool csv_needfields(AVFormatContext *s)
{
    CSVContext *csv = s->priv_data;

    return csv->decode_subtitle ?
        csv->pkt_q.end.label == csv->pkt_q.beg.label :
        csv->pkt_q.end.field == csv->pkt_q.beg.field;
}

/**
 * generate an audio or subtitle packet depending the current queue fill state.
 * Per default an audio packet is generated. However if labels are decoded,
 * audio packets are only generated if the labels have been changed and a
 * subtitle packet is generated first.
 *
 * @param s csv context
 * @param pkt pacet to be filled
 */
static int csv_packet(AVFormatContext *s, AVPacket *pkt)
{
# define onsubedge(csv) !strcmp(csv->pkt_q.end.label, csv->pkt_q.beg.label)
    CSVContext *csv = s->priv_data;
    AVStream *st    = s->streams[pkt->stream_index];
    int channels    = st->codecpar->channels,
            err     = 0;

    if (csv->used == 0)
        return AVERROR_EOF;

    if (!csv->flush && csv_needfields(s))
    /**
     * is more data required or are we at EOF already? In subtitle mode, we read
     * a whole block, i.e. a number of lines with matching labels.
     */
    {
        int64_t dur = 0;

        do {
            dur += 1;
            err = csv_readfields(s);
        } while (!err && csv->decode_subtitle &&
                 !strcmp(csv->pkt_q.end.label, csv->pkt_q.beg.label));

        if (err == AVERROR_EOF)
            csv->flush = true;
        else if (err)
            return err; // either EOF or fail

        if (csv->decode_subtitle)
        /**
         * emit a subtitle packet for the just read block and exit
         */
        {
            if (err = av_new_packet(pkt, strlen(csv->pkt_q.beg.label)))
                return err;

            pkt->stream_index = 1;
            pkt->pts = csv->pkt_q.beg.timestamp;
            pkt->dts = csv->pkt_q.beg.timestamp;
            pkt->duration = !csv->decode_time ? dur :
                            csv->pkt_q.end.timestamp - csv->pkt_q.beg.timestamp;
            strcpy(pkt->data, csv->pkt_q.beg.label);

            /**
             * exit early on if we are in subtitle mode
             */
            return 0;
        }
    }


    /**
     *  parse the fields of the CSV packet and emit an audio packet
     */
    char *field = csv->pkt_q.beg.field;
    int i;

    err = av_new_packet(pkt, channels * st->codecpar->bits_per_coded_sample/8);

    if (err)
        return err;

    for(i=0, pkt->size=0;
        i < channels;
        i++, pkt->size += av_get_bits_per_sample(st->codecpar->codec_id)/8,
        field += strlen(field)+1 )
        switch(st->codecpar->codec_id)
        {
        case AV_CODEC_ID_PCM_F64LE:
        case AV_CODEC_ID_PCM_F64BE:
            ((double*) pkt->data)[i] =
                strtod(field, NULL);
            break;

        case AV_CODEC_ID_PCM_F32LE:
        case AV_CODEC_ID_PCM_F32BE:
            ((float*) pkt->data)[i] =
                strtof(field, NULL);
            break;

        case AV_CODEC_ID_PCM_U32LE:
        case AV_CODEC_ID_PCM_U32BE:
            ((uint32_t*) pkt->data)[i] = (uint32_t)
                strtoll(field, NULL, 10);
            break;

        case AV_CODEC_ID_PCM_S32LE:
        case AV_CODEC_ID_PCM_S32BE:
            ((int32_t*) pkt->data)[i] = (int32_t)
                strtoll(field, NULL, 10);
            break;

        case AV_CODEC_ID_PCM_U16LE:
        case AV_CODEC_ID_PCM_U16BE:
            ((uint16_t*) pkt->data)[i] = (uint16_t)
                strtol(field, NULL, 10);
            break;

        case AV_CODEC_ID_PCM_S16LE:
        case AV_CODEC_ID_PCM_S16BE:
            ((int16_t*) pkt->data)[i] = (int16_t)
                strtol(field, NULL, 10);
            break;

        case AV_CODEC_ID_PCM_U8:
            ((uint8_t*) pkt->data)[i] = (uint8_t)
                strtol(field, NULL, 10);
            break;

        case AV_CODEC_ID_PCM_S8:
            ((int8_t*) pkt->data)[i] = (int8_t)
                strtol(field, NULL, 10);
            break;

        default:
            av_log(s, AV_LOG_ERROR, "unknown codec: %s\n", 
                   avcodec_get_name(st->codecpar->codec_id));
        }

    pkt->stream_index = 0;
    pkt->pts = pkt->dts = csv->pkt_q.beg.timestamp;
    csv->pts += !csv->decode_time; // this can only be done here

    if (field < csv->buffer + csv->used)
        csv_fillq(s, &csv->pkt_q.beg, field);
    else
        csv->used = 0;

    return 0;
}

static const AVOption csv_options[] = {
    { "sample_rate", "sample rate of the input", offsetof(CSVContext, sample_rate), AV_OPT_TYPE_INT, {.i64 = 1}, 1, INT_MAX, AV_OPT_FLAG_DECODING_PARAM },
    { "delimiters",  "the field separator characters", offsetof(CSVContext, delims), AV_OPT_TYPE_STRING, {.str = " \t"}, 0, 0, AV_OPT_FLAG_DECODING_PARAM },
    { "subtitle",    "whether the first field shall be interpreted as a subtitle, default is auto-detect", offsetof(CSVContext, decode_subtitle), AV_OPT_TYPE_BOOL, {.i64 = -1}, -1, 1, AV_OPT_FLAG_DECODING_PARAM },
    { "timecoded",   "whether to interpret the first field as a timecode", offsetof(CSVContext, decode_time), AV_OPT_TYPE_BOOL, {.i64 = 0}, 0, 1, AV_OPT_FLAG_DECODING_PARAM },
    { NULL },
};

static const AVClass csv_demuxer_class = {
    .class_name = "CSV demuxer",
    .item_name  = av_default_item_name,
    .option     = csv_options,
    .version    = LIBAVUTIL_VERSION_INT,
};

AVInputFormat ff_csv_demuxer = {
    .name           = "csv",
    .long_name      = NULL_IF_CONFIG_SMALL("Comma Separated Values"),
    .extensions     = "csv",
    .flags          = AVFMT_NOBINSEARCH|AVFMT_NOGENSEARCH|AVFMT_NO_BYTE_SEEK|AVFMT_VARIABLE_FPS|AVFMT_TS_DISCONT|AVFMT_ALLOW_FLUSH,
    .read_header    = csv_header,
    .read_packet    = csv_packet,
    .priv_data_size = sizeof(CSVContext),
    .mime_type      = "text/csv,text/plain,application/csv,text/comma-separated-values", // http://mimeapplication.net/text-csv
    .priv_class     = &csv_demuxer_class,
};
