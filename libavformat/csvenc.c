/*
 * CSV Muxer
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
#include "libavcodec/bytestream.h"
#include "libavcodec/avcodec.h"
#include "libavutil/opt.h"
#include "avformat.h"
#include "internal.h"

#define DELIM " "

/**
 * current state of the muxer
 */
typedef struct CSVState {
    int64_t      pts;
    AVPacketList *labels_head; // queue subtitle packets
    AVPacketList *labels_tail;
    AVPacket     **tracks;     // queue all samples of tracks
    uint8_t      **streams;    // keep track of input streams
    bool         hassub;       // is there a subtitle stream
} CSVState;

/**
 * check if the audio stream is in native byteoder or needs to be converted
 *
 * @param s stream to check
 */
static bool is_nativeaudio(AVStream *s)
{
    enum AVCodecID cid = s->codecpar->codec_id;
#if HAVE_BIGENDIAN
    return cid == AV_CODEC_ID_PCM_F32BE ||
           cid == AV_CODEC_ID_PCM_F64BE ||
           cid == AV_CODEC_ID_PCM_S16BE ||
           cid == AV_CODEC_ID_PCM_U16BE ||
           cid == AV_CODEC_ID_PCM_S32BE ||
           cid == AV_CODEC_ID_PCM_U32BE ||
           cid == AV_CODEC_ID_PCM_U8    || cid == AV_CODEC_ID_PCM_S8;

#else
    return cid == AV_CODEC_ID_PCM_F32LE ||
           cid == AV_CODEC_ID_PCM_F64LE ||
           cid == AV_CODEC_ID_PCM_S16LE ||
           cid == AV_CODEC_ID_PCM_U16LE ||
           cid == AV_CODEC_ID_PCM_S32LE ||
           cid == AV_CODEC_ID_PCM_U32LE ||
           cid == AV_CODEC_ID_PCM_U8    || cid == AV_CODEC_ID_PCM_S8;
#endif
}

/**
 * check if the stream is a subtitle
 *
 * @param s stream to check
 */
static bool is_subtitle(AVStream *s) 
{
    return s->codecpar->codec_type == AVMEDIA_TYPE_SUBTITLE;
}

/**
 * check if the input stream can actually be converted to a CSV format
 *
 * @param s csv context
 */
static av_cold int csv_write_header(struct AVFormatContext *s)
{
    CSVState *csv = s->priv_data;
    size_t nb_subs = 0, nb_auds = 0, sample_rate = 0;

    for (size_t i=0; i < s->nb_streams; i++)
    {
        nb_subs += is_subtitle(s->streams[i]);
        nb_auds += is_nativeaudio(s->streams[i]);

        if (!is_nativeaudio(s->streams[i]) && !is_subtitle(s->streams[i]))
        {
            av_log(s, AV_LOG_ERROR, "This muxer only supports PCM (in native byte order) and subtitle streams.\n");
            return AVERROR(EINVAL);
        }

        if (is_nativeaudio(s->streams[i]))
            if (sample_rate == 0)
                sample_rate = s->streams[i]->codecpar->sample_rate;
            else if (sample_rate != s->streams[i]->codecpar->sample_rate)
            {
                av_log(s, AV_LOG_ERROR, "Sample rates for all stream must be the same");
                return AVERROR(EINVAL);
            }
    }

    if (nb_subs > 1)
    {
        av_log(s, AV_LOG_ERROR, "This muxer only supports a single subtitle stream.\n");
        return AVERROR(EINVAL);
    }

    /* buffer all tracks to mux them into one output */
    csv->labels_head = csv->labels_tail = NULL;
    csv->tracks  = av_mallocz_array(s->nb_streams, sizeof(csv->tracks[0]));
    csv->streams = av_mallocz_array(s->nb_streams, sizeof(csv->streams[0]));
    csv->pts = 0;

    for (size_t i=0; i < s->nb_streams; i++)
        csv->tracks[i] = av_packet_alloc();

    csv->hassub = nb_subs > 0;

    return 0;
}

/**
 * copy dst to src, escaping all characters in set.
 *
 * @param dst destination packet
 * @param src source packet
 * @param set characters to be escaped
 */
static int copy_and_escape(AVPacket *dst, AVPacket *src, const char *set)
{
    char *buf = src->data;
    const char *esc = "";
    size_t  n = 0, i;

    av_copy_packet(dst, src);
    for (i=0, n=0; i < src->size; i++, n++)
    {
        char c = buf[i];

        if (*esc == '\\' ||
                *esc == '"' && c == '"')
            esc = "";
        else if (!*esc && (c == '\\' || c == '"'))
            esc = &buf[i];
        else if (!*esc && strchr(set, c) != NULL)
        {
            av_grow_packet(dst, 1);
            dst->data[n++] = '\\';
        }

        dst->data[n] = c;
    }

    return 0;
}

/**
 * enqueue a label packet into the queue
 *
 * @param s csv context
 * @param pkt packet to be queued
 */
static int csv_put_label(struct AVFormatContext *s, AVPacket *pkt)
{
    AVPacketList *item = malloc(sizeof(AVPacketList));
    CSVState *csv      = s->priv_data;

    if (item == NULL) {
        av_log(s, AV_LOG_ERROR, "unable to allocate memory");
        return AVERROR(ENOMEM);
    }

    for (size_t i=0; i < s->nb_streams; i++)
      if (csv->tracks[i]->pts > pkt->pts) {
        av_log(s, AV_LOG_ERROR, "got a subtitle packet after an audio packet."
               "Try increasing max_interleave_delta, e.g. with "
               "-max_interleave_delta 100000000!\n");
        return AVERROR(EINVAL);
      }

    copy_and_escape(&item->pkt, pkt, DELIM);
    item->next = NULL;

    if (csv->labels_head == NULL)
        csv->labels_head = csv->labels_tail = item;
    else
        csv->labels_tail = csv->labels_tail->next = item;

    return 0;
}

/**
 * dequeue and dereference a label from the csv context
 *
 * @param s csv context
 */
static int csv_pop_label(struct AVFormatContext *s)
{
    CSVState *csv            = s->priv_data;
    AVPacketList *item = csv->labels_head;

    if (item == NULL) {
        av_log(s, AV_LOG_ERROR, "unable to remove item from empty list");
        return AVERROR(EINVAL);
    }

    if (item == csv->labels_tail)
        csv->labels_tail = item->next;
    csv->labels_head = item->next;

    // unref is fine for a copied packet
    av_packet_unref(&item->pkt);
    free(item);

    return 0;
}

/**
 * convert all packets into a CSV line and output
 *
 * @param s csv context
 */
static int csv_flush_buffers(struct AVFormatContext *s)
{
    CSVState *csv = s->priv_data;
    AVRational *timebase = NULL;
    int64_t duration = -1;

    /*
     * select the minimum duration of all streams and check if all them carry
     * data. If so empty to duration and return ENODATA. In the case that all
     * durations are the same exit cleanly.
     */
    for (size_t i=0; i < s->nb_streams; i++)
    {
        AVStream *p = s->streams[i];
        AVPacket *t = csv->tracks[i];

        if (p->codecpar->codec_type != AVMEDIA_TYPE_AUDIO)
            continue;

        if (duration == -1)
            duration = t->duration;
        else if (duration == 0)
            return 1;
        else if (duration != t->duration) {
            duration = t->duration < duration ? t->duration : duration;
            av_log(s, AV_LOG_WARNING, "duration of stream %zd is different "
                                      "decreasing total duration\n", i);
            //return 1;
        }

        if (timebase == NULL)
            timebase = &p->time_base;
        else if (timebase->num != p->time_base.num ||
                 timebase->den != p->time_base.den)
        {
            av_log(s, AV_LOG_ERROR, "time bases for streams do not match,"
                                    " running at the same rate?\n");
            return AVERROR(EINVAL);
        }
    }

    if (timebase == NULL) /* this is equivalent to no available audio-stream */
        return 0;

    /*
     * wind the pts forward and select the current subtitle accordingly. The
     * duration to forward the pts is detected beforehand and checked to match
     * since we can't merge multiple streams with differing frame rates.
     */
    for(duration += csv->pts; csv->pts < duration; csv->pts++)
    {
        char *label = (char*) "NULL";

        if (csv->hassub)
        {
            bool removed = false;
            /*
             * remove the oldest subtitle if its end is past the current pts. Only do
             * this once if the pts is equal.
             */
            for (AVPacketList *item = csv->labels_head; item;)
            {
                AVPacket *pkt = &item->pkt;
                int end = av_compare_ts(csv->pts, *timebase, pkt->pts+
                                        (pkt->duration==0 ? 1 : pkt->duration),
                                        s->streams[pkt->stream_index]->time_base),
                     eq = av_compare_ts(csv->pts, *timebase, pkt->pts,
                                        s->streams[pkt->stream_index]->time_base),
                    beg = 0;

                if (end >= 0 && (!removed || !eq))
                {
                    item = item->next;
                    csv_pop_label(s);
                    removed = true;
                    continue;
                }

                beg = av_compare_ts(csv->pts, *timebase, pkt->pts,
                                    s->streams[pkt->stream_index]->time_base);

                if (beg >= 0)
                  label = (char*) pkt->data;

                break;
            }

            avio_write(s->pb, label, strlen(label));
            avio_write(s->pb, DELIM, sizeof(DELIM)-1);
        }

        /*
         * print each sample onto the current line
         */
        for (size_t j=0; j < s->nb_streams; j++)
        {
            enum AVCodecID f = s->streams[j]->codecpar->codec_id;
            size_t channels  = s->streams[j]->codecpar->channels;
            char buf[512];

            if (!is_nativeaudio(s->streams[j]))
                continue;

#           define print(type, fmt) do {\
                while(channels--) {\
                avio_write(s->pb, buf, snprintf(buf, sizeof(buf),\
                                     fmt, *((type*) csv->streams[j])));\
                csv->streams[j] += sizeof(type);\
                }} while(0)


            switch(f)
            {
                case AV_CODEC_ID_PCM_S8:
                    print(int8_t, "%hhd"DELIM);
                    break;

                case AV_CODEC_ID_PCM_U8:
                    print(uint8_t, "%hhu"DELIM);
                    break;

                case AV_CODEC_ID_PCM_S16LE:
                case AV_CODEC_ID_PCM_S16BE:
                    print(int16_t, "%hd"DELIM);
                    break;

                case AV_CODEC_ID_PCM_U16LE:
                case AV_CODEC_ID_PCM_U16BE:
                    print(uint16_t, "%hu"DELIM);
                    break;

                case AV_CODEC_ID_PCM_S32LE:
                case AV_CODEC_ID_PCM_S32BE:
                    print(int32_t, "%d"DELIM);
                    break;

                case AV_CODEC_ID_PCM_U32LE:
                case AV_CODEC_ID_PCM_U32BE:
                    print(uint32_t, "%d"DELIM);
                    break;

                case AV_CODEC_ID_PCM_F32LE:
                case AV_CODEC_ID_PCM_F32BE:
                    print(float, "%f"DELIM);
                    break;

                case AV_CODEC_ID_PCM_F64LE:
                case AV_CODEC_ID_PCM_F64BE:
                    print(double, "%f"DELIM);
                    break;

                default:
                    av_log(s, AV_LOG_ERROR, "unknown codec %s\n", avcodec_get_name(f));
                    return AVERROR(EINVAL);
            }
        }

        avio_write(s->pb, "\n", 1);
    }

    return 0;
}

/**
 * enqueue packet until all audio tracks have been read and until a subtitle
 * packet that is later then the first audio packet is there. In that case, or
 * when there a no more audio packets, flush the current buffer.
 *
 * @param s csv context
 * @param pkt the current packet sent from the current graph
 */
static int csv_write_packet(struct AVFormatContext *s, AVPacket *pkt)
{
    CSVState *csv = s->priv_data;
    AVPacket *track = csv->tracks[pkt->stream_index];
    AVCodecParameters *p = s->streams[pkt->stream_index]->codecpar;

    /* buffer all subtitle packets until we have enough audio packets that
     * can be merged and printed. The subtitle buffer will then be emptied
     * in sync with the audio packets. We assume that the audio packets are
     * written interleaved, i.e. that there is a maximum of one packet
     * difference between several streams. */
    if (p->codec_type == AVMEDIA_TYPE_SUBTITLE)
        return csv_put_label(s, pkt);
    else if (p->codec_type != AVMEDIA_TYPE_AUDIO)
        return 0;

    /*
     * At this point only audio is passed, flush the buffers first if there is no
     * space to store the current packet. This ensures that we (hopefully) have
     * seen all subtitle packets prior to the pts+duration. csv_flush_buffers
     * additionally checks if one the audio stream is beyond it's duration and
     * return ENODATA then.
     *
     * Also check whether the pts is increasing by one each time, if not the
     * sample_rate is not constant, which may or may not be what the user wants.
     */
    if (track->data != NULL)
    {
        int ret;

        // if (pkt->pts - track->pts > track->duration)
        //     av_log(s, AV_LOG_WARNING, "sample rate is not constant (pts diff was %ld)"
        //                  " maybe use -af aresample=async=XXX to sync audio?\n", pkt->pts-csv->pts);

        if (ret = csv_flush_buffers(s))
            return ret;

        for (size_t i=0; i < s->nb_streams; i++)
            av_packet_unref(csv->tracks[i]);
    }

    av_packet_ref(track, pkt);
    csv->streams[pkt->stream_index] = pkt->data;

    return 0;
}

/**
 * flush the buffer if there is something left
 *
 * @param s csv conctext
 */
static int csv_write_trailer(struct AVFormatContext *s)
{
    CSVState *csv = s->priv_data;
    csv_flush_buffers(s);

    for (size_t i=0; i < s->nb_streams; i++)
        av_packet_unref(csv->tracks[i]);

    return 0;
}

AVOutputFormat ff_csv_muxer = {
        .name               = "csv",
        .long_name          = NULL_IF_CONFIG_SMALL("Comma Separated Values"),
        .extensions         = "csv",
#  if HAVE_BIGENDIAN
        .audio_codec        = AV_CODEC_ID_PCM_F64BE,
#  else
        .audio_codec        = AV_CODEC_ID_PCM_F64LE,
#  endif
        .video_codec        = AV_CODEC_ID_NONE,
        .subtitle_codec     = AV_CODEC_ID_WEBVTT,
        .flags              = AVFMT_NO_BYTE_SEEK,
        .priv_data_size     = sizeof(CSVState),

        .write_header       = csv_write_header,
        .write_trailer      = csv_write_trailer,
        .write_packet       = csv_write_packet,
};
