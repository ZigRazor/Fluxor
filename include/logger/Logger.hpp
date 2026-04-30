#pragma once

#include <cstdio>
#include <cstring>
#include <mutex>
#include <chrono>
#include <ctime>

namespace Fluxor
{

    // --------------------------------------------------
    // Log levels
    // --------------------------------------------------
    enum class LoggerLevel : uint8_t
    {
        Trace,
        Debug,
        Info,
        Warn,
        Error,
        Fatal,
        Off
    };

#ifndef FLUXORLOGGER_ACTIVE_LEVEL
#define FLUXORLOGGER_ACTIVE_LEVEL Fluxor::LoggerLevel::Trace
#endif

    // --------------------------------------------------
    // Small fixed buffer (zero allocation)
    // --------------------------------------------------
    template <size_t N>
    class FixedBuffer
    {
    public:
        void append(const char *data, size_t len)
        {
            if (size_ + len > N)
                return; // truncate
            memcpy(buffer_ + size_, data, len);
            size_ += len;
        }

        void append_char(char c)
        {
            if (size_ < N)
                buffer_[size_++] = c;
        }

        const char *data() const { return buffer_; }
        size_t size() const { return size_; }

        void clear() { size_ = 0; }

    private:
        char buffer_[N];
        size_t size_ = 0;
    };

    // --------------------------------------------------
    // Sink interface
    // --------------------------------------------------
    class Sink
    {
    public:
        virtual ~Sink() = default;
        virtual void write(const char *data, size_t len) = 0;
    };

    // --------------------------------------------------
    // Console sink (no allocation)
    // --------------------------------------------------
    class ConsoleSink : public Sink
    {
    public:
        void write(const char *data, size_t len) override
        {
            fwrite(data, 1, len, stdout);
            fwrite("\n", 1, 1, stdout);
        }
    };

    // --------------------------------------------------
    // Logger
    // --------------------------------------------------
    class Logger
    {
    public:
        static Logger &instance()
        {
            static Logger inst;
            return inst;
        }

        void set_level(LoggerLevel lvl) { level_ = lvl; }

        void set_sink(Sink *sink) { sink_ = sink; }

        template <typename... Args>
        void log(LoggerLevel lvl,
                 const char *file,
                 int line,
                 const char *func,
                 const char *fmt,
                 Args... args)
        {
            if (lvl < level_)
                return;

            FixedBuffer<1024> buf;

            append_timestamp(buf);
            append_level(buf, lvl);

            buf.append(" [", 2);
            buf.append(func, strlen(func));
            buf.append("] ", 2);

            // format message (no heap alloc)
            int written = snprintf(temp_, sizeof(temp_), fmt, args...);
            if (written > 0)
            {
                buf.append(temp_, static_cast<size_t>(written));
            }

            std::lock_guard<std::mutex> lock(mutex_);
            sink_->write(buf.data(), buf.size());
        }

    private:
        Logger() : sink_(&console_) {}

        void append_timestamp(FixedBuffer<1024> &buf)
        {
            auto now = std::chrono::system_clock::now();
            auto seconds = time_point_cast<std::chrono::seconds>(now);
            auto micros = duration_cast<std::chrono::microseconds>(now - seconds).count();
            auto t = std::chrono::system_clock::to_time_t(now);

            std::tm tm{};
#if defined(_WIN32)
            localtime_s(&tm, &t);
#else
            localtime_r(&t, &tm);
#endif

            char timebuf[32];
            strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tm);

            // final format: HH:MM:SS.ffffff
            char finalbuf[64];
            int len = snprintf(finalbuf, sizeof(finalbuf),
                               "%s.%06lld", timebuf, (long long)micros);

            buf.append(finalbuf, static_cast<size_t>(len));
            buf.append(" ", 1);
        }

        void append_level(FixedBuffer<1024> &buf, LoggerLevel lvl)
        {
            const char *s = "UNK";
            switch (lvl)
            {
            case LoggerLevel::Trace:
                s = "TRACE";
                break;
            case LoggerLevel::Debug:
                s = "DEBUG";
                break;
            case LoggerLevel::Info:
                s = "INFO";
                break;
            case LoggerLevel::Warn:
                s = "WARN";
                break;
            case LoggerLevel::Error:
                s = "ERROR";
                break;
            case LoggerLevel::Fatal:
                s = "FATAL";
                break;
            default:
                break;
            }
            buf.append(s, strlen(s));
        }

    private:
        LoggerLevel level_ = LoggerLevel::Info;
        Sink *sink_;
        ConsoleSink console_;
        std::mutex mutex_;

        char temp_[512]; // formatting buffer (reused, no alloc)
    };

} // namespace Fluxor

#define LOG_TRACE(fmt, ...) \
    Fluxor::Logger::instance().log(Fluxor::LoggerLevel::Trace, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_DEBUG(fmt, ...) \
    Fluxor::Logger::instance().log(Fluxor::LoggerLevel::Debug, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_INFO(fmt, ...) \
    Fluxor::Logger::instance().log(Fluxor::LoggerLevel::Info, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_WARN(fmt, ...) \
    Fluxor::Logger::instance().log(Fluxor::LoggerLevel::Warn, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_ERROR(fmt, ...) \
    Fluxor::Logger::instance().log(Fluxor::LoggerLevel::Error, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)