module libmucor.error;

/// \file htslib/log.h
/// Configuration of log levels.
/* The MIT License
Copyright (C) 2017 Genome Research Ltd.

Author: Anders Kaplan

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import std.stdio;
import core.stdc.stdlib : exit, free;
import libmucor.utility;

@nogc nothrow @trusted pragma(inline, true) : // @suppress(dscanner.trust_too_much)
/// Log levels.
enum LogLevel
{
    Off = 0, ///< All logging disabled.
    Err = 1, ///< Logging of errs only.
    Warn = 3, ///< Logging of errs and warns.
    Info = 4, ///< Logging of errs, warns, and normal but significant events.
    Debug = 5, ///< Logging of all except the most detailed debug events.
    Trace = 6 ///< All logging enabled.
}

/// Sets the selected log level.
void set_log_level(LogLevel level)
{
    verbose = cast(int) level;
}

/// Gets the selected log level.
LogLevel get_log_level()
{
    return cast(LogLevel) verbose;
}

/// Selected log level.
/*!
 * One of the * values. The default is Warn.
 * \note Avoid direct use of this variable. Use set_log_level and hts_get_log_level instead.
 */
shared int verbose = 4;

static immutable string[] LogLevelMsg = ["","[Err::%s]: %s","", "[Warn::%s]: %s", "[Info::%s]: %s", "[Debug::%s]: %s", "[Trace::%s]: %s"];
static immutable string[] LogLevelColorPrefix = ["","\x1b[0;31m", "", "\x1b[0;33m", "\x1b[0;32m", "\x1b[0;36m", "\x1b[1;36m"];
static immutable string LogLevelColorPostfix = "\x1b[0m";

/*! Logs an event.
* \param severity      Severity of the event:
*                      - Err means that something went wrong so that a task could not be completed.
*                      - Warn means that something unexpected happened, but that execution can continue, perhaps in a degraded mode.
*                      - Info means that something normal but significant happened.
*                      - Debug means that something normal and insignificant happened.
*                      - Trace means that something happened that might be of interest when troubleshooting.
* \param context       Context where the event occurred. Typically set to "__func__".
* \param format        Format string with placeholders, like printf.
*/

void log(LogLevel level, Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= level)
    {
        static if(args.length == 0){
            auto tmp = format(LogLevelMsg[level], ctx, fmt);
            auto tmp2 = format("%s%s%s", LogLevelColorPrefix[level], tmp, LogLevelColorPostfix);
            ewrite(tmp2);
            ewrite("\n");
            free(cast(void*)tmp.ptr);
            free(cast(void*)tmp2.ptr);
        } else {
            auto tmp = format(LogLevelMsg[level], ctx, fmt);
            auto tmp2 = format(tmp, args);
            auto tmp3 = format("%s%s%s", LogLevelColorPrefix[level], tmp2, LogLevelColorPostfix);
            ewrite(tmp3);
            ewrite("\n");
            free(cast(void*)tmp.ptr);
            free(cast(void*)tmp2.ptr);
            free(cast(void*)tmp3.ptr);
        }
    }

}

void log_err_no_exit(Args...)(string ctx, string fmt, Args args) {
    log!(LogLevel.Err, Args)(ctx, fmt, args);
}
void log_warn(Args...)(string ctx, string fmt, Args args) {
    log!(LogLevel.Warn, Args)(ctx, fmt, args);
}
void log_info(Args...)(string ctx, string fmt, Args args) {
    log!(LogLevel.Info, Args)(ctx, fmt, args);
}
void log_debug(Args...)(string ctx, string fmt, Args args) {
    log!(LogLevel.Debug, Args)(ctx, fmt, args);
}
void log_trace(Args...)(string ctx, string fmt, Args args) {
    log!(LogLevel.Trace, Args)(ctx, fmt, args);
}
// pragma(inline, true):
/**! Logs an event with severity Err and default context. Parameters: format, ... */
//#define log_err(...) log(Err, __func__, __VA_ARGS__)
void log_err(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= LogLevel.Err)
    {
        static if(args.length == 0){
            auto tmp = format(LogLevelMsg[LogLevel.Err], ctx, fmt);
            auto tmp2 = format("%s%s%s", LogLevelColorPrefix[LogLevel.Err], tmp, LogLevelColorPostfix);
            ewrite(tmp2);
            ewrite("\n");
            free(cast(void*)tmp.ptr);
            free(cast(void*)tmp2.ptr);
        } else {
            auto tmp = format(LogLevelMsg[LogLevel.Err], ctx, fmt);
            auto tmp2 = format(tmp, args);
            auto tmp3 = format("%s%s%s", LogLevelColorPrefix[LogLevel.Err], tmp2, LogLevelColorPostfix);
            ewrite(tmp3);
            ewrite("\n");
            free(cast(void*)tmp.ptr);
            free(cast(void*)tmp2.ptr);
            free(cast(void*)tmp3.ptr);
        }
        debug {}
        else exit(1);
    }

}

//
unittest
{
    import std.exception : assertThrown;

    set_log_level(LogLevel.Trace);

    log_trace(__FUNCTION__, "Test: trace");
    log_debug(__FUNCTION__, "Test: debug");
    log_info(__FUNCTION__, "Test: info");
    log_warn(__FUNCTION__, "Test: warn");
    log_err_no_exit(__FUNCTION__, "Test: err");
}