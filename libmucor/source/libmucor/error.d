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

import std.format;
import std.stdio;
import core.stdc.stdlib : exit;

/// Log levels.
enum LogLevel // @suppress(dscanner.style.phobos_naming_convention)
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
void log(Args...)(LogLevel severity, string context, string fmt, Args args)
{
    final switch (severity)
    {
    case LogLevel.Err:
        log_err(context, fmt, args);
        break;
    case LogLevel.Warn:
        log_warn(context, fmt, args);
        break;
    case LogLevel.Info:
        log_info(context, fmt, args);
        break;
    case LogLevel.Debug:
        log_debug(context, fmt, args);
        break;
    case LogLevel.Trace:
        log_trace(context, fmt, args);
        break;
    case LogLevel.Off:
        break;
    }
}

// pragma(inline, true):
/**! Logs an event with severity Err and default context. Parameters: format, ... */
//#define log_err(...) log(Err, __func__, __VA_ARGS__)
void log_err(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= LogLevel.Err)
    {
        string open_err_color = "\x1b[0;31m";
        string close_color = "\x1b[0m";
        fmt = format("[Err::%s]: ", ctx) ~ fmt;
        stderr.writeln(open_err_color, format(fmt, args), close_color);
        debug throw new Exception("An error occured");
        else exit(1);
    }

}

// pragma(inline, true):
/**! Logs an event with severity Err and default context. Parameters: format, ... */
//#define log_err(...) log(Err, __func__, __VA_ARGS__)
void log_err_no_exit(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= LogLevel.Err)
    {
        string open_err_color = "\x1b[0;31m";
        string close_color = "\x1b[0m";
        fmt = format("[Err::%s]: ", ctx) ~ fmt;
        stderr.writeln(open_err_color, format(fmt, args), close_color);
    }

}
/**! Logs an event with severity Warn and default context. Parameters: format, ... */
//#define log_warn(...) log(Warn, __func__, __VA_ARGS__)
void log_warn(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= LogLevel.Warn)
    {
        string open_warn_color = "\x1b[0;33m";
        string close_color = "\x1b[0m";
        fmt = format("[Warn::%s]: ", ctx) ~ fmt;
        stderr.writeln(open_warn_color, format(fmt, args), close_color);
    }
}

/**! Logs an event with severity Info and default context. Parameters: format, ... */
//#define log_info(...) log(Info, __func__, __VA_ARGS__)
void log_info(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= LogLevel.Info)
    {
        string open_info_color = "\x1b[0;32m";
        string close_color = "\x1b[0m";
        fmt = format("[Info::%s]: ", ctx) ~ fmt;
        stderr.writeln(open_info_color, format(fmt, args), close_color);
    }
}

/**! Logs an event with severity Debug and default context. Parameters: format, ... */
//#define log_debug(...) log(Debug, __func__, __VA_ARGS__)
void log_debug(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level >= LogLevel.Debug)
    {
        string open_debug_color = "\x1b[0;36m";
        string close_color = "\x1b[0m";
        fmt = format("[Debug::%s]: ", ctx) ~ fmt;
        stderr.writeln(open_debug_color, format(fmt, args), close_color);
    }
}

/**! Logs an event with severity Trace and default context. Parameters: format, ... */
//#define log_trace(...) log(Trace, __func__, __VA_ARGS__)
void log_trace(Args...)(string ctx, string fmt, Args args)
{
    if (get_log_level == LogLevel.Trace)
    {
        string open_trace_color = "\x1b[1;36m";
        string close_color = "\x1b[0m";
        fmt = format("[Trace::%s]: ", ctx) ~ fmt;
        stderr.writeln(open_trace_color, format(fmt, args), close_color);
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
    assertThrown(log_err(__FUNCTION__, "Test: err"));
}
