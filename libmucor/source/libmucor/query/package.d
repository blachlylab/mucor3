module libmucor.query;
import libmucor.error;
import std.range : repeat, take;
import std.array : array;

public import libmucor.query.expr.query;
public import libmucor.query.eval;

void queryErr(string query, ulong idx, string err, string example = "")
{
    log_err_no_exit("parseQuery", "Query sytax error!");
    log_err_no_exit("parseQuery", err);
    if (example != "")
    {
        log_err_no_exit("parseQuery", "Example: %s", example);
    }
    log_err_no_exit("parseQuery", "Problem found here: " ~ (' '.repeat.take(idx).array.idup) ~ "v");
    log_err("parseQuery", "QueryFragment:      %s", query);
}
