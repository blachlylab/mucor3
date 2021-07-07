module varquery.query;

import std.stdio;
import std.algorithm.setops;
import std.regex;
import std.algorithm : map, fold;
import std.array : array;
import std.string : replace;
import std.conv : to;
import std.format : format;
import std.typecons : Tuple;

import varquery.singleindex;
import varquery.invertedindex;


/**
* Logic for parsing string query and filter results using inverted index.
* Uses NOT, AND, and OR operators with key and values represent as key:value
* e.g.: key1=val1 -> get all records where key1=val1
* e.g.: key1=val1 AND key2=val2 -> get all records where key1=val1 and key2=val2
* e.g.: key1=val1 OR key2=val2 -> get all records where key1=val1 or key2=val2
* e.g.: key1=(val1 OR key2) -> get all records where key1=val1 or key1=val2
* can get complicated
* e.g.: NOT (key1:val1 AND key2:(val2 OR val3) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)
*
* Operators: =, :, >, >=, <, <=, AND, NOT, OR
* Other specials: (, )
**/

/// Capture a Key
enum KEY_CAP_PATTERN = `([A-Za-z0-9._/*]+)`;

/// Capture a numeric
enum NUM_CAP_PATTERN = `([-]?[0-9.ef]+)`;

/// No operators (capture a value)
enum VAL_CAP_PATTERN = `([^\s\:=<>\(\)]+)`;

/// Regex patterns for the base queries (can be done independently)
auto simple_patterns = regex([

    //key1 = val1 : val2
    `%s[\s]*=[\s]*%s[\s]*:[\s]*%s`.format(KEY_CAP_PATTERN, NUM_CAP_PATTERN, NUM_CAP_PATTERN), //range

    //key1 = (val1 AND val2)
    `%s[\s]*=[\s]*\(((?:[\s]*%s[\s]+AND[\s]+)+(?:[^\s]+))\)`.format(KEY_CAP_PATTERN, VAL_CAP_PATTERN), //and

    //key1 = (val1 OR val2)
    `%s[\s]*=[\s]*\(((?:[\s]*%s[\s]+OR[\s]+)+(?:[^\s]+))\)`.format(KEY_CAP_PATTERN, VAL_CAP_PATTERN), //or

    //key1 = val1
    `%s[\s]*=[\s]*%s`.format(KEY_CAP_PATTERN, VAL_CAP_PATTERN), //simple

    //key1 == val1
    `%s[\s]*==[\s]*%s`.format(KEY_CAP_PATTERN, VAL_CAP_PATTERN), //numeric equals

    //key1 > val1
    `%s[\s]*>[\s]*%s`.format(KEY_CAP_PATTERN, NUM_CAP_PATTERN), //GT

    //key1 >= val1
    `%s[\s]*>=[\s]*%s`.format(KEY_CAP_PATTERN, NUM_CAP_PATTERN), //GTE

    //key1 < val1
    `%s[\s]*<[\s]*%s`.format(KEY_CAP_PATTERN, NUM_CAP_PATTERN), //LT

    //key1 <= val1
    `%s[\s]*<=[\s]*%s`.format(KEY_CAP_PATTERN, NUM_CAP_PATTERN), //LTE
]);

/// Describes a query subunit's
/// type for parsing
enum QueryType
{
    Equal,
    EqualsNumeric,
    Range,
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,
    AND,
    OR,
    NOT
}

/// Query subunit
alias Query = Tuple!(string, "key", string[], "values", QueryType, "type");

/// Query parser results
alias QueryParserResult = Tuple!(string, "query", string, "leftover", Query[], "results");


/// Match simple/base queries and extract
auto parseSimpleQueries(string query)
{
    QueryParserResult res;
    res.query = query;
    auto i = 0;
    // loop over all patterns matched
    foreach(m;query.matchAll(simple_patterns)){
        query = query.replace(m[0],i.to!string);
        switch(m.whichPattern){
            case 1: //range
                res.results ~= Query(m[1],[m[2], m[3]], QueryType.Range);
                break;
            case 2: //and
                string[] vals = m[2].splitter(regex(`[\s]+AND[\s]+`)).array;
                res.results ~= Query(m[1], vals, QueryType.AND);
                break;
            case 3: //or
                string[] vals = m[2].splitter(regex(`[\s]+OR[\s]+`)).array;
                res.results ~= Query(m[1], vals, QueryType.OR);
                break;
            case 4: //simple
                res.results ~= Query(m[1], [m[2]], QueryType.Equal);
                break;
            case 5: //equals numeric
                res.results ~= Query(m[1], [m[2]], QueryType.EqualsNumeric);
                break;
            case 6: //GT
                res.results ~= Query(m[1], [m[2]], QueryType.GreaterThan);
                break;
            case 7: //GTE
                res.results ~= Query(m[1], [m[2]], QueryType.GreaterThanEqual);
                break;
            case 8: //LT
                res.results ~= Query(m[1], [m[2]], QueryType.LessThan);
                break;
            case 9: //LTE
                res.results ~= Query(m[1], [m[2]], QueryType.LessThanEqual);
                break;
            default:
                throw new Exception("No simple patterns identified.");
        }
        i++;
    }
    res.leftover = query;
    return res;
}

unittest
{
    auto q1 = "key=val";
    auto q2 = "key=0:1";
    auto q3 = "key=(val AND val2)";
    auto q4 = "key=(val OR val2)";
    auto q5 = "(key=val OR key2=val2)";
    auto q6 = "(key=val AND key2=val2)";
    auto q7 = "NOT (key=val OR key2=val2)";
    auto q8 = "(NOT key=val OR NOT key2=val2)";
    auto q9 = "NOT (key=val AND key2=val2)";
    auto q10 = "(NOT key=val AND NOT key2=val2)";
    auto q11 = "key=val OR key2=val2";
    auto q12 = "key=val AND key2=val2";

    auto q13 = "key==1";

    assert(parseSimpleQueries(q1) == QueryParserResult(q1,"0",[Query("key",["val"],QueryType.Equal)]));
    assert(parseSimpleQueries(q2) == QueryParserResult(q2,"0",[Query("key",["0", "1"],QueryType.Range)]));
    assert(parseSimpleQueries(q3) == QueryParserResult(q3,"0",[Query("key",["val", "val2"],QueryType.AND)]));
    assert(parseSimpleQueries(q4) == QueryParserResult(q4,"0",[Query("key",["val", "val2"],QueryType.OR)]));
    assert(parseSimpleQueries(q5) == QueryParserResult(q5,"(0 OR 1)",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q6) == QueryParserResult(q6,"(0 AND 1)",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q7) == QueryParserResult(q7,"NOT (0 OR 1)",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q8) == QueryParserResult(q8,"(NOT 0 OR NOT 1)",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q9) == QueryParserResult(q9,"NOT (0 AND 1)",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q10) == QueryParserResult(q10,"(NOT 0 AND NOT 1)",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q11) == QueryParserResult(q11,"0 OR 1",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q12) == QueryParserResult(q12,"0 AND 1",[Query("key",["val"], QueryType.Equal), Query("key2", ["val2"],QueryType.Equal)]));
    assert(parseSimpleQueries(q13) == QueryParserResult(q13,"0",[Query("key",["1"],QueryType.EqualsNumeric)]));
}

/// Patterns that combine base queries
/// think logic gates
/// AND -> intersection
/// OR -> union
/// NOT -> negation
auto logic_patterns = regex([
    //(1 AND 2)
    `\(((?:[0-9]+[\s]+AND[\s]+)+[0-9]+)\)`, //and
    //(1 OR 2)
    `\(((?:[0-9]+[\s]+OR[\s]+)+[0-9]+)\)`, //or
    //NOT 1
    `NOT[\s]+([0-9]+)`,
    //1 AND 2
    `((?:[0-9]+[\s]+AND[\s]+)+[0-9]+)`, //and
    //1 OR 2
    `\(((?:[0-9]+[\s]+OR[\s]+)+[0-9]+)\)`, //or
]);

/// Parse secondary queries (used after basic queries are parse)
auto parseLogicalStatements(string query, ulong i)
{
    // auto processedQueries = parseSimpleQueries(query);
    QueryParserResult res; //= processedQueries.results;
    res.query = query;
    outer: while(true)
    {
        auto matches = query.matchAll(logic_patterns);
        if(matches.empty == true) break;
        foreach(m;matches){
            query = query.replace(m[0],i.to!string);
            switch(m.whichPattern){
                case 1: //(0 AND 1)
                    string[] vals = m[1].splitter(regex(`[\s]+AND[\s]+`)).array;
                    res.results ~= Query("", vals, QueryType.AND);
                    break;
                case 2: //(0 OR 1)
                    string[] vals = m[1].splitter(regex(`[\s]+OR[\s]+`)).array;
                    res.results ~= Query("", vals, QueryType.OR);
                    break;
                case 3: //simple
                    res.results ~= Query("", [m[1]], QueryType.NOT);
                    break;
                case 4: //0 AND 1
                    string[] vals = m[1].splitter(regex(`[\s]+AND[\s]+`)).array;
                    res.results ~= Query("", vals, QueryType.AND);
                    break;
                case 5: //0 OR 1
                    string[] vals = m[1].splitter(regex(`[\s]+OR[\s]+`)).array;
                    res.results ~= Query("", vals, QueryType.OR);
                    break;
                default:
                    break outer;
            }
            i++;
        }
    }
    res.leftover = query;
    return res;
}

unittest
{
    auto q1 = "0";
    auto q2 = "1";
    auto q3 = "0 AND 1";
    auto q4 = "(0 OR 1)";
    auto q5 = "(0 OR 1) AND 1";
    auto q6 = "((0 AND 1) OR (1 AND 2)) AND 1";
    auto q7 = "((0 AND 1) OR (1 AND 2)) AND 1";
    auto q8 = "NOT 0";
    auto q9 = "NOT 1";
    auto q10 = "NOT 0 AND 1";
    auto q11 = "NOT (0 OR 1)";
    auto q12 = "NOT (NOT 0 OR 1) AND 1";
    auto q13 = "NOT ((0 AND 1) OR (1 AND 2)) AND 1";

    assert(parseLogicalStatements(q1, 1) == QueryParserResult(q1,"0",[]));
    assert(parseLogicalStatements(q2, 2) == QueryParserResult(q2,"1",[]));
    assert(parseLogicalStatements(q3, 2) == QueryParserResult(q3,"2",[Query("",["0", "1"],QueryType.AND)]));
    assert(parseLogicalStatements(q4, 2) == QueryParserResult(q4,"2",[Query("",["0", "1"],QueryType.OR)]));
    assert(parseLogicalStatements(q5, 2) == QueryParserResult(q5,"3",[Query("",["0","1"], QueryType.OR), Query("", ["2","1"],QueryType.AND)]));
    assert(parseLogicalStatements(q6, 3) == QueryParserResult(q6,"6",[Query("",["0","1"], QueryType.AND), Query("", ["1","2"],QueryType.AND), Query("", ["3","4"],QueryType.OR), Query("", ["5","1"],QueryType.AND)]));
    assert(parseLogicalStatements(q7, 3) == QueryParserResult(q7,"6",[Query("",["0","1"], QueryType.AND), Query("", ["1","2"],QueryType.AND), Query("", ["3","4"],QueryType.OR), Query("", ["5","1"],QueryType.AND)]));
    assert(parseLogicalStatements(q8, 1) == QueryParserResult(q8,"1",[Query("",["0"], QueryType.NOT)]));
    assert(parseLogicalStatements(q9, 2) == QueryParserResult(q9,"2",[Query("",["1"], QueryType.NOT)]));
    assert(parseLogicalStatements(q10, 2) == QueryParserResult(q10,"3",[Query("",["0"], QueryType.NOT), Query("", ["2","1"],QueryType.AND)]));
    assert(parseLogicalStatements(q11, 2) == QueryParserResult(q11,"3",[Query("",["0","1"], QueryType.OR), Query("", ["2"],QueryType.NOT)]));
    assert(parseLogicalStatements(q12, 2) == QueryParserResult(q12,"5",[Query("",["0"], QueryType.NOT), Query("", ["2", "1"],QueryType.OR), Query("", ["3"],QueryType.NOT), Query("", ["4", "1"],QueryType.AND)]));    
    assert(parseLogicalStatements(q13, 3) == QueryParserResult(q13,"7",[Query("",["0", "1"], QueryType.AND), Query("",["1", "2"], QueryType.AND), Query("", ["3", "4"],QueryType.OR), Query("", ["5"],QueryType.NOT), Query("", ["6", "1"],QueryType.AND)]));    
}

/// evaluate query as a whole and get results from 
/// the inverted index
/// returns the ids/md5sums of records that 
/// fufill the criteria of the query (based on current index) 
auto evalQuery(string q, JSONInvertedIndex * idx)
{
    /// Parse queries into basic steps
    auto primaryQueries = parseSimpleQueries(q);
    auto secondaryQueries = parseLogicalStatements(primaryQueries.leftover, primaryQueries.results.length);

    auto allQueries = primaryQueries.results ~ secondaryQueries.results;

    /// For each query step, perform actual queries to inverted index
    ulong[][] queryResults; 
    foreach (query; allQueries)
    {
        // if there is a key involved its a basic query
        if(query.key!=""){
            switch(query.type){
                // key=val
                case QueryType.Equal:
                    queryResults ~= idx.query(query.key,query.values[0]);
                    break;
                // key==val
                case QueryType.EqualsNumeric:
                    if(query.values[0][$-1] == 'f'){
                        queryResults ~= idx.query(query.key,query.values[0][0..$-1].to!double);    
                    }else{
                        queryResults ~= idx.query(query.key,query.values[0].to!long);
                    }
                    break;
                // key=val:val (numeric values only)
                case QueryType.Range:
                    if(query.values[0][$-1] == 'f' && query.values[1][$-1] == 'f'){
                        queryResults ~= idx.queryRange(query.key, query.values[0][0..$-1].to!double, query.values[1][0..$-1].to!double);
                    }else if(query.values[0][$-1] == 'f' || query.values[1][$-1] == 'f'){
                        throw new Exception("Range query does not allow mixin of int and float types, use a float only query");
                    }else{
                        queryResults ~= idx.queryRange(query.key, query.values[0].to!long, query.values[1].to!long);
                    }
                    break;
                // key>val (numeric values only)
                case QueryType.GreaterThan:
                    if(query.values[0][$-1] == 'f'){
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!">"(query.key,query.values[0].to!double);
                    }else{
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!">"(query.key,query.values[0].to!long);
                    }
                    break;
                // key>=val (numeric values only)
                case QueryType.GreaterThanEqual:
                    if(query.values[0][$-1] == 'f'){
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!">="(query.key,query.values[0].to!double);
                    }else{
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!">="(query.key,query.values[0].to!long);
                    }
                    break;
                // key<val (numeric values only)
                case QueryType.LessThan:
                    if(query.values[0][$-1] == 'f'){
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!"<"(query.key,query.values[0].to!double);
                    }else{
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!"<"(query.key,query.values[0].to!long);
                    }
                    break;
                // key<=val (numeric values only)
                case QueryType.LessThanEqual:
                    if(query.values[0][$-1] == 'f'){
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!"<="(query.key,query.values[0].to!double);
                    }else{
                        query.values[0] = query.values[0][0..$-1];
                        queryResults ~= idx.queryOp!"<="(query.key,query.values[0].to!long);
                    }
                    break;
                // key=(val AND val)
                case QueryType.AND:
                    queryResults ~= idx.queryAND(query.key,query.values);
                    break;
                // key=(val OR val)
                case QueryType.OR:
                    queryResults ~= idx.queryOR(query.key,query.values);
                    break;
                // not evaluated here
                case QueryType.NOT:
                default:
                    throw new Exception("Bad query");
            }
            debug stderr.writeln(query);
            debug stderr.writefln("basic query return %d results", queryResults[$-1].length);
        }else{
            // if there isn't a key involved its a compound 
            // query bc we have to use a previous queries
            // results
            switch(query.type){
                // 0 AND 1
                case QueryType.AND:
                    auto intersect = queryResults[query.values[0].to!ulong];
                    foreach (item; query.values[1..$])
                        intersect = setIntersection(intersect,queryResults[item.to!ulong]).array;
                    queryResults ~= intersect;
                    break;
                // 0 OR 1
                case QueryType.OR:
                    queryResults ~= multiwayUnion(query.values.map!(x=>queryResults[x.to!ulong]).array).array;
                    break;
                // NOT 0 
                case QueryType.NOT:
                    queryResults ~= idx.allIds.setDifference(queryResults[query.values[0].to!ulong]).array;
                    break;
                default:
                    throw new Exception("Bad query");
            }
        }
    }
    // at the end convert internal ids to md5sums
    return idx.convertIds(queryResults[secondaryQueries.leftover.to!ulong]);
}
