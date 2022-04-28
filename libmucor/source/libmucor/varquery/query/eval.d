module libmucor.varquery.query.eval;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple, tuple;
import std.string;
import std.algorithm.searching;
import std.conv: ConvException, parse;

// import libmucor.varquery.query.primary;
import libmucor.varquery.query.value;
import libmucor.varquery.query.keyvalue;
import libmucor.varquery.query.util;
import libmucor.varquery.query.expr;
