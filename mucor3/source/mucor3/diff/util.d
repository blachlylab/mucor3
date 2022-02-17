module mucor3.diff.util;

import std.math: std_round = round, pow, isClose;
import asdf : deserializeAsdf = deserialize, Asdf, AsdfNode, serializeToAsdf;

import libmucor.jsonlops.range: subset;
import mucor3.diff: samVarKeys;

Asdf getAFValue(Asdf obj, float precision){
    auto ret = AsdfNode(subset(obj, samVarKeys));
    Asdf af, info_af, fmt_af;
    if(obj["INFO"] != Asdf.init){
        info_af = obj["INFO", "AF"];
        if(info_af != Asdf.init)
            af = info_af;
    } else if(obj["FORMAT"] != Asdf.init){
        fmt_af = obj["FORMAT", "AF"];
        if(fmt_af != Asdf.init)
            af = fmt_af;
    } else {
        throw new Exception("No AF value found!");
    }
    if(af.kind != Asdf.Kind.number){
        throw new Exception("AF value is not a number!");
    }
    ret["AF"] = AsdfNode(round(af.deserializeAsdf!float, precision).serializeToAsdf);
    return cast(Asdf) ret;
}

float round(float x, uint places)
{
    float pwr = pow(10.0,places);
    return std_round(x * pwr) / pwr;
}

uint precisionToPlaces(float precision)
{
    assert(precision < 1.0);
    float p = precision;
    uint places = 0;
    while(!isClose(p, 1.0)){
        places++;
        p = p * 10.0;
    }
    return places;
}


float round(float x, float precision)
{
    return round(x, precisionToPlaces(precision));
}

unittest
{
    import std.stdio;

    auto p1 = precisionToPlaces(0.1);
    auto p2 = precisionToPlaces(0.01);
    auto p3 = precisionToPlaces(0.001);
    auto p4 = precisionToPlaces(0.0001);
    auto p5 = precisionToPlaces(0.00001);
    auto p6 = precisionToPlaces(0.000001);
    
    assert(p1 == 1);
    assert(p2 == 2);
    assert(p3 == 3);
    assert(p4 == 4);
    assert(p5 == 5);
    assert(p6 == 6);
    
    assert(round(0.123456, 1) == 0.1f);
    assert(round(0.123456, 2) == 0.12f);
    assert(round(0.123456, 3) == 0.123f);
    assert(round(0.123456, 4) == 0.1235f);
    assert(round(0.123456, 5) == 0.12346f);
    assert(round(0.123456, 6) == 0.123456f);

    assert(round(0.123456, 0.1) == 0.1f);
    assert(round(0.123456, 0.01) == 0.12f);
    assert(round(0.123456, 0.001) == 0.123f);
    assert(round(0.123456, 0.0001) == 0.1235f);
    assert(round(0.123456, 0.00001) == 0.12346f);
    assert(round(0.123456, 0.000001) == 0.123456f);
}