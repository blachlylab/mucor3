module mucor3.diff.stats;
import std.format: format;

struct VarStats
{
    ulong aCount;
    ulong bCount;

    ulong[5] varCounts;

    ulong[5] samVarCounts;

    ulong[5] afVarCounts;

    string toString()
    {
        string ret;
        ret ~= "Total Variant Records:\n";
        ret ~= "\tSet A:\t%d\n".format(aCount);
        ret ~= "\tSet B:\t%d\n".format(bCount);

        ret ~= "Unique Variant Records:\n";
        ret ~= "\tSet A:\t%d\n".format(varCounts[0]);
        ret ~= "\tSet B:\t%d\n".format(varCounts[1]);
        ret ~= "\tConserved (A & B):\t%d\n".format(varCounts[2]);
        ret ~= "\tLost (A - B):\t%d\n".format(varCounts[3]);
        ret ~= "\tGained (B - A):\t%d\n".format(varCounts[4]);

        ret ~= "Unique Variant Records Per Sample:\n";
        ret ~= "\tSet A:\t%d\n".format(samVarCounts[0]);
        ret ~= "\tSet B:\t%d\n".format(samVarCounts[1]);
        ret ~= "\tConserved (A & B):\t%d\n".format(samVarCounts[2]);
        ret ~= "\tLost (A - B):\t%d\n".format(samVarCounts[3]);
        ret ~= "\tGained (B - A):\t%d\n".format(samVarCounts[4]);

        ret ~= "Unique Variant Records Per Sample with Matching AF:\n";
        ret ~= "\tSet A:\t%d\n".format(afVarCounts[0]);
        ret ~= "\tSet B:\t%d\n".format(afVarCounts[1]);
        ret ~= "\tConserved (A & B):\t%d\n".format(afVarCounts[2]);
        ret ~= "\tLost (A - B):\t%d\n".format(afVarCounts[3]);
        ret ~= "\tGained (B - A):\t%d".format(afVarCounts[4]);

        return ret;
    }
}