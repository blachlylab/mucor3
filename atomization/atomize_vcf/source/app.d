import std.stdio;

import vcf : parseVCF;

void main(string[] args)
{
	parseVCF(args[1]);
}
