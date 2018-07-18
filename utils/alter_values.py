import sys
import argparse
import json

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("datasheet")
    parser.add_argument("keys")
    parser.add_argument("value_key")
    args=parser.parse_args()
    keys=args.keys.split("=")
    index=dict()
    for x in open(args.datasheet,"r").readlines():
        line=json.loads(x)
        if (keys[0] not in line):
            print("{} not present".format(keys[1]),sys.stderr)
            sys.exit(1)
        if (keys[1] not in line):
            print("{} not present".format(args.replace),sys.stderr)
            sys.exit(1)
        index[line[keys[0]]]=line[keys[1]]
    for x in sys.stdin:
        line=json.loads(x)
        if args.value_key in line:
            if line[args.value_key] in index:
                line[args.value_key]=index[line[args.value_key]]
            print(json.dumps(line))
