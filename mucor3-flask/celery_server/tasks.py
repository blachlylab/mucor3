from celery_server.celery import app
import subprocess

@app.task
def mucor3_pivot(filein, piv_index,piv_on,piv_value,outfile):
    mucor=subprocess.Popen(
        ["python","../aggregate.py","-pi"]+
        [x.strip() for x in piv_index.split(",")]+
        ["-po"]+[x.strip() for x in piv_on.split(",")]+
        ["-pv", piv_value.strip()],
        stdin=open(filein,'r'),stdout=subprocess.PIPE)
    to_tsv=subprocess.Popen(
        ["python","../utils/jsonl2csv.py","-t","-i"]+[x.strip() for x in piv_index.split(",")],
        stdin=mucor.stdout,stdout=open(outfile,"w"))
        #stdin=mucor.stdout)
    return to_tsv.returncode

@app.task
def mucor3_master(filein,outfile):
    to_tsv=subprocess.Popen(
        ["python","../utils/jsonl2csv.py","-t"],
        stdin=open(filein,"r"),stdout=open(outfile,"w"))
    return to_tsv.returncode

@app.task
def zip(folder,outfile):
    zip=subprocess.Popen(
        ["zip","-r",outfile,folder])
    return zip.returncode

@app.task
def combine(files, outfile):
    cat=subprocess.Popen(
        ["cat"]+[x for x in files],stdout=open(outfile,"w"))
    return cat.returncode

@app.task
def atomizer(file):
    atom=subprocess.Popen(
        ["../vcf_atomizer/bin/atomizer",file],stdout=open(file+".jsonl","w"))
    atom.communicate()
    return atom.returncode
