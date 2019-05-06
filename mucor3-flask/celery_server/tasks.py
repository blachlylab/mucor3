from celery_server.celery import app
import subprocess

@app.task
def mucor3_pivot(filein, folder):
    mucor=subprocess.Popen(
        ["python","../mucor.py",filein,folder])
    mucor.communicate()
    return mucor.returncode

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
def download(url, outfile):
    cat=subprocess.Popen(
        ["curl",url],stdout=open(outfile,"w"))
    return cat.returncode

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
