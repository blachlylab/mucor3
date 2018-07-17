from celery_server.celery import app
import subprocess

@app.task
def mucor3(filein, piv_index,piv_on,piv_value,outfile):
    mucor=subprocess.Popen(
        ["python","aggregate.py",
        "-pi"," ".join(piv_index),
        "-po"," ".join(piv_on),
        "-pv",piv_value],
        stdin=open(filein,'r'),stdout=subprocess.PIPE)
    to_tsv=subprocess.Popen(
        ["python","jsonl2csv.py",
        "-pi"," ".join(piv_index),
        "-po"," ".join(piv_on),
        "-pv",piv_value],
        stdin=mucor.stdout,stdout=open(outfile,"w"))
    to_tsv.communicate()
    return to_tsv.returncode

@app.task
def combine(files, outfile):
    cat=subprocess.Popen(
        ["cat"]+files,
        stdin=mucor.stdout,stdout=open(outfile,"w"))
    cat.communicate()
    return cat.returncode

@app.task
def atomizer(file):
    atom=subprocess.Popen(
        ["../vcf_atomizer/bin/atomizer",file],stdout=open(file+".jsonl","w"))
    atom.communicate()
    return atom.returncode
