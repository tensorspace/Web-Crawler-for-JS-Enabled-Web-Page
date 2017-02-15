from datetime import date
from workdays import workday
# generate the bash for each trading day
def main():
    holiday = [date(year=2016,month=4,day=4), date(year=2016,month=5,day=2), date(year=2016,month=6,day=9), date(year=2016,month=6,day=10), date(year=2016,month=9,day=15), date(year=2016,month=9,day=16)]
    sh_file = open('test.sh', 'w+') # test.sh is the bash to call to submit all jobs to the queue
    sh_file.write('#!/bin/bash\n')
    for i in range(1, 126):
        dt = workday(date(2016, 3, 31), i, holiday)
        sh_file.write('qsub '+ str(dt) + '.pbs\n')
        csv_file = open(str(dt)+'.pbs', 'w+')
        # .pbs is the command specifying each job
        csv_file.write('#!/bin/sh\n#PBS -o ' + str(dt) + '_output\n#PBS -N sample_job\n#PBS -M wenschen@umich.edu\n#PBS -m abe\n#PBS -l nodes=1:ppn=1,mem=2000mb,walltime=01:00:00\n#PBS -j oe\n#PBS -V\n#PBS -A lsa_flux\n#PBS -q flux\n#PBS -l qos=flux\nif [ -n "$PBS_NODEFILE" ]; then cat $PBS_NODEFILE; fi\nif [ -n "$PBS_O_WORKDIR" ]; then cd $PBS_O_WORKDIR; fi\nmodule load python-anaconda3/latest\nrm ' + str(dt)  + '_output\npython web_crawler2.py ' + str(dt))

if __name__ == '__main__':
    main()
