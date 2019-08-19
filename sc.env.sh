# !/bin/bash

#
  set -o nounset -o pipefail -o verbose -o xtrace #+o verbose +o xtrace -o errexit
#
  export MKT=$1
  export PID=$$
#
  export OF_FUL=$MKT.ful.txt
  export OF_INC=$MKT.inc.txt
  export URL_FILE=$MKT.url.txt
  export INS_FILE=$MKT.instrument.txt
  export AMC_LIST=mf.amc.txt
  export IDX_FILE="idx.ful.txt"
#
  export WGET="wget -nv -c -t0 -a wget.log --waitretry=20 -U Mozilla -4 "
  #export WGETO_RWAIT="-w2 --random-wait --limit-rate=54k "
  export WGETO_RWAIT="-w2 --random-wait "
  export SORT="sort --parallel=4 --buffer-size=70% "
  #export THREADS=27
  export THREADS=9
  export BCKTRK_DAYS=3
#
  export PGPASSWORD="password"