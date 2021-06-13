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
  #export WGET="wget -nv -c -t0 -a wget.log --waitretry=20 -U Mozilla -4 "
  #export WGET_USER_AGENT='"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"'
  #export WGET="wget -nv -c --tries=99 -a wget.log --waitretry=20 "
  #export WGET="wget --verbose --continue --tries=99 --append-output=wget.log --timeout=90 --debug --waitretry=20 "
  #export WGET_USER_AGENT="Mozilla/5.0 "
  #export WGETO_RWAIT="-w2 --random-wait "

  export WGET="wget --append-output=wget.log "

  export SORT="sort --parallel=4 --buffer-size=70% "
  #export THREADS=27
  export THREADS=1
  export BCKTRK_DAYS=1
#
  export PGPASSWORD="password"
