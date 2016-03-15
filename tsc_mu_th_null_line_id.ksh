#!/bin/ksh
##############################################################################
#  Program Name         : tsl_mu_th_null_line_id.ksh                              #
#  Language/Shell       : ksh                                                  #
#  Description          : The script will update the null line id in mu table  #
#  Parameters           : <userid/passsword>                                   #
#  Returns              : Return Values  : Return code of the job run          #
#                                   255  : abort                               #
#                                     0  : success                             #
#  (batch/interactive)  : batch                                                #
#  Author               : On Mohammad                                          #
#  Written By           : RPM Team                                             #
#  Date written         : 17-02-2016                                           #
#  Version No           : V1.0                                                 #
#  Modification history :                                                      #
#                                                                              #
#  Description of change                Date            Modified by            #
#  ---------------------                -------         -----------            #
##############################################################################
set -o xtrace

. /appl/retek/rms_batch_rmsnbs01.sh

if [ $# -lt 1 ]
then
echo "ERROR  Expecting parameter not passed.  Aborting."
echo "Usage: tsl_mu_th_null_line_id.ksh <userid/passwd>"
exit 255   # FATAL
fi

echo " STARTING PROGRAM !!! "

ORACLE_USER=$1
dt=`date +"%Y%m%d"`
st=`date`
STAMP=`date +"%Y%m%d"`
LOG_PATH=$MMHOME/log
ERR_DIR=$MMHOME/error
RTK_SCRIPTS=$MMHOME/scripts/ksh
LOG_FILE=$LOG_PATH/"tsl_mu_th_null_line_id_${STAMP}.log"
ERR_FILE=$ERR_DIR/"tsl_mu_th_null_line_id_${STAMP}.err"
SPOOL_FILE=`basename $0 ".ksh"`.lst
cd $RTK_SCRIPTS

sqlplus $UP<<EOF

SPOOL $SPOOL_FILE;

DECLARE

  c_tran tsc_mu_th_promotion_upld.tran_id%TYPE;
  c_line tsc_mu_th_promotion_upld.line_id%TYPE;

  CURSOR stage_rec IS
  
    select /*++ index(tsc_mu_th_promotion_upld_I1)*/
     tran_id, nvl(max(line_id) + 1, 1) line_id
      from tsc_mu_th_promotion_upld P
     where trunc(create_datetime) > get_vdate - 1
       and exists (select 1
              from tsc_mu_th_promotion_upld
             where tran_id = P.tran_id
               and line_id is null)
     group by tran_id;

BEGIN
  if NOT stage_rec%ISOPEN then
    Open stage_rec;
  end if;

  LOOP
  
    FETCH stage_rec
      into c_tran, c_line;
  
    EXIT WHEN stage_rec%NOTFOUND;
  
    update tsc_mu_th_promotion_upld
       set line_id             = c_line,
           status              = 'LOADED',
           report_sent         = 'N',
           component_generator = null
     where tran_id = c_tran
       and line_id is null;
  
    COMMIT;
  END LOOP;

  close stage_rec;


EXCEPTION
WHEN OTHERS then
dbms_output.put_line ('Other issue happened');
rollback;
if stage_rec%ISOPEN then
	close stage_rec;
end if;

END;
/

EOF

NUM_ERROR=`cat ${SPOOL_FILE} | grep "ORA-" | wc -l`
if [ $NUM_ERROR -gt 0 ]; then
        print "Error" >> ${LOG_FILE}
        return 1
fi

return 0

