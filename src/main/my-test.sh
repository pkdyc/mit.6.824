#!/usr/bin/env bash

#
# map-reduce tests
#

# comment this out to run the tests without the Go race detector.
RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

TIMEOUT=timeout
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT+=" -k 2s 180s "
fi
echo "the value of timeout is [$TIMEOUT]"

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
#(cd ../../mrapps && go clean)
#(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
#(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output


#echo '***' Starting wc test.
#
#$TIMEOUT ../mrcoordinator ../pg*txt &
##pid=$!
#
## give the coordinator time to create the sockets.
##sleep 1
#
### start multiple workers.
#$TIMEOUT ../mrworker ../../mrapps/wc.so &
#$TIMEOUT ../mrworker ../../mrapps/wc.so &
#$TIMEOUT ../mrworker ../../mrapps/wc.so &
#
## wait for the coordinator to exit.
#wait $pid
#
## since workers are required to exit when a job is completely finished,
## and not before, that means the job has finished.
#sort mr-out* | grep . > mr-wc-all
#if cmp mr-wc-all ../correct
#then
#  echo '---' wc test: PASS
#else
#  echo '---' wc output is not the same as mr-correct-wc.txt
#  echo '---' wc test: FAIL
#  failed_any=1
#fi
#
## wait for remaining workers and coordinator to exit.
#wait


##########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

$TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

$TIMEOUT ../mrworker ../../mrapps/mtiming.so &
$TIMEOUT ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait



########################################################
echo '***' Starting job count test.

rm -f mr-*

$TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

$TIMEOUT ../mrworker ../../mrapps/jobcount.so &
$TIMEOUT ../mrworker ../../mrapps/jobcount.so
$TIMEOUT ../mrworker ../../mrapps/jobcount.so &
$TIMEOUT ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait
