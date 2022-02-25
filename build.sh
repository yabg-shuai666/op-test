#!/bin/bash

cd `dirname $0`
WORK_DIR="`pwd`"

[[ -f ./build.config ]] && . ./build.config
export WITH_MLE_APP=1

if [ "x${PICO_PATH}" == "x" ]; then
    PICO_PATH="${WORK_DIR}/pico"
fi

pushd "${PICO_PATH}"

case $1 in
    link_all)
        if [ -e ./applications/mle/applications ]; then
            unlink ./applications/mle/applications
        fi
        ln -s "${WORK_DIR}/applications" ./applications/mle/
    ;;

    unlink_all)
        if [ -e ./applications/mle/applications ]; then
            unlink ./applications/mle/applications
        fi
    ;;

    *)
        if [ -e ./applications/mle/applications ]; then
            unlink ./applications/mle/applications
        fi
        ln -s "${WORK_DIR}/applications" ./applications/mle/
        custom_pom_version=$(cat "${WORK_DIR}/VERSION") \
            custom_git_commit_id=$(git describe --abbrev=100 --always) \
            ./build.sh $@
        unlink ./applications/mle/applications
    ;;
esac
