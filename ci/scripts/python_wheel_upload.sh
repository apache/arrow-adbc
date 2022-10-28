#!/bin/bash

main() {
    touch curl_output.txt
    for wheel in "$@"
    do
        echo "==== Uploading $wheel"
        local response_code=$(curl \
                                  --silent \
                                  -o curl_output.txt \
                                  -w '%{http_code}' \
                                  -F "package=@${wheel}" \
                                  https://${GEMFURY_PUSH_TOKEN}@push.fury.io/arrow-adbc-nightlies/)
        cat curl_output.txt
        if [[ $response_code -eq 409 ]]; then
            # Ignore the failure, since we build 'none' wheels, we may have duplicate wheels across pipelines
            true
        elif [[ $response_code -ge 300 ]]; then
            exit 1
        fi
    done
}

main "$@"
