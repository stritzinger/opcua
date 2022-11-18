#!/bin/bash

ARGS=( "$@" )
ROOT=$( cd $(dirname $0); cd ..; pwd )

VERSION="v1.04"
SOURCE_URL="https://raw.githubusercontent.com/OPCFoundation/UA-Nodeset/{{VERSION}}/Schema"
FILE_LIST=( "AttributeIds.csv" "StatusCode.csv" "Opc.Ua.NodeSet2.Services.xml" )
OUTPUT_DIR="priv/nodeset/reference/Schema"

show_usage()
{
    echo "USAGE: update_nodes.sh [-h] [-v VERSION] [-u URL] [update|refresh]"
    echo "OPTIONS:"
    echo " -h Show this."
    echo " -d Show debug information."
    echo " -v VERSION: The version of the nodeset to use."
    echo "    Default: $VERSION"
    echo " -u URL: The base URL of the nodeset files."
    echo "    Default: $SOURCE_URL"
    echo
    echo "e.g. update_nodes.sh"
}

error()
{
    msg="$*"
    echo "ERROR: $msg"
    show_usage
    exit 1
}

# Parse script's arguments
OPTIND=1

while getopts "hdv:u:" opt; do
    case "$opt" in
    d)
        set -x
        ;;
    v)
        VERSION="${OPTARG}"
        ;;
    u)
        SOURCE_URL="${OPTARG}"
        ;;
    *)
        show_usage
        exit 0
        ;;
    esac
done
shift $((OPTIND-1))
[[ "${1:-}" == "--" ]] && shift
if [[ $# -eq 0 ]]; then
    COMMAND="update"
else
    COMMAND="$1"
    shift
fi
if [[ $# > 0 ]]; then
    error "Too many arguments"
fi

TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" 0 2 3 15

ERl_CONFIG="$TMP_DIR/erlang"
ERl_CONFIG_FILE="$ERl_CONFIG.config"
cat > "$ERl_CONFIG_FILE" <<EOF
[
    {kernel, [
        {logger_level, debug},
        {logger, [
            {handler, default, logger_std_h, #{
                level => debug,
                filter_default => log,
                config => #{type => standard_io},
                formatter => {logger_formatter, #{
                    legacy_header => false,
                    single_line => true,
                    template => [msg, "\n"]
                }}
            }}
        ]}
    ]}
].
EOF

update()
{
    echo "Updating NodeSet to version $VERSION..."
    URL="${SOURCE_URL/"{{VERSION}}"/$VERSION}"
    cd "${ROOT}/${OUTPUT_DIR}"
    for f in ${FILE_LIST[@]}; do
        echo "Retrieving $f..."
        rm -f "$f"
        wget -q --timeout=20 "${URL}/$f" || error "Failed to retrieve $f"
    done
}

generate()
{
    echo "Generating NodeSet data..."
    cd "$ROOT"
    rebar3 compile || error "Build failed"
    ERL_LIBS=_build/default/lib erl -run opcua_nodeset_parser parse -run init stop -noshell -config "$ERl_CONFIG" || error "Parsing failed"
}

case "$COMMAND" in
    update)
        update
        generate
        ;;
    refresh)
        generate
        ;;
    *)
        error "Invalid argument"
        ;;
esac
