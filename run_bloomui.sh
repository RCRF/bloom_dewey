#source this

# Check if $1 is null and set host accordingly
if [ -z "$1" ]; then
  host="0.0.0.0"
else
  host="$1"
fi

# Run uvicorn with the specified host
uvicorn main:app --reload --log-level trace --port 8911 --timeout-keep-alive 303 --host $host
