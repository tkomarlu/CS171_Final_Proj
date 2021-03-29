#Run this script by running chmod +x runLocally.sh
#Then run ./runLocally.sh
set -e
set -x

xterm -geometry 55x24+000+000 -hold -e /bin/bash -c "python3.7 server.py 1" &
xterm -geometry 55x24+340+000 -hold -e /bin/bash -c "python3.7 server.py 2" &
xterm -geometry 55x24+680+000 -hold -e /bin/bash -c "python3.7 server.py 3" &
xterm -geometry 55x24+1020+000 -hold -e /bin/bash -c "python3.7 server.py 4" &
xterm -geometry 55x24+000+345 -hold -e /bin/bash -c "python3.7 server.py 5" &

xterm -geometry 55x24+340+345 -hold -e /bin/bash -c "python3.7 client.py C1" &
xterm -geometry 55x24+680+345 -hold -e /bin/bash -c "python3.7 client.py C2" &
xterm -geometry 55x24+1020+345 -hold -e /bin/bash -c "python3.7 client.py C3" &

echo "waiting..."
wait