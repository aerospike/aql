# Download and install lua 5.1.5
sudo wget https://sourceforge.net/projects/luabinaries/files/5.1.5/Linux%20Libraries/lua-5.1.5_Linux26g4_64_lib.tar.gz/download
sudo tar xvf download

# compile module
echo "gcc -Wall -shared -fPIC -o power.so power.c"
gcc -fPIC -o power.so -shared power.c -I ./include/
echo "aql -c \"register package './power.so'\""
aql -c "register package './power.so'"
echo "aql -c \"register package './use_power.lua'\""
aql -c "register package './use_power.lua'"

echo "Done! "
echo "To test, run:"
echo "aql -c \"execute use_power.call_go(123) on test where pk='x'\""
