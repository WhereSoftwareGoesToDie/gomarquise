# This is here so we can build with Travis; if you just want to use the
# library then you would probably be better off linking to a packaged
# version of libmarquise.

sudo apt-get install -y autoconf libtool automake build-essential libzmq3-dev libglib2.0-dev libprotobuf-c0-dev protobuf-c-compiler
mkdir -p deps/
cd deps
git clone https://github.com/anchor/libmarquise
cd libmarquise
autoreconf -i
./configure && make && make check 
sudo make install
