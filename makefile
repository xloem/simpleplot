CPPFLAGS=-I/usr/local/include $$(pkg-config --cflags fuse3) $$(pkg-config --cflags openssl) -Isiaskynetpp/dependencies/json/include
LDFLAGS=-L/usr/local/lib64 -ggdb
CXXFLAGS=-std=c++17 -ggdb
LDLIBS=-lsiaskynetpp -lcpr -lcurl $$(pkg-config --libs fuse3) $$(pkg-config --libs openssl) -LlibShabal/target/release -lshabal -Wl,-rpath,libShabal/target/release

runtest: skystreamtest bufferedskystreamtest
	./bufferedskystreamtest 879bd2f1acf9d80f0910f9d4c152515fc328fbae253b407dfc2607f5696f88d9837a2b2f388319d19ed7e96af5a351a0c28989737973bb8c854eb678cfee93e5.json | sha512sum
	echo 'hello world' | ./bufferedskystreamtest

dbg: bufferedskystreamtest
	gdb --args ./bufferedskystreamtest helloworld.json

run: simpleplot
	./simpleplot 0.json -d test

all: simpleplot

main.o: main.cpp portalpool.hpp skystream.hpp crypto.hpp

simpleplot: main.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@
