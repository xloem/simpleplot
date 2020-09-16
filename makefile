CPPFLAGS=-I/usr/local/include $$(pkg-config --cflags fuse3) $$(pkg-config --cflags openssl) -Isiaskynetpp/dependencies/json/include
LDFLAGS=-L/usr/local/lib64 -ggdb
CXXFLAGS=-std=c++17 -ggdb
LDLIBS=-lsiaskynetpp -lcpr -lcurl $$(pkg-config --libs fuse3) $$(pkg-config --libs openssl) -LlibShabal/target/release -lshabal -Wl,-rpath,libShabal/target/release

run: simpleplot
	./simpleplot 0.json -d test/

all: simpleplot

main.o: main.cpp portalpool.hpp skystream.hpp crypto.hpp

simpleplot: main.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@
