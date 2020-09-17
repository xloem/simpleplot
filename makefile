CPPFLAGS=-I/usr/local/include $$(pkg-config --cflags fuse3) $$(pkg-config --cflags openssl) -Isiaskynetpp/dependencies/json/include
LDFLAGS=-L/usr/local/lib64 -ggdb
CXXFLAGS=-std=c++17 -ggdb -Wall -O0
LDLIBS=-lsiaskynetpp -lcpr -lcurl $$(pkg-config --libs fuse3) $$(pkg-config --libs openssl) -LlibShabal/target/release -lshabal -Wl,-rpath,libShabal/target/release

run: simpleplot
	./simpleplot 0.json -d test

all: simpleplot

clean:
	-rm skystreamtest bufferedskystreamtest simpleplot *.o

runtest: skystreamtest bufferedskystreamtest
	-rm tmp.json
	./skystreamtest helloworld.json | ./skystreamtest --up=tmp.json
	./skystreamtest tmp.json | grep 'hello world'
	rm tmp.json
	./bufferedskystreamtest 879bd2f1acf9d80f0910f9d4c152515fc328fbae253b407dfc2607f5696f88d9837a2b2f388319d19ed7e96af5a351a0c28989737973bb8c854eb678cfee93e5.json | ./bufferedskystreamtest --up=tmp.json
	./bufferedskystreamtest tmp.json | sha512sum | grep 879bd2f1acf9d80f0910f9d4c152515fc328fbae253b407dfc2607f5696f88d9837a2b2f388319d19ed7e96af5a351a0c28989737973bb8c854eb678cfee93e5
	rm tmp.json

dbg: bufferedskystreamtest
	gdb --args ./bufferedskystreamtest helloworld.json

main.o: main.cpp portalpool.hpp skystream.hpp crypto.hpp

simpleplot: main.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@
