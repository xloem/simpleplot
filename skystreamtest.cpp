#include "skystream.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdio>
#include <cstring>
#include <getopt.h>
#include <unistd.h>

#include "tools.hpp"

int main(int argc, char **argv)
{
	auto options = parseoptions(argc, argv, {
		{"up", optional_argument, 0, 'u'},
		{"down", optional_argument, 0, 'd'},
		{"size", optional_argument, 0, 's'},
		{"span", required_argument, 0, 'n'},
		{"offset", required_argument, 0, 'o'},
		{"length", required_argument, 0, 'l'},
		{"help", no_argument, 0, 'h'}
	});
	if (!options.count("down") && !options.count("up") && !options.count("size")) {
		if (options.count("pos1")) {
			options["down"] = options["pos1"];
			std::cerr << "Assuming you mean to download from " << options["down"] << std::endl;
		} else {
			std::cerr << "Assuming you mean to upload" << std::endl;
			options["up"] = "";
		}
	}
	if (options.count("help") ) {
		std::cerr << "Specify --up or --down json file; see source for options." << std::endl;
		return 0;
	}

	if (options.count("down") + options.count("up") + options.count("size") > 1) {
		std::cerr << "Launch multiple processes to do multiple things." << std::endl;
		return -1;
	}

	std::cerr << "Measuring mirror speed ..." << std::endl;
	sia::portalpool pool;
	double offset = 0;
	if (options.count("offset")) {
		offset = std::stoull(options["offset"]);
	}
	double length = 0;
	if (options.count("length")) {
		length = std::stoull(options["length"]);
	}
	std::string span = "bytes";
	if (options.count("span")) {
		span = options["span"];
	}
	if (options.count("size")) {
		if (!options["size"].size()) {
			options["size"] = "skystream.json";
			std::cerr << "No --size=, assuming " << options["size"] << std::endl;
		}
		skystream stream(pool, file2json(options["size"]));
		auto range = stream.span(span);
		std::cout << range.first << std::endl;
		std::cout << range.second << std::endl;
	}
	if (options.count("down")) {
		if (!options["down"].size()) {
			options["down"] = "skystream.json";
			std::cerr << "No --down=, assuming " << options["down"] << std::endl;
		}
		skystream stream(pool, file2json(options["down"]));

		auto range = stream.span(span);
		if (range.first > offset) {
			offset = range.first;
		}
		std::cerr << "Downloading from " << options["down"] << " to stdout starting from " << span << " " << offset << std::endl;
		if (length == 0 || length + offset > range.second) {
			length = range.second - offset;
		}
		double end = offset + length;
		while (offset < end) {
			auto data = stream.read(span, offset);
			std::cerr << "Downloaded " << data.size() << " bytes" << std::endl;
			size_t suboffset = 0;
			while (suboffset < data.size()) {
				ssize_t size = write(1, data.data() + suboffset, data.size() - suboffset);
				if (size < 0) {
					perror("write");
					return size;
				}
				suboffset += size;
			}
		}
	} else if (options.count("up")) {
		if (!options["up"].size()) {
			options["up"] = "skystream.json";
			std::cerr << "No --up=, assuming " << options["up"] << std::endl;
		}
		std::unique_ptr<skystream> streamptr;
		streamptr.reset(new skystream(pool, file2json(options["up"])));
		skystream & stream = *streamptr;
		auto range = stream.span(span);
		double offset = range.second;
		std::cerr << "Uploading to " << options["up"] << " from stdout starting from " << span << " " << offset << std::endl;
		std::vector<uint8_t> data;
		data.reserve(1024*1024*16);
		data.resize(data.capacity());
		ssize_t size;
		while ((size = read(0, data.data(), data.size()))) {
			if (size < 0) {
				perror("read");
				return size;
			}
			data.resize(size);
			stream.write(data, "bytes", offset);
			std::cerr << "Uploaded " << data.size() << " bytes" << std::endl;
			json2file(stream.identifiers(), options["up"]);
			offset += data.size();
			data.resize(data.capacity());
		}
	}
}
