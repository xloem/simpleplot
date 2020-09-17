#include "bufferedskystream.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdio>
#include <cstring>
#include <unistd.h>

#include "tools.hpp"

int main(int argc, char **argv)
{
	auto options = parseoptions(argc, argv, {
		{"up", optional_argument, 0, 'u'},
		{"down", optional_argument, 0, 'd'},
		{"size", optional_argument, 0, 's'},
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

	std::cerr << "Finding responsive mirrors ..." << std::endl;
	sia::portalpool pool;
	uint64_t offset = 0;
	if (options.count("offset")) {
		offset = std::stoull(options["offset"]);
	}
	uint64_t length = 0;
	if (options.count("length")) {
		length = std::stoull(options["length"]);
	}
	if (options.count("size")) {
		if (!options["size"].size()) {
			options["size"] = options["pos1"];
		}
		if (!options["size"].size()) {
			options["size"] = "skystream.json";
			std::cerr << "No --size=, assuming " << options["size"] << std::endl;
		}
		skystream stream(pool, file2json(options["size"]));
		auto range = stream.span("bytes");
		//std::cout << (uint64_t)range.first << std::endl;
		std::cout << (uint64_t)range.second << std::endl;
	}
	bufferedskystreams streams(pool);
	if (options.count("down")) {
		if (!options["down"].size()) {
			options["down"] = options["pos1"];
		}
		if (!options["down"].size()) {
			options["down"] = "skystream.json";
			std::cerr << "No --down=, assuming " << options["down"] << std::endl;
		}
		streams.add(file2json(options["down"]));
		auto & stream = streams.get(0);

		auto range = stream.span("bytes");
		if (range.first > offset) {
			offset = range.first;
		}
		if (length == 0 || length + offset > range.second) {
			length = range.second - offset;
		}
		uint64_t end = offset + length;
		std::cerr << "Downloading range [" << offset << "," << end << ") from " << options["down"] << " to stdout ..." << std::endl;
		std::mutex outputline;
		streams.set_down_callback([&outputline](bufferedskystream&stream, uint64_t size){
			std::scoped_lock lock(outputline);
	                std::cerr << "Finished queuing download of " << size << " bytes" << std::endl;
		});
		while (offset < end) {
			auto data = stream.xfer_local_down(offset, 0, end);
			{
				std::scoped_lock lock(outputline);
				std::cerr << "Downloaded " << data.size() << " bytes" << std::endl;
			}
			size_t suboffset = 0;
			while (suboffset < data.size()) {
				ssize_t size = write(1, data.data() + suboffset, data.size() - suboffset);
				if (size < 0) {
					perror("write");
					return size;
				}
				suboffset += size;
			}
			offset += data.size();
		}
		streams.shutdown();
	} else if (options.count("up")) {
		if (!options["up"].size()) {
			options["up"] = options["pos1"];
		}
		if (!options["up"].size()) {
			options["up"] = "skystream.json";
			std::cerr << "No --up=, assuming " << options["up"] << std::endl;
		}
		streams.add(file2json(options["up"]));
		bufferedskystream & stream = streams.get(0);
		auto range = stream.span("bytes");
		double offset = range.second;
		std::cerr << "Uploading to " << options["up"] << " from stdin starting from " << "bytes" << " " << (uint64_t)offset << std::endl;
		std::vector<uint8_t> data;
		data.reserve(1024*1024*16);
		data.resize(data.capacity());
		ssize_t size;
		std::mutex outputline;
		streams.set_up_callback([&outputline,&options](bufferedskystream&stream, uint64_t size){
			json2file(stream.identifiers(), options["up"]);
			std::scoped_lock lock(outputline);
			std::cerr << "Uploaded " << size << " bytes" << std::endl;
		});
		while ((size = read(0, data.data(), data.size()))) {
			if (size < 0) {
				perror("read");
				return size;
			}
			data.resize(size);
			stream.queue_local_up(std::move(data));
			{
				std::scoped_lock lock(outputline);
				std::cerr << "Queued upload of " << size << " bytes" << std::endl;
			}
			offset += size;
			data.resize(data.capacity());
		}
		streams.shutdown();
	}
}
